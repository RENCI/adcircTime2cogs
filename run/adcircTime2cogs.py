#!/usr/bin/env python
import sys
import os
import argparse
import shutil
import glob
from loguru import logger

import pandas as pd
import numpy as np
import matplotlib.tri as Tri
from scipy import interpolate
from scipy.spatial import Delaunay

import dask
import dask.array as da

import geopandas as gpd
import dask_geopandas as dgp

import xarray as xr
from datacube.utils.cog import write_cog

from affine import Affine
from pyproj import CRS

import utilities.adcirc_dask_utilities as adcirc_utilities

def construct_geopandas(agdict, targetepsg):
    """
    Define geopandas processors
    project grid coords, before making Triangulation object
    """

    df_Adcirc = pd.DataFrame({'Longitude': agdict['lon'],'Latitude': agdict['lat']})

    gdf = gpd.GeoDataFrame(df_Adcirc, geometry=gpd.points_from_xy(agdict['lon'], agdict['lat']))

    ddf = dgp.from_geopandas(gdf, npartitions=4)
    
    # init crs is LonLat, WGS84
    adcircepsg = agdict['crs']
    # Adding crs to initial GDF
    ddf.crs = CRS(adcircepsg)
    # Converting GDF
    ddf = ddf.to_crs(CRS(targetepsg))
    return ddf

def compute_geotiff_grid(targetgrid, adcircepsg, targetepsg):
    """
    project raster grid to target crs

    Results:
        rasdict. Dict of raster grid parameters and coords
        Values for upperleft_x, upperleft_y, x,y,xx,yy,xxm,yym
    """
    
    df_target = pd.DataFrame(data=targetgrid)
    gdf_target = gpd.GeoDataFrame(
        df_target, geometry=gpd.points_from_xy(df_target.ul_longitude, df_target.ul_latitude))

    # init projection is LonLat, WGS84
    gdf_target.crs = CRS(adcircepsg)

    # convert to "targetepsg"
    gdf_target = gdf_target.to_crs(CRS(targetepsg)) # CHECK!

    # compute spatial grid for raster
    upperleft_x = gdf_target['geometry'][0].x
    upperleft_y = gdf_target['geometry'][0].y
    lowerright_x = df_target.lr_longitude
    lowerright_y = df_target.lr_latitude

    x = dask.array.arange(upperleft_x, lowerright_x[0], targetgrid['res'][0])
    y = dask.array.arange(upperleft_y, lowerright_y[0], -targetgrid['res'][0])
    xx, yy = np.meshgrid(x, y)

    # get centroid coords
    xm = (x[1:] + x[:-1]) / 2
    ym = (y[1:] + y[:-1]) / 2
    xxm, yym = np.meshgrid(xm, ym)

    return {'uplx': upperleft_x,
            'uply': upperleft_y,
            'x':    x,
            'y':    y,
            'xx':   xx,
            'yy':   yy,
            'xxm':  xxm,
            'yym':  yym,
            'nx':   x.shape,
            'ny':   y.shape}

def create_xarray(rasdict, zi_in, targetepsg):
    x = rasdict['xxm'][0,:]
    y = rasdict['yym'][:,0] 
    data = xr.DataArray(
        zi_in,
        dims=("y", "x"),
        coords={
            "y": xr.DataArray(y, name="y", dims="y"),
            "x": xr.DataArray(x, name="x", dims="x"),
        },
        attrs={
        "crs": targetepsg,
        },
    )
    
    return data

# Make output directory if it does not exist
def makeDirs(dirPath):
    # Create directory path
    if not os.path.exists(dirPath):
        mode = 0o777
        os.makedirs(dirPath, mode, exist_ok=True)
        logger.info('Made directory '+dirPath+ '.')
    else:
        logger.info('Directory '+dirPath+' already made.')

@logger.catch
def main(inputDir, inputFile, inputVariable):
    # Creat output variable directory 
    outputVarDir = os.path.join(os.path.join(os.environ['COG_MOSAIC_PATH'], '')+inputDir.split('/')[2]+'/'+"".join(inputFile[:-3].split('.'))+'_'+inputVariable, '')
    logger.info('Created outputVarDir '+outputVarDir+'.')

    # Define tmp directory
    tmpDir = "/".join(inputDir.split("/")[:-2])+"/"+inputFile.split('.')[0]+"_"+inputVariable+"_dask_tmp/"

    # Make tmpDir 
    os.makedirs(tmpDir, exist_ok=True)

    # Config DASK to use tmpDir
    dask.config.set(temporary_directory=tmpDir)
    logger.info('Configure tmp directory for DASK: '+tmpDir)

    adcircepsg = 'EPSG:4326'
    targetepsg = 'EPSG:4326'

    # Make output directory
    makeDirs(outputVarDir.strip())

    logger.info('Read '+inputDir+inputFile+' and create agdict')
    nc, agdict = adcirc_utilities.extract_url_grid(inputDir+inputFile)
    agdict['crs'] = adcircepsg

    logger.info('Define targetgrid')
    targetgrid = {'res':[0.005],
                  'nx':[8838],
                  'ny':[8000],
                  'theta':[0]}
    targetgrid['ul_latitude'] = agdict['lat'].max()
    targetgrid['ul_longitude'] = agdict['lon'].min()
    targetgrid['lr_longitude'] = agdict['lon'].max()
    targetgrid['lr_latitude'] = agdict['lat'].min()

    logger.info('Create geopandas DataFrame from agdict and tartgetepsg')
    gdf = construct_geopandas(agdict, targetepsg)
    xtemp, ytemp = gdf['geometry'].x, gdf['geometry'].y

    logger.info('Create triang using Tri.Triangulation')
    triang = Tri.Triangulation(xtemp, ytemp, triangles=agdict['ele'])
    triang.triangles = da.from_array(triang.triangles).rechunk(445513,8)
    triang.x = da.from_array(triang.x).rechunk(302240,6)
    triang.y = da.from_array(triang.y).rechunk(302240,6)

    logger.info('Create traingd using Delaunay')
    triangd = Delaunay(np.stack((agdict['lon'].values, agdict['lat'].values), axis=1))

    logger.info('Compute geotiff grid coordinates')
    rasdict = compute_geotiff_grid(targetgrid, adcircepsg, targetepsg)
    xxm, yym = rasdict['xxm'].rechunk(4000,4419,2), rasdict['yym'].rechunk(4000,4419,2)

    logger.info('Computer ones for mask')
    advardict = adcirc_utilities.get_adcirc_slice(nc, inputVariable, 0)
    z_ones = np.ones((len(advardict['data']),), dtype=float)
    logger.info('Create onesinterp_lin')
    onesinterp_lin = Tri.LinearTriInterpolator(triang, z_ones)
    logger.info('Compute ones')
    ones_z = onesinterp_lin(xxm,yym)

    mindex = np.where(ones_z.mask == True)

    i = 0

    logger.info('Loop through each timestep in '+inputFile+' and regrid data')
    for timestep in nc.variables['time']:
        logger.info('Get file data time from '+inputFile)
        fileDateTime = "".join("".join(str(timestep.values).split('-')).split(':')).split('.')[0]+'Z'

        outputFile = '_'.join(['_'.join(inputFile.split('.')[0:2]),inputVariable,fileDateTime+'.tiff'])
        logger.info('Get data for timestep in '+inputFile)
        advardict = adcirc_utilities.get_adcirc_slice(nc, inputVariable, i)

        logger.info('Start regrid of timestepp: '+fileDateTime)
        interpolator = interpolate.LinearNDInterpolator(triangd, advardict['data'])
        grid_zi = interpolator((xxm, yym))
        grid_zi[mindex] = np.nan
        logger.info('Finish regrid of timestepp: '+fileDateTime)

        logger.info('Start writing regridded data to tiff file: '+outputVarDir+outputFile)
        zi_data = create_xarray(rasdict, grid_zi, targetepsg)
        write_cog(geo_im=zi_data,fname=outputVarDir+outputFile,overwrite=True)
        logger.info('Finish writing regridded data to tiff file: '+outputVarDir+outputFile)

        i = i + 1

    logger.info('Create meta file for timeseries mosaic COGs')
    f = open(outputVarDir+'indexer.properties', 'w')
    f.write('TimeAttribute=ingestion\nElevationAttribute=elevation\nSchema=*the_geom:Polygon,location:String,ingestion:java.util.Date,elevation:Integer\nPropertyCollectors=TimestampFileNameExtractorSPI[timeregex](ingestion)\n')
    f.close()

    f = open(outputVarDir+'timeregex.properties', 'w')
    f.write('regex=[0-9]{8}T[0-9]{6}\n')
    f.close()

    f = open(outputVarDir+'datastore.properties', 'w')
    f.write('SPI=org.geotools.data.postgis.PostgisNGDataStoreFactory\nhost='+os.environ['ASGS_HOST']+'\nport='+os.environ['ASGS_PORT']+'\ndatabase='+os.environ['COG_MOSAIC_DATABASE']+'\nschema=public\nuser='+os.environ['COG_MOSAIC_USERNAME']+'\npasswd='+os.environ['COG_MOSAIC_PASSWORD']+'\nLoose\ bbox=true\nEstimated\ extends=false\nvalidate\ connections=true\nConnection\ timeout=10\npreparedStatements=true\n')
    f.close()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir", required=True)
    parser.add_argument("--inputFILE", "--inputFile", help="Input file name", action="store", dest="inputFile", required=True)
    parser.add_argument("--inputPARAM", "--inputVariable", help="Input parameter", action="store", dest="inputVariable", required=True)

    args = parser.parse_args()

    # Remove old logger and start new logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), '../logs')), '')
    logger.add(log_path+'adcircTime2cogs.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")
    logger.info('Started log file adcircTime2cogs.log')

    # get input variables from args
    inputDir = os.path.join(args.inputDir, '')
    inputFile = args.inputFile
    inputVariable = args.inputVariable

    if os.path.exists(inputDir+inputFile):
         main(inputDir, inputFile, inputVariable)
    else:
         logger.info(inputDir+inputFile+' does not exist')
         if inputFile.startswith("swan"):
             logger.info('The input file is a swan file : '+inputDir+inputFile+' so do a soft exit')
             sys.exit(0)
         else:
             logger.info('The input file is not a swan file : '+inputDir+inputFile+' so do a hard exit')
             sys.exit(1)

