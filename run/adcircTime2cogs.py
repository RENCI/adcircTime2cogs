#!/usr/bin/env python
import sys, os, argparse, shutil, glob, pdb
from pathlib import Path
from loguru import logger

import pandas as pd
import numpy as np
import matplotlib.tri as Tri
from scipy import interpolate
from scipy.spatial import Delaunay

import dask
import dask.array as da

#import rasterio as rio
from rasterio.io import MemoryFile
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

import geopandas as gpd
import dask_geopandas as dgp

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

def write_tif(rasdict, zi_lin, targetgrid, targetepsg, filename='test.tif'):
    """
    Construct the new TIF file and store it to disk in filename
    """
    xm0, ym0 = rasdict['uplx'], rasdict['uply']

    a=targetgrid['res'][0]
    b=0
    c=xm0
    d=0
    e=-targetgrid['res'][0]
    f=ym0

    aft=Affine(a,b,c,b,e,f)*Affine.rotation(-targetgrid['theta'][0])

    # TIF transform {aft}

    md = {'crs':       targetepsg,
          'driver':    'GTiff',
          'height':    targetgrid['ny'][0],
          'width':     targetgrid['nx'][0],
          'count':     1,
          'dtype':     zi_lin.dtype,
          'nodata':    -99999,
          'transform': aft}

    # output a geo-referenced tiff
    """
    dst = rio.open(filename, 'w', **md)
    try:
        dst.write(zi_lin, 1)
        # Wrote TIF file to {filename}
    except:
        print('Failed to write Tiff file')
        # Failed to write TIF file to {filename}
    dst.close()
    """
    with MemoryFile() as memfile:
        with memfile.open(**md) as mem:
            # Populate the input file with numpy array
            mem.write(zi_lin, 1)

            dst_profile = cog_profiles.get("deflate")
        
            cog_translate(
                mem,
                filename,
                dst_profile,
                in_memory=True,
                quiet=True,
            )

# Make output directory if it does not exist
def makeDirs(outputDir):
    # Create cogeo directory path
    if not os.path.exists(outputDir):
        mode = 0o777
        os.makedirs(outputDir, mode, exist_ok=True)
        logger.info('Made directory '+outputDir+ '.')
    else:
        logger.info('Directory '+outputDir+' already made.')

@logger.catch
def main(args):
    # get input variables from args
    inputDir = os.path.join(args.inputDir, '')
    outputDir = os.path.join(args.outputDir, '')
    inputFile = args.inputFile
    inputVariable = args.inputVariable
    outputDir = os.path.join(outputDir+"".join(inputFile[:-3].split('.')), '')

    # Remove old logger and start new logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'geotiffTime2cogs.log', level='DEBUG')

    adcircepsg = 'EPSG:4326'
    targetepsg = 'EPSG:4326'

    # Check to see if input directory exits and if it does create tiff
    if os.path.exists(inputDir+inputFile):
        # When error exit program
        logger.add(lambda _: sys.exit(1), level="ERROR")

        # Make output directory
        makeDirs(outputDir.strip())

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

        i = 0

        logger.info('Loop through each timestep in '+inputFile+' and regrid data')
        for timestep in nc.variables['time']:
            logger.info('Get file data time from '+inputFile)
            fileDateTime = str(timestep.values)
    
            outputFile = '_'.join(['_'.join(inputFile.split('.')[0:2]),inputVariable,fileDateTime+'.tiff'])

            logger.info('Get data for timestep in '+inputFile)
            advardict = adcirc_utilities.get_adcirc_slice(nc, inputVariable, i)

            if i == 0:
                logger.info('Start regid of timestep')
                interp_lin = Tri.LinearTriInterpolator(triang, advardict['data'])
                grid_zi = interp_lin(xxm, yym)
                mindex = np.where(grid_zi.mask == True)
                logger.info('Finish regid of timestep')
            else:
                logger.info('Start regid of timestep')
                interpolator = interpolate.LinearNDInterpolator(triangd, advardict['data'])
                grid_zi = interpolator((xxm, yym))
                grid_zi[mindex] = np.nan
                logger.info('Finish regid of timestep')

            logger.info('Start writing regridded data to tiff file: '+outputDir+outputFile)
            write_tif(rasdict, grid_zi, targetgrid, targetepsg, outputDir+outputFile)
            logger.info('Finish writing regridded data to tiff file: '+outputDir+outputFile)
 
            i = i + 1

    else:
         logger.info(inputFile+' does not exist')
         sys.exit(1)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir", required=True)
    parser.add_argument("--outputDIR", "--outputDir", help="Output directory path", action="store", dest="outputDir", required=True)
    parser.add_argument("--inputFILE", "--inputFile", help="Input file name", action="store", dest="inputFile", required=True)
    parser.add_argument("--inputPARAM", "--inputVariable", help="Input parameter", action="store", dest="inputVariable")

    args = parser.parse_args()
    main(args)

