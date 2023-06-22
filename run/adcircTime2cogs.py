#!/usr/bin/env python
import sys
import os
import argparse
import shutil
from loguru import logger

import pandas as pd
import numpy as np
import matplotlib.tri as Tri
from scipy import interpolate
from scipy.spatial import Delaunay

import dask # DASK
import dask.array as da # DASK

import geopandas as gpd
import dask_geopandas as dgp # DASK

import xarray as xr
from datacube.utils.cog import write_cog

from affine import Affine
from pyproj import CRS

from multiprocessing.pool import ThreadPool as Pool
#from multiprocessing import Pool
from multiprocessing import Queue as PQueue

import utilities.adcirc_dask_utilities as adcirc_utilities # DASK
#import utilities.adcirc_utilities as adcirc_utilities  # NODASK

import faulthandler; faulthandler.enable()

def construct_geopandas(agdict, targetepsg):
    """
    Define geopandas processors
    project grid coords, before making Triangulation object
    """

    df_Adcirc = pd.DataFrame({'Longitude': agdict['lon'],'Latitude': agdict['lat']})

    gdf = gpd.GeoDataFrame(df_Adcirc, geometry=gpd.points_from_xy(agdict['lon'], agdict['lat']))

    ddf = dgp.from_geopandas(gdf, npartitions=4) # DASK
    
    # init crs is LonLat, WGS84
    adcircepsg = agdict['crs']
    # Adding crs to initial GDF
    ddf.crs = CRS(adcircepsg) # DASK
    #gdf.crs = CRS(adcircepsg) # NODASK
    # Converting GDF
    ddf = ddf.to_crs(CRS(targetepsg)) # DASK
    #gdf = gdf.to_crs(CRS(targetepsg)) # NODASK
    return ddf # DASK
    return gdf # NODASK

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

    x = dask.array.arange(upperleft_x, lowerright_x[0], targetgrid['res'][0]) # DASK
    y = dask.array.arange(upperleft_y, lowerright_y[0], -targetgrid['res'][0]) # DASK
    #x = np.arange(upperleft_x, lowerright_x[0], targetgrid['res'][0]) # NODASK
    #y = np.arange(upperleft_y, lowerright_y[0], -targetgrid['res'][0]) # NODASK
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

class mesh2tiff:
    def __init__(self, inputDir, outputDir, finalDir, inputFile, inputVariable, numCPU):
        # Make input variables accessable throughout class
        self.inputFile = inputFile
        self.inputVariable = inputVariable

        # Creat output variable directory name
        self.outputVarDir = os.path.join(outputDir+"".join(inputFile[:-3].split('.'))+'_'+inputVariable+'_'+"_".join(inputDir.split('/')[2].split('-')), '')
        logger.info('Created outputVarDir '+self.outputVarDir+'.')

        # Make output directory
        makeDirs(self.outputVarDir.strip())

        # Creat final variable directory
        self.finalVarDir = finalDir+"".join(inputFile[:-3].split('.'))+'_'+inputVariable+'_'+"_".join(inputDir.split('/')[2].split('-'))
        logger.info('Created finalVarDir '+self.finalVarDir+'.')

        # Make final cogeo directory
        makeDirs(finalDir.strip())

        logger.info('Read '+inputDir+self.inputFile+' and create agdict')
        self.nc, self.agdict = adcirc_utilities.extract_url_grid(inputDir+self.inputFile)

        self.adcircepsg = 'EPSG:4326'
        self.targetepsg = 'EPSG:4326'
        self.agdict['crs'] = self.adcircepsg

        logger.info('Define targetgrid')
        targetgrid = {'res':[0.005],
                      'nx':[8838],
                      'ny':[8000],
                      'theta':[0]}
        targetgrid['ul_latitude'] = self.agdict['lat'].max()
        targetgrid['ul_longitude'] = self.agdict['lon'].min()
        targetgrid['lr_longitude'] = self.agdict['lon'].max()
        targetgrid['lr_latitude'] = self.agdict['lat'].min()

        logger.info('Create geopandas DataFrame from agdict and tartgetepsg')
        gdf = construct_geopandas(self.agdict, self.targetepsg)
        xtemp, ytemp = gdf['geometry'].x, gdf['geometry'].y

        logger.info('Compute geotiff grid coordinates')
        self.rasdict = compute_geotiff_grid(targetgrid, self.adcircepsg, self.targetepsg)
        self.xxm, self.yym = self.rasdict['xxm'].rechunk(4000,4419,2), self.rasdict['yym'].rechunk(4000,4419,2) # DASK
        #self.xxm, self.yym = self.rasdict['xxm'], self.rasdict['yym'] # NODASK

        logger.info('Computer ones for mask')
        advardict = adcirc_utilities.get_adcirc_slice(self.nc, inputVariable, 0)
        z_ones = np.ones((len(advardict['data']),), dtype=float)

        logger.info('Create triang using Tri.Triangulation')
        triang = Tri.Triangulation(xtemp, ytemp, triangles=self.agdict['ele'])
        triang.triangles = da.from_array(triang.triangles).rechunk(445513,8) # DASK
        triang.x = da.from_array(triang.x).rechunk(302240,6) # DASK
        triang.y = da.from_array(triang.y).rechunk(302240,6) # DASK
        #triang.triangles = np.array(triang.triangles) #.rechunk(445513,8) # NODASK
        #triang.x = np.array(triang.x) #.rechunk(302240,6) # NODASK
        #triang.y = np.array(triang.y) #.rechunk(302240,6) # NODASK

        logger.info('Create traingd using Delaunay')
        self.triangd = Delaunay(np.stack((self.agdict['lon'].values, self.agdict['lat'].values), axis=1)) # DASK
        #self.triangd = Delaunay(np.stack((self.agdict['lon'].data, self.agdict['lat'].data), axis=1)) # NODASK

        logger.info('Create onesinterp_lin')
        onesinterp_lin = Tri.LinearTriInterpolator(triang, z_ones)
        logger.info('Compute ones')
        ones_z = onesinterp_lin(self.xxm,self.yym)

        self.mindex = np.where(ones_z.mask == True)

        # Define input_list times index
        inputs_list = []
        i = 0

        logger.info('Loop through each timestep in '+self.inputFile+' and regrid data')
        for timestep in self.nc.variables['time']:
            logger.info('Get file data time from '+self.inputFile)
            fileDateTime = "".join("".join(str(timestep.values).split('-')).split(':')).split('.')[0]+'Z' # DASK
            #fileDateTime = "".join("".join(str(timestep.data).split('-')).split(':')).split('.')[0]+'Z' # NODASK

            # Create outputFile name
            outputFile = '_'.join(['_'.join(self.inputFile.split('.')[0:2]),inputVariable,fileDateTime+'.tiff'])
            logger.info('Created outputFile name: '+outputFile+'.')

            inputs_list.append([fileDateTime, i, outputFile])

            i = i + 1

        # Define queue
        input_q = PQueue()

        if numCPU is None:
            cpus = 4
            chunkSize = int(len(self.nc.variables['time'])/cpus) 
        else:
            cpus = int(numCPU)
            chunkSize = int(len(self.nc.variables['time'])/cpus)

        # Run regrid2Raster using multiprocessinng pool, and imput_list
        logger.info('Run regrid2Raster in Pool, with inputs_list')
        with Pool(processes=3) as pool:
            try:
               #outputs_list = pool.map(self.regrid2Raster, [(input_q, input_list) for input_list in inputs_list], chunksize=28) 
               outputs_list = pool.map(self.regrid2Raster, [(input_q, input_list) for input_list in inputs_list], chunksize=28) 
               logger.info(outputs_list)
            except Exception as e:
               logger.error(e)
            finally:
               pool.close()
               pool.terminate()
               pool.join()

        logger.info('Create meta file for timeseries mosaic COGs')
        f = open(self.outputVarDir+'indexer.properties', 'w')
        f.write('TimeAttribute=ingestion\nElevationAttribute=elevation\nSchema=*the_geom:Polygon,location:String,ingestion:java.util.Date,elevation:Integer\nPropertyCollectors=TimestampFileNameExtractorSPI[timeregex](ingestion)\n')
        f.close()

        f = open(self.outputVarDir+'timeregex.properties', 'w')
        f.write('regex=[0-9]{8}T[0-9]{6}\n')
        f.close()

        f = open(self.outputVarDir+'datastore.properties', 'w')
        f.write('SPI=org.geotools.data.postgis.PostgisNGDataStoreFactory\nhost='+os.environ['ASGS_DB_HOST']+'\nport='+os.environ['ASGS_DB_PORT']+'\ndatabase='+os.environ['COG_MOSAIC_DATABASE']+'\nschema=public\nuser='+os.environ['COG_MOSAIC_USERNAME']+'\npasswd='+os.environ['COG_MOSAIC_PASSWORD']+'\nLoose\ bbox=true\nEstimated\ extends=false\nvalidate\ connections=true\nConnection\ timeout=10\npreparedStatements=true\n')
        f.close()

        # move output directory to final directory
        shutil.move(self.outputVarDir, self.finalVarDir)
        logger.info('Moved output directory: '+self.outputVarDir+' to final directory: '+self.finalVarDir)

    def regrid2Raster(self, inputList):
        fileDateTime = inputList[1][0]
        time_index = inputList[1][1]
        outputFile = inputList[1][2]

        logger.info('Get data for timestep '+fileDateTime+' in '+self.inputFile)
        advardict = adcirc_utilities.get_adcirc_slice(self.nc, self.inputVariable, time_index)

        logger.info('Start regrid of timestepp: '+fileDateTime)
        interpolator = interpolate.LinearNDInterpolator(self.triangd, advardict['data'])
        interpolator((0, 0))

        grid_zi = interpolator((self.xxm, self.yym))
        grid_zi[self.mindex] = np.nan
        logger.info('Finish regrid of timestepp: '+fileDateTime)

        logger.info('Start writing regridded data to tiff file: '+self.outputVarDir+outputFile)
        zi_data = create_xarray(self.rasdict, grid_zi, self.targetepsg)
        write_cog(geo_im=zi_data, fname=self.outputVarDir+outputFile, overwrite=True)
        logger.info('Finish writing regridded data to tiff file: '+self.outputVarDir+outputFile)

@logger.catch
def main(inputDir, outputDir, finalDir, inputFile, inputVariable, numCPU):
    # Define tmp directory
    tmpDir = "/".join(inputDir.split("/")[:-2])+"/"+inputFile.split('.')[0]+"_"+inputVariable+"_dask_tmp/" # DASK

    # Make tmpDir 
    os.makedirs(tmpDir, exist_ok=True)

    # Config DASK to use tmpDir
    dask.config.set(temporary_directory=tmpDir) # DASK
    logger.info('Configure tmp directory for DASK: '+tmpDir)

    mesh2tiff(inputDir, outputDir, finalDir, inputFile, inputVariable, numCPU)

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir", required=True)
    parser.add_argument("--outputDIR", "--outputDir", help="Output directory path", action="store", dest="outputDir", required=True)
    parser.add_argument("--finalDIR", "--finalDir", help="Final directory path", action="store", dest="finalDir", required=True)
    parser.add_argument("--inputFILE", "--inputFile", help="Input file name", action="store", dest="inputFile", required=True)
    parser.add_argument("--inputPARAM", "--inputVariable", help="Input parameter", action="store", dest="inputVariable", required=True)
    parser.add_argument("--numCPU", "--numCpu", help="Number of CPUs", action="store", dest="numCPU", required=False)

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
    outputDir = os.path.join(args.outputDir, '')
    finalDir = os.path.join(args.finalDir, '')
    inputFile = args.inputFile
    inputVariable = args.inputVariable
    numCPU = args.numCPU

    if os.path.exists(inputDir+inputFile):
         main(inputDir, outputDir, finalDir, inputFile, inputVariable, numCPU)
    else:
         logger.info(inputDir+inputFile+' does not exist')
         if inputFile.startswith("swan"):
             logger.info('The input file is a swan file : '+inputDir+inputFile+' so do a soft exit')
             sys.exit(0)
         else:
             logger.info('The input file is not a swan file : '+inputDir+inputFile+' so do a hard exit')
             sys.exit(1)
