#!/usr/bin/env python
import os, sys, time, tarfile, shutil
from loguru import logger

# Remove old logger and start new logger
logger.remove()
log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
logger.add(log_path+'tarTest.log', level='DEBUG')
logger.add(sys.stdout, level="DEBUG")
logger.add(sys.stderr, level="ERROR")
logger.info('Started log file tarTest.log')

# Make output directory if it does not exist
def makeDirs(dirPath):
    # Create directory path
    if not os.path.exists(dirPath):
        mode = 0o777
        os.makedirs(dirPath, mode, exist_ok=True)
        logger.info('Made directory '+dirPath+ '.')
    else:
        logger.info('Directory '+dirPath+' already made.')

def tardir(path, tar_name):
    with tarfile.open(tar_name, "w") as tar_handle:
        for root, dirs, files in os.walk(path):
            for file in files:
                tar_handle.add(os.path.join(root, file))

runDir = '/home/nru/repos/adcircTime2cogs/run'
outputDir = '/data/sj37392jdj28538/cogeo/'
finalDir = '/data/sj37392jdj28538/final/cogeo/'
inputFile = 'fort.63.nc'
outputTarFile = "".join(inputFile[:-3].split('.'))+'.tar'

os.chdir(outputDir)

logger.info('Start taring file')
start = time.time()
tardir(outputTarFile.split('.')[0], outputTarFile)
logger.info(time.time()-start)
logger.info('Finnish taring file')

try:
    shutil.rmtree(outputTarFile.split('.')[0])
    logger.info('Removed variable directory: '+outputTarFile.split('.')[0])
except OSError as err:
    logger.error('Problem removing variable directory '+outputTarFile.split('.')[0])
    sys.exit(1)

# Create final directory path
logger.info('Make final directory')
makeDirs(finalDir.strip())

# Move cogs to final directory
try:
    shutil.move(outputTarFile, finalDir)
    logger.info('Moved cog file '+finalPathFile.split("/")[-1]+' to '+finalDir+' directory.')
except OSError as err:
    logger.error('Failed to move cog file '+finalPathFile.split("/")[-1]+' to '+finalDir+' directory.')
    sys.exit(1)

os.chdir(runDir)
