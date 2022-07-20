import os
import tarfile
def tardir(path, tar_name):
    with tarfile.open(tar_name, "w") as tar_handle:
        for root, dirs, files in os.walk(path):
            for file in files:
                tar_handle.add(os.path.join(root, file))

outputDir = '/Users/jmpmcman/Work/Surge/data/apsvizvolume/cogeo/'
inputFile = 'fort.63.nc'
outputFile = "".join(inputFile[:-3].split('.'))
os.chdir(outputDir)

tardir(outputFile, outputFile+'.tar')

