# adcircTime2cogs
Produces COGs from from ADCIRC timeseries data, such as fort.63.

## Build
  cd build  
  docker build -t adcircTime2cogs:latest .

## Create Container

  To create a stand alone container for testing use the command shown below:

    docker run -ti --name adcirctime2cogs_latest --volume /directory/path/to/storage:/data/sj37392jdj28538 -d adcirctime2cogs /bin/bash

  After the container has been created, you can access it using the following command:

    docker exec -it adcirctime2cogs_latest bash

  To create tiffs and cogs you must first activate the conda enviroment using the following command:

    conda activate adcircTime2cogs

  Now you can run the command to create a tiff:

    python adcircTime2geotiffs.py --inputDIR /data/sj37392jdj28538/input --outputDIR /data/sj37392jdj28538/cogeo --inputFile fort.63.nc --inputVariable zeta

  and the command to create the cog file:

    python geotiffs2cogs.py --inputDIR /data/sj37392jdj28538/cogeo --finalDIR /data/sj37392jdj28538/final/cogeo --inputParam fort63

## Running in Kubernetes

When running the container in Kubernetes the command line for adcircTime2geotiffs.py is:

    conda run -n adcirctime2cogs python adcirc2geotiff.py --inputDIR /xxxx/xxxxxxxxxx/input --outputDIR /xxxx/xxxxxxxxxx/cogeo --inputFile fort.63.nc 

and the command line for geotiffs2cogs.py is:

    conda run -n adcirctime2cogs python geotiffs2cogs.py --inputDIR /xxxx/xxxxxxxxxx/cogeo --finalDIR /xxxx/xxxxxxxxxx/final/cogeo --inputParam fort63

Where /xxxx/xxxxxxxxxx would be a specified directory path.
 
