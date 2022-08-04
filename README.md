# adcircTime2cogs
Produces COGs from from ADCIRC timeseries data, such as fort.63.

## Build
  cd build  
  docker build -t adcircTime2cogs:latest .

## Create Container

  To create a stand alone container for testing use the command shown below:

    docker run -ti --name adcirctime2cogs_latest --volume /directory/path/to/storage:/data/4221-2022080406-namforecast -d adcirctime2cogs /bin/bash

  After the container has been created, you can access it using the following command:

    docker exec -it adcirctime2cogs_latest bash

  To create tiffs and cogs you must first activate the conda enviroment using the following command:

    conda activate adcircTime2cogs

  Now you can run the command to create the COGs:

    python adcircTime2cogs.py --inputDIR /data/4221-2022080406-namforecast/input --outputDIR /data/4221-2022080406-namforecast/cogeo --finalDIR /data/4221-2022080406-namforecast/final/cogeo --inputFile fort.63.nc --inputVariable zeta

## Running in Kubernetes

When running the container in Kubernetes the command line for adcircTime2cogs.py is:

    conda run -n adcircTime2cogs python adcircTime2cogs.py --inputDIR /data/xxxxxxxxxx/input --outputDIR /data/xxxxxxxxxx/cogeo --finalDIR /data/xxxxxxxxxx/final/cogeo --inputFile fort.63.nc  --inputVariable zeta

Where xxxxxxxxxx would be a specified model run name in the directory path.
 
