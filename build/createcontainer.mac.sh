#!/bin/bash
# setup specific to apsviz-maps
version=$1;

docker run -ti --name adcirctime2cogs_$version \
  --volume /Users/jmpmcman/Work/Surge/data/apsvizvolume:/data \
  -d adcirctime2cogs:$version /bin/bash 
