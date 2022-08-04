#!/bin/bash
# setup specific to apsviz-maps
version=$1;

docker run -ti --name adcirctime2cogs_$version \
  --volume /d/dvols/apzviz:/data \
  -d adcirctime2cogs:$version /bin/bash 
