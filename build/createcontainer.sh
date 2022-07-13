#!/bin/bash
# setup specific to apsviz-maps
version=$1;

docker run -ti --name adcirctime2cogs_$version \
  --volume /d/dvols/apzviz:/data/sj37392jdj28538 \
  -d adcirctime2cogs:$version /bin/bash 
