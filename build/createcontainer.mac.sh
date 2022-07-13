#!/bin/bash
# setup specific to apsviz-maps
version=$1;

docker run -ti --name adcirctime2cogs_$version \
  --volume /Users/jmpmcman/Work/Surge/data/apsvizvolume:/data/sj37392jdj28538 \
  -d adcirctime2cogs:$version /bin/bash 
