#!/bin/bash
version=$1;

docker build -t adcirctime2cogs:$version .
