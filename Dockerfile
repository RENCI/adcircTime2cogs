##############
# Docker file for the creation of the ADCIRC Time to COG image.
# stage 1: get a conda virtual environment
##############
FROM continuumio/miniconda3 as build

# get some credit
LABEL maintainer="Jim McManus (jmpmcman@renci.org)"

# extra metadata
LABEL version="v0.0.1"
LABEL description="ADCIRC Time to COG Dockerfile."

# update conda
RUN conda update conda

# Create the virtual environment
COPY build/environment.yml .
RUN conda env create -f environment.yml

# install conda pack to compress this stage
RUN conda install -c conda-forge conda-pack

# conpress the virtual environment
RUN conda-pack -n adcircTime2cogs -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar

# fix up the paths
RUN /venv/bin/conda-unpack

##############
# stage 2: create a python implementation using the stage 1 virtual environment
##############
FROM python:3.9-slim

RUN apt-get update

# install wget and bc
RUN apt-get install -y wget bc

# clear out the apt cache
RUN apt-get clean

# add user nru and switch to it
RUN useradd --create-home -u 1000 nru
USER nru

# Create a directory for the log
RUN mkdir -p /home/nru/adcircTime2cogs/logs

# move the the code location
WORKDIR /home/nru/adcircTime2cogs

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# make the virtual environment active
ENV VIRTUAL_ENV /venv
ENV PATH /venv/bin:$PATH

# Copy in the rest of the code
COPY run/utilities run/utilities
COPY run/adcircTime2cogs.py run

# set the python path
ENV PYTHONPATH=/home/nru/adcircTime2cogs/run

# set the location of the output directory
ENV RUNTIMEDIR=/data
ENV PKLDIR=/data/pkldir

# set the log dir. use this for debugging if desired
ENV LOG_PATH=/data/logs

# example command line
# python run/adcircTime2cogs.py --inputDIR /data/4271-33-nhcOfcl/cogeo --outputDIR /data/4271-33-nhcOfcl/cogeo --finalDIR /data/4271-33-nhcOfcl/final/cogeo --inputFile fort.63.nc --inputVariable zeta
