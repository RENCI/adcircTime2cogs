##############
# Docker file for the creation of the adcirc timeseries geotiff and cog files.
#
# to create image: docker build -t adcircTime2cogs:latest .
# to push image:
#       docker tag adcircTime2cogs:latest renciorg/adcircTime2cogs:latest
#       docker push renciorg/adcircTime2cogs:latest
##############
FROM continuumio/miniconda3

# author
MAINTAINER Jim McManus

# extra metadata
LABEL version="1.0"
LABEL description="adcircTime2cogs image with Dockerfile."

# update sources list, and install basic apps, one per line for better caching
RUN apt-get clean && apt-get --allow-releaseinfo-change update &&\
    apt-get install -qy vim \
    libgl1 \
    libgl1-mesa-dev \
    aria2

# update conda and set the download channel
RUN conda update conda && \
    conda config --add channels conda-forge

# add user nru
RUN useradd -m -d /home/nru -u 1000 nru
RUN chmod 777 /home/nru
USER nru

# make app directory in nru
RUN mkdir /home/nru/app
WORKDIR /home/nru/app
RUN chmod 777 -R .

# Create the environment, initialize conda in bash config files, and install repos:
COPY environment.yml .
RUN conda env create -f environment.yml

# make d directory for the repos and go there
RUN mkdir /home/nru/repos
WORKDIR /home/nru/repos
RUN chmod 777 -R .

# get the repo
RUN git clone https://github.com/RENCI/adcircTime2cogs.git

# change to the run directory                        
WORKDIR /home/nru/repos/adcircTime2cogs/run

# set the python path
ENV PYTHONPATH=/home/nru/repos/adcircTime2cogs

# tell conda what the shell is
RUN conda init bash
