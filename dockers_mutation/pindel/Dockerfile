# Pindel Dockerfile
#
# 
# build with: sudo docker build --rm --no-cache -t pindel - < Dockerfile
# run with: sudo docker run -i -t pindel /bin/bash

#use ubuntu
FROM ubuntu

MAINTAINER Arjun Rao aarjunrao@soe.ucsc.edu

#get the essentials
RUN apt-get update
RUN apt-get install -y software-properties-common wget libgomp1 curl build-essential checkinstall

#install zlib for samtools
RUN curl -L http://zlib.net/zlib-1.2.8.tar.gz > zlib-1.2.8.tar.gz 
RUN tar -zxvf zlib-1.2.8.tar.gz ; rm zlib-1.2.8.tar.gz
RUN cd zlib-1.2.8 ; ./configure ; make install

#install ncurses for samtools
RUN wget -c http://ftp.gnu.org/pub/gnu/ncurses/ncurses-5.9.tar.gz
RUN tar -zxvf ncurses-5.9.tar.gz ; rm ncurses-5.9.tar.gz
RUN cd ncurses-5.9 ; ./configure ; make ; make install

#install samtools
RUN curl -L http://sourceforge.net/projects/samtools/files/latest/download?source=files > samtools.tar.bz2
RUN bunzip2 samtools.tar.bz2 ; tar -xvf samtools.tar; rm samtools.tar
RUN cd samtools* ; make; cp samtools bcftools/bcftools misc/* /usr/local/bin

# install pindel
RUN apt-add-repository "deb http://apt.genome.wustl.edu/ubuntu lucid-genome main"
RUN wget https://apt.genome.wustl.edu/ubuntu/files/genome-institute.asc
RUN apt-key add genome-institute.asc
RUN apt-get update
RUN apt-get install -y pindel0.2.4t

