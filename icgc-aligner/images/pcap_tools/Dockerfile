
FROM	ubuntu:12.10

RUN		apt-get update
RUN		apt-get install -y zlib1g-dev g++ dh-autoreconf libncurses-dev pkg-config git libgd2-noxpm-dev unzip python python-dateutil libipc-system-simple-perl

RUN		mkdir /opt/ICGC
RUN		cd /opt && git clone https://github.com/kellrott/PCAP-core.git 
RUN		useradd -u 1000 -g root -d /home/aligner -m -s /bin/bash aligner 
RUN		chown -R aligner /opt/*
RUN		cd /opt/PCAP-core/ && export HOME=/home/aligner &&  sudo -u aligner ./setup.sh /opt/ICGC/
ADD		cgquery /opt/ICGC/bin/

ENV		PERL5LIB /opt/ICGC/lib/perl5/
ENV		PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/ICGC/bin
