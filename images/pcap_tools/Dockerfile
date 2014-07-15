
FROM	ubuntu:12.04

RUN		apt-get update
RUN		apt-get install -y zlib1g-dev g++ dh-autoreconf libncurses-dev pkg-config git libgd2-noxpm-dev unzip python python-dateutil libipc-system-simple-perl python-lxml libjson-perl

#Get PCAP Tools
RUN		mkdir /opt/ICGC
RUN		cd /opt && git clone https://github.com/kellrott/PCAP-core.git
RUN		cd /opt/PCAP-core/ && ./setup.sh /opt/ICGC/

#Install GeneTorrent
RUN		apt-get install -y wget

RUN		cd /opt && wget --no-check-certificate https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.5a/genetorrent-download_3.8.5a-ubuntu2.94-12.04_amd64.deb
RUN		cd /opt && wget --no-check-certificate https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.5a/genetorrent-common_3.8.5a-ubuntu2.94-12.04_amd64.deb
RUN		cd /opt && wget --no-check-certificate https://cghub.ucsc.edu/software/downloads/GeneTorrent/3.8.5a/genetorrent-upload_3.8.5a-ubuntu2.94-12.04_amd64.deb
RUN		apt-get install -y libcurl3 libxqilla6 python
RUN		apt-get install -y libboost-program-options1.48.0 libboost-system1.48.0  libboost-filesystem1.48.0 libboost-regex1.48.0

RUN		cd /opt && dpkg --install genetorrent-download_3.8.5a-ubuntu2.94-12.04_amd64.deb genetorrent-common_3.8.5a-ubuntu2.94-12.04_amd64.deb genetorrent-upload_3.8.5a-ubuntu2.94-12.04_amd64.deb

ADD		cgquery /opt/ICGC/bin/
ADD		pyscripts /opt/pyscripts
ADD		synapseICGCMonitor /opt/ICGC/bin/

ENV		PERL5LIB /opt/ICGC/lib/perl5/
ENV		PATH /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/ICGC/bin
