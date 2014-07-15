
FROM base

RUN apt-get update
RUN apt-get install -y software-properties-common

#
# Install general system tools
#
RUN apt-get install -y zip curl git python

#
# Install Oracle Java 7
#
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update && apt-get -y upgrade
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get -y install oracle-java7-installer && apt-get clean
RUN update-alternatives --display java
ENV JAVA_HOME /usr/lib/jvm/java-7-oracle


#
# Install samtools
#
RUN apt-get install -y samtools

#
# Install mutation call wrapper
#
RUN	cd /opt && git clone https://github.com/kellrott/mutcall_wrapper.git
RUN	cd /opt/mutcall_wrapper/tools && sh ./mutect_install.sh

