
FROM java

RUN apt-get update
RUN apt-get install -y zip curl git python wget
#
# Install samtools
#
RUN apt-get install -y samtools

#
# Install mutation call wrapper
#
RUN	cd /opt && git clone https://github.com/kellrott/mutcall_wrapper.git
RUN	cd /opt/mutcall_wrapper/tools && sh ./mutect_install.sh

