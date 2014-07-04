FROM	ubuntu

RUN		apt-get update
RUN		apt-get install -y wget make g++ libboost-all-dev
RUN		cd /opt && wget http://downloads.sourceforge.net/project/defuse/defuse/0.6/defuse-0.6.2.tar.gz
RUN		cd /opt && tar xvzf defuse-0.6.2.tar.gz 
RUN		cd /opt/defuse-0.6.2/tools && make