
from fabric.api import run, put
from fabric.contrib.files import exists

def install_docker():
	run("sudo apt-get update")
	run("sudo apt-get install -y linux-image-generic-lts-raring linux-headers-generic-lts-raring nfs-common")
	run("sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9")
	run("sudo sh -c 'echo deb http://get.docker.io/ubuntu docker main > /etc/apt/sources.list.d/docker.list'")
	run("sudo apt-get update")
	run("sudo apt-get install -y lxc-docker")
	run("sudo reboot")

def install_synapse():
	run("sudo apt-get install -y python-pip")
	run("sudo pip install synapseclient")
	run("touch ~/.synapseConfig")
	if not exists("~/.synapseCache"):
		run("mkdir ~/.synapseCache")
	put("~/.synapseCache/.session", "~/.synapseCache/.session")

def load_aligner():
	mount_pancanfs()
	run("cat /pancanfs/software/icgc-aligner.tar | sudo docker load")

def start_splitting():
	run("nohup /pancanfs/software/split_bams.sh >& ~/nohup.out < /dev/null &")

def ps():
	run("sudo docker ps")
	
def images():
	run("sudo docker images")

def df():
	run("df -h")

	
def mount_pancanfs():
	if not exists("/pancanfs"):
		run("sudo mkdir /pancanfs")
	if not exists("/pancanfs/software"):
		run("sudo mount	10.2.31.251:/pancanfs /pancanfs")
