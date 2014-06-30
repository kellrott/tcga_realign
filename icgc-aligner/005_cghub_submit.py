
import sys
import subprocess
from subprocess import CalledProcessError
from subprocess import Popen
import string
from glob import glob
import os
	
#just for initial debug, these will be filled in by the conf read
PYTHON="/pod/opt/bin/python"
SCRIPT_DIR="/pod/home/cwilks/p/tcga_realign_new/pyscripts"
PERL = "/pod/home/kellrott/opt/perl/bin/perl"
#UPLOAD_KEY = "/pod/home/cwilks/UCSC_PAWG.key"
UPLOAD_KEY = "/pod/home/cwilks/JOSH_PAWG_stage.key"

def run_command(command=str):
    print "Running: %s" % (command)
    run=Popen(["-c",command],stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    (stdout,stderr)=run.communicate()
    if run.returncode != 0:
        for line in stderr.split("\n"):
            print "ERROR:\t"+line.rstrip()
        #sys.exit(-1)
	raise CalledProcessError(run.returncode,stderr)
    return (stdout,stderr)

def cghub_submit(params):
	VOLUME=params['volume']
	UUID=params['UUID']
	#the realigned BAM file
	BAM_FILE=params['BAM_FILE']
	#the BAM file downloaded from CGHub
	ORIG_FILE=params['ORIG_BAM_FILE']	
	MD5=params['BAM_MD5']
	QC_STATS_FILE=params['QC_STATS_FILE']

	#the submission directory
	SUB_DIR="%s/submit/%s.partial"%(VOLUME,UUID)
	FIN_DIR="%s/submit/%s"%(VOLUME,UUID)

	if os.path.exists(FIN_DIR):
		sys.stderr.write("Upload already FINISHED\n")
		sys.exit(0)
	
	if not os.path.exists(SUB_DIR):
		os.mkdir(SUB_DIR)

	if not os.path.exists("%s/PAWG.%s.bam" % (SUB_DIR,UUID)):
		os.symlink(BAM_FILE,"%s/PAWG.%s.bam"%(SUB_DIR,UUID))

	#put metric compareing $ORIG_FILE and $BAM_FILE and save stats to $SUB_DIR
	try:
		cmd = "%s %s/realigned_bam_check -o %s -n %s -p %s" % (PYTHON,SCRIPT_DIR,ORIG_FILE,BAM_FILE,SUB_DIR)
		(stdout,stderr)=run_command(cmd)
	#	subprocess.check_call(cmd, shell=True)
	except CalledProcessError as cpe:
        	sys.stderr.write("Realignment Check error\n")
		raise cpe
	try:
		cmd = "%s %s/synapseICGCMonitor getInfo %s --get-normal" % (PYTHON,SCRIPT_DIR,UUID)
		(NORMAL_UUID,stderr)=run_command(cmd)
	except CalledProcessError as cpe:
        	sys.stderr.write("Failed to get normal uuid from Synapse error\n")
		raise cpe
	try:
		cmd = "%s %s/synapseICGCMonitor getResultID %s" % (PYTHON,SCRIPT_DIR,NORMAL_UUID)
		(NEW_NORMAL_UUID,stderr)=run_command(cmd)
	except CalledProcessError as cpe:
        	sys.stderr.write("Failed to get new normal uuid from Synapse error\n")
		raise cpe
	try:
		cmd = "%s %s/synapseICGCMonitor getResultID %s" % (PYTHON,SCRIPT_DIR,UUID)
		(NEW_UUID,stderr)=run_command(cmd)
	except CalledProcessError as cpe:
        	sys.stderr.write("Failed to get new uuid from Synapse error\n")
		raise cpe
	NEW_UUID=NEW_UUID.rstrip()	
	NORMAL_UUID=NORMAL_UUID.rstrip()	
	NEW_NORMAL_UUID=NEW_NORMAL_UUID.rstrip()	
#create cghub validating metadata with ICGC specific metadata added to it
	if not os.path.exists("%s/%s" % (SUB_DIR,NEW_UUID)) or not os.path.exists("%s/%s/trans.map" % (SUB_DIR,UUID)):
		try:
			cmd = "cd %s ; %s %s/create_pawg_metadata -u %s -f PAWG.%s.bam -c %s -p %s -t %s -n %s" % (SUB_DIR,PYTHON,SCRIPT_DIR,UUID,UUID,MD5,SUB_DIR,NEW_NORMAL_UUID,NEW_UUID)
			(stdout,stderr)=run_command(cmd)
		except CalledProcessError as cpe:
       		 	sys.stderr.write("CGHub metadata creation error\n")
			raise cpe
		try:
			cmd = "cd %s ; %s %s/add_qc_and_icgc_metadata_to_analysis.pl %s/%s/analysis.xml %s" % (SUB_DIR,PERL,SCRIPT_DIR,SUB_DIR,NEW_UUID,QC_STATS_FILE)
			(stdout,stderr)=run_command(cmd)
		except CalledProcessError as cpe:
        		sys.stderr.write("CGHub QC stats/ICGC fields addition to metadata error\n")
			raise cpe
		
	try:
		cmd = "cd %s ; curl -sk https://cghub.ucsc.edu/cghub/metadata/analysisDetail?analysis_id=%s | egrep -ie '<state>' | cut -d'>' -f 2 | cut -d\"<\" -f 1" % (SUB_DIR,NEW_UUID)
		(state,stderr)=run_command(cmd)
	except CalledProcessError as cpe:
        	sys.stderr.write("CGHub WSI query for state for %s failed\n" % (NEW_UUID))
		raise cpe

	if state is None or state == "":
		try:
			cmd = "%s %s/cgsubmit -c %s -u %s" % (PYTHON,SCRIPT_DIR,UPLOAD_KEY,NEW_UUID)
			(stdout,stderr)=run_command(cmd)
		except CalledProcessError as cpe:
       	 		sys.stderr.write("CGHub metadata submission error\n")
			raise cpe
					
	if state is None or state == "" or state == "submitted" or state == "uploading":
		try:
			cmd = "cd %s ; gtupload -c %s -u %s/manifest.xml -vv 2>%s/upload.stderr.log" % (SUB_DIR,UPLOAD_KEY,NEW_UUID,SUB_DIR)
			(stdout,stderr)=run_command(cmd)
		except CalledProcessError as cpe:
       	 		sys.stderr.write("CGHub file upload error, check error log %s/upload.stderr.log\n" % SUB_DIR)
			raise cpe
	elif state != "live":
		sys.stderr.write("not in a submitting/uploading state, but also not live, CHECK THIS ONE\n")
		raise CalledProcessError(1,"state not live")
			
	os.rename(SUB_DIR,FIN_DIR)

#STEPS=[realigned_bam_check,create_pawg_metadata,cgsubmit,gtupload]
STEPS=cghub_submit
RESUME=True
STORE=False
IMAGE="gtupload"

def main():
	params={}
	params['volume']='/pod/home/cwilks/p'
	params['UUID']='251916ec-f78e-4eae-99fe-ff802e3ce2fe'
	params['ORIG_BAM_FILE']='/pod/home/cwilks/p/input/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam'	
	params['BAM_FILE']='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam'
	params['QC_STATS_FILE']='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam.bas'
	params['BAM_MD5']='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam.md5'
	
	cghub_submit(params)

if __name__ == '__main__':
	main()
