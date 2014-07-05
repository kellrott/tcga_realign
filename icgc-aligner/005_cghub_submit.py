
import sys
import subprocess
from subprocess import CalledProcessError
from subprocess import Popen
import string
from glob import glob
import os
    
#just for initial debug, these will be filled in by the conf read
PYTHON="/usr/bin/python"
SCRIPT_DIR="/opt/pyscripts"
PERL = "/usr/bin/perl"
#UPLOAD_KEY = "/keys/UCSC_PAWG.key"
UPLOAD_KEY = "/keys/JOSH_PAWG_stage.key"

def run_command(command=str, cwd=None):
    print "Running: %s" % (command)
    run=Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,cwd=cwd)
    (stdout,stderr)=run.communicate()
    if run.returncode != 0:
        print "ERROR:\t" + stdout
        print "ERROR:\t" + stderr #line.rstrip()
        #sys.exit(-1)
        raise CalledProcessError(run.returncode,stderr)
    return (stdout,stderr)

def cghub_submit(UUID, NEW_UUID, BAM_FILE, ORIG_BAM_FILE, MD5, QC_STATS_FILE, NORMAL_UUID, NEW_NORMAL_UUID):

    #the submission directory
    SUB_DIR="%s.partial"%(UUID)
    FIN_DIR="%s"%(UUID)

    if os.path.exists(FIN_DIR):
        sys.stderr.write("Upload already FINISHED\n")
        sys.exit(0)
    
    if not os.path.exists(SUB_DIR):
        os.mkdir(SUB_DIR)

    if not os.path.exists("%s/PAWG.%s.bam" % (SUB_DIR,UUID)):
        os.symlink(os.path.relpath(BAM_FILE, SUB_DIR),"%s/PAWG.%s.bam"%(SUB_DIR,UUID))

    #put metric compareing $ORIG_FILE and $BAM_FILE and save stats to $SUB_DIR
    try:
        cmd = "%s %s/realigned_bam_check -o %s -n %s -p %s" % (PYTHON,SCRIPT_DIR,ORIG_BAM_FILE,BAM_FILE,SUB_DIR)
        (stdout,stderr)=run_command(cmd)
    except CalledProcessError as cpe:
        sys.stderr.write("Realignment Check error\n")
        raise cpe
    
    NEW_UUID=NEW_UUID.rstrip()    
    NORMAL_UUID=NORMAL_UUID.rstrip()    
    NEW_NORMAL_UUID=NEW_NORMAL_UUID.rstrip()    
#create cghub validating metadata with ICGC specific metadata added to it
    if not os.path.exists("%s/%s" % (SUB_DIR,NEW_UUID)) or not os.path.exists("%s/%s/trans.map" % (SUB_DIR,UUID)):
        try:
            cmd = "%s %s/create_pawg_metadata -u %s -f PAWG.%s.bam -c %s -t %s -n %s -p %s" % (PYTHON,SCRIPT_DIR,UUID,UUID,MD5,NEW_NORMAL_UUID,NEW_UUID,SUB_DIR)
            (stdout,stderr)=run_command(cmd)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub metadata creation error\n")
            raise cpe
        try:
            cmd = "%s %s/add_qc_results_to_metadata.pl %s/analysis.xml %s" % (PERL,SCRIPT_DIR,NEW_UUID,QC_STATS_FILE)
            (stdout,stderr)=run_command(SUB_DIR, cmd)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub QC stats/ICGC fields addition to metadata error\n")
            raise cpe
        
    try:
        cmd = "curl -sk https://cghub.ucsc.edu/cghub/metadata/analysisDetail?analysis_id=%s | egrep -ie '<state>' | cut -d'>' -f 2 | cut -d\"<\" -f 1" % (NEW_UUID)
        (state,stderr)=run_command(cmd, SUB_DIR)
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
            cmd = "gtupload -c %s -u %s/manifest.xml -vv 2>%s/upload.stderr.log" % (UPLOAD_KEY,NEW_UUID,SUB_DIR)
            (stdout,stderr)=run_command(cmd, SUB_DIR)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub file upload error, check error log %s/upload.stderr.log\n" % SUB_DIR)
            raise cpe
    elif state != "live":
        sys.stderr.write("not in a submitting/uploading state, but also not live, CHECK THIS ONE\n")
        raise CalledProcessError(1,"state not live")
            
    os.rename(SUB_DIR,FIN_DIR)

def cghub_submit_normal(params):
    bas_file = "PAWG.%s.bas" % ( params['normal_id'] )
    cghub_submit(UUID=params['normal_id'], 
        NEW_UUID=params['new_normal_id'],
        NORMAL_UUID=params['normal_id'],
        NEW_NORMAL_UUID=params['new_normal_id'],
        ORIG_BAM_FILE=params['normal_bam'], 
        BAM_FILE=params['normal_merged'],
        MD5=params['normal_merged'] + ".md5",
        QC_STATS_FILE=bas_file)



#STEPS=[realigned_bam_check,create_pawg_metadata,cgsubmit,gtupload]
STEPS=[cghub_submit_normal]
RESUME=False
STORE=False
IMAGE="pcap_tools"

def main():
    params={}
    params['UUID']='251916ec-f78e-4eae-99fe-ff802e3ce2fe'
    params['ORIG_BAM_FILE']='/pod/home/cwilks/p/input/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam'    
    params['BAM_FILE']='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam'
    params['QC_STATS_FILE']='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam.bas'
    params['BAM_MD5']='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam.md5'
    
    cghub_submit(params)

if __name__ == '__main__':
    main()
