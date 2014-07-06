
import sys
import subprocess
from subprocess import CalledProcessError
from subprocess import Popen
import string
from glob import glob
import os
    
#just for initial debug, these will be filled in by the conf read
#PYTHON="/pod/home/kellrott/bin/python"
#PYTHON="/pod/home/kellrott/bin/python"
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

def cghub_submit(UUID, NEW_UUID, BAM_FILE, ORIG_BAM_FILE, MD5, QC_STATS_FILE, NORMAL_UUID, NEW_NORMAL_UUID, UPLOAD_KEY, debug=False):

    #the submission directory
    CWD=os.getcwd()
    SUB_DIR="%s/%s.partial"%(CWD,UUID)
    FIN_DIR="%s/%s.FINISHED"%(CWD,UUID)

    if os.path.exists(FIN_DIR):
        sys.stderr.write("Upload already FINISHED\n")
        sys.exit(0)
    
    if not os.path.exists(SUB_DIR):
        os.mkdir(SUB_DIR)

    if not os.path.exists("%s/PAWG.%s.bam" % (SUB_DIR,UUID)):
        os.symlink(os.path.relpath(BAM_FILE, SUB_DIR),"%s/PAWG.%s.bam"%(SUB_DIR,UUID))

    #put metric compareing $ORIG_FILE and $BAM_FILE and save stats to $SUB_DIR
    try:
        if debug:
            cmd = "%s %s/realigned_bam_check -o %s -n %s -p %s" % (PYTHON,SCRIPT_DIR,ORIG_BAM_FILE,BAM_FILE,SUB_DIR)
        else:
            cmd = "realigned_bam_check -o %s -n %s -p %s" % (ORIG_BAM_FILE,BAM_FILE,SUB_DIR)
        (stdout,stderr)=run_command(cmd)
    except CalledProcessError as cpe:
        sys.stderr.write("Realignment Check error\n")
        raise cpe
    
    NEW_UUID=NEW_UUID.rstrip()    
    NORMAL_UUID=NORMAL_UUID.rstrip()    
    NEW_NORMAL_UUID=NEW_NORMAL_UUID.rstrip()
    
    with open(MD5,"r") as md5f:
        md5=md5f.readline()
        md5=md5.rstrip()
    
#create cghub validating metadata with ICGC specific metadata added to it
    if not os.path.exists("%s/%s" % (SUB_DIR,NEW_UUID)) or not os.path.exists("%s/%s/trans.map" % (SUB_DIR,UUID)):
        try:
            if debug:
                cmd = "%s %s/create_pawg_metadata -u %s -f PAWG.%s.bam -c %s -t %s -n %s -p %s" % (PYTHON,SCRIPT_DIR,UUID,UUID,md5,NEW_NORMAL_UUID,NEW_UUID,SUB_DIR)
            else:
                cmd = "create_pawg_metadata -u %s -f PAWG.%s.bam -c %s -t %s -n %s -p %s" % (UUID,UUID,md5,NEW_NORMAL_UUID,NEW_UUID,SUB_DIR)
            (stdout,stderr)=run_command(cmd)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub metadata creation error\n")
            raise cpe
        try:
            if debug:
                cmd = "%s %s/add_qc_results_to_metadata.pl %s/%s/analysis.xml %s" % (PERL,SCRIPT_DIR,SUB_DIR,NEW_UUID,QC_STATS_FILE)
            else:
                cmd = "add_qc_results_to_metadata.pl %s/%s/analysis.xml %s" % (SUB_DIR,NEW_UUID,QC_STATS_FILE)
            (stdout,stderr)=run_command(cmd)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub QC stats/ICGC fields addition to metadata error\n")
            raise cpe
        
    try:
        if debug:
            cmd = "curl -sk https://stage.cghub.ucsc.edu/cghub/metadata/analysisDetail?analysis_id=%s | egrep -ie '<state>' | cut -d'>' -f 2 | cut -d\"<\" -f 1" % (NEW_UUID)
        else:
            cmd = "curl -sk https://cghub.ucsc.edu/cghub/metadata/analysisDetail?analysis_id=%s | egrep -ie '<state>' | cut -d'>' -f 2 | cut -d\"<\" -f 1" % (NEW_UUID)
        (state,stderr)=run_command(cmd, SUB_DIR)
        state = state.rstrip()
    except CalledProcessError as cpe:
        sys.stderr.write("CGHub WSI query for state for %s failed\n" % (NEW_UUID))
        raise cpe

    if state is None or state == "":
        try:
            if debug:
                cmd = "%s %s/cgsubmit -s https://stage.cghub.ucsc.edu -c %s -u %s" % (PYTHON,SCRIPT_DIR,UPLOAD_KEY,NEW_UUID)
            else:
                cmd = "cgsubmit -c %s -u %s" % (UPLOAD_KEY,NEW_UUID)
            (stdout,stderr)=run_command(cmd, SUB_DIR)
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
        raise CalledProcessError(1,"state not live: %s" % (state))
    print "calling rename"        
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
        UPLOAD_KEY=UPLOAD_KEY,
        QC_STATS_FILE=bas_file,
        debug=True)



#STEPS=[realigned_bam_check,create_pawg_metadata,cgsubmit,gtupload]
STEPS=[cghub_submit_normal]
RESUME=False
STORE=False
IMAGE="pcap_tools"

def main():
    #ONLY USE test (non-protected) data with debug as it will upload to stage 
    cghub_submit(UUID='251916ec-f78e-4eae-99fe-ff802e3ce2fe',
                 NEW_UUID='d3afc141-bd34-41a5-bbab-4a65c3b0ec27',
                 NORMAL_UUID='a0963407-05e7-4c84-bfe0-34aacac08eed',
                 NEW_NORMAL_UUID='97112394-e3e6-4bf4-b4a6-6251fd80c711',
                 ORIG_BAM_FILE='/pod/home/cwilks/p/input/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam',
                 BAM_FILE='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam',
                 MD5='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam.md5',
                 QC_STATS_FILE='/pod/home/cwilks/p/output/251916ec-f78e-4eae-99fe-ff802e3ce2fe/251916ec-f78e-4eae-99fe-ff802e3ce2fe.bam.bas',
                 UPLOAD_KEY=UPLOAD_KEY,
                 debug=True)


if __name__ == '__main__':
    main()
