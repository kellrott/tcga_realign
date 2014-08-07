import sys
import subprocess
from subprocess import CalledProcessError
from subprocess import Popen
import string
from glob import glob
import os
import uuid

#just for initial debug, these will be filled in by the conf read
#used currently primarily to test different paths, the destination repo server and study is the same (cghub prod:PCAWG_TEST)
DEBUG_PYTHON="/pod/opt/bin/python"
DEBUG_SCRIPT_DIR="/pod/home/cwilks/p/tcga_realign_merged/dockers/pcap_tools/pyscripts"
#DEBUG_UPLOAD_KEY="/pod/home/cwilks/JOSH_PAWG_stage.key"
DEBUG_UPLOAD_KEY="/pod/home/cwilks/UCSC_PAWG.key"
DEBUG_PERL="/pod/home/cwilks/p/myperl/perl"

PYTHON="/usr/bin/python"
SCRIPT_DIR="/opt/pyscripts"
PERL = "/usr/bin/perl"
UPLOAD_KEY = "/keys/UCSC_PAWG.key"

REPO_SERVER='https://cghub.ucsc.edu'


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

#def cghub_submit(UUID, NEW_UUID, BAM_FILE, ORIG_BAM_FILE, MD5, QC_STATS_FILE, NORMAL_UUID, NEW_NORMAL_UUID, UPLOAD_KEY, mode, params, debug=False):
def cghub_submit(UUID, NEW_UUID, BAM_FILE, ORIG_BAM_FILE, MD5, NORMAL_UUID, NEW_NORMAL_UUID, UPLOAD_KEY, mode, params, test=0, debug=False):

    download_timing = params["%s_download_timing" % mode]
    merged_metrics = params["%s_merged_metrics" % mode]
    #merged_metrics = params["%s.markdup.metrics" % UUID]
    #merged_timing = "%s_merge_timing.txt" % BAM_FILE
    #merged_timing = "PAWG.%s.bam_merge_timing.txt" % UUID
    merged_timing = params["%s_merged_timing" % mode]
    #the submission directory
    CWD=os.getcwd()
    SUB_DIR="%s/%s.partial"%(CWD,UUID)
    FIN_DIR="%s/%s.FINISHED"%(CWD,UUID)

    if os.path.exists(FIN_DIR):
        sys.stderr.write("Upload already FINISHED\n")
        sys.exit(0)

    if not os.path.exists(SUB_DIR):
        os.mkdir(SUB_DIR)

    if not os.path.exists("%s/PCAWG.%s.bam" % (SUB_DIR,UUID)):
        os.symlink(os.path.relpath(BAM_FILE, SUB_DIR),"%s/PCAWG.%s.bam"%(SUB_DIR,UUID))

    #put metric compareing $ORIG_FILE and $BAM_FILE and save stats to $SUB_DIR
    try:
        if debug:
            cmd = "%s %s/realigned_bam_check -o %s -n %s -p %s" % (DEBUG_PYTHON,DEBUG_SCRIPT_DIR,ORIG_BAM_FILE,BAM_FILE,SUB_DIR)
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
    #if not os.path.exists("%s/%s" % (SUB_DIR,NEW_UUID)) or not os.path.exists("%s/%s" % (SUB_DIR,UUID)):
    #if not os.path.exists( os.path.join(SUB_DIR,NEW_UUID"trans.map") ):
    if not os.path.exists( os.path.join(SUB_DIR,"MD_DONE") ):
        additional_test_options = ""
        if test:
            #use the test template, goes to a different study
            additional_test_options = "-d analysis.pawg_template.test.xml"
        try:
            if debug:
                cmd = "%s %s/create_pawg_metadata -u %s -f PCAWG.%s.bam -c %s -t %s -n %s -p %s %s" % (DEBUG_PYTHON,DEBUG_SCRIPT_DIR,UUID,UUID,md5,NEW_NORMAL_UUID,NEW_UUID,SUB_DIR,additional_test_options)
            else:
                cmd = "create_pawg_metadata -u %s -f PCAWG.%s.bam -c %s -t %s -n %s -p %s %s" % (UUID,UUID,md5,NEW_NORMAL_UUID,NEW_UUID,SUB_DIR,additional_test_options)
            (stdout,stderr)=run_command(cmd)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub metadata creation error\n")
            raise cpe

        #add the QC metrics to the metadata
        try:
            if debug:
                #cmd = "%s %s/add_qc_results_to_metadata.pl %s/%s/analysis.xml %s" % (DEBUG_PERL,DEBUG_SCRIPT_DIR,SUB_DIR,NEW_UUID,QC_STATS_FILE)
                cmd = "%s %s/add_qc_results_to_metadata.pl %s/%s/analysis.xml %s %s %s %s %s" % (DEBUG_PERL,DEBUG_SCRIPT_DIR,SUB_DIR,NEW_UUID,params["%s:aligned_bam_dir" % mode],params["%s:stats_dir" % mode],download_timing,merged_metrics,merged_timing)
            else:
                cmd = "add_qc_results_to_metadata.pl %s/%s/analysis.xml %s %s %s %s %s" % (SUB_DIR,NEW_UUID,params["%s:aligned_bam_dir" % mode],params["%s:stats_dir" % mode],download_timing,merged_metrics,merged_timing)
            (stdout,stderr)=run_command(cmd)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub QC stats/ICGC fields addition to metadata error\n")
            raise cpe

        #write sentinal file
        with open( os.path.join(SUB_DIR,"MD_DONE"), "w" ) as outf:
            outf.write("metadata generated finished successfully\n")

    #check submission state
    try:
        if debug:
            cmd = "curl -sk %s/cghub/metadata/analysisDetail?analysis_id=%s | egrep -ie '<state>' | cut -d'>' -f 2 | cut -d\"<\" -f 1" % (REPO_SERVER,NEW_UUID)
        else:
            cmd = "curl -sk %s/cghub/metadata/analysisDetail?analysis_id=%s | egrep -ie '<state>' | cut -d'>' -f 2 | cut -d\"<\" -f 1" % (REPO_SERVER,NEW_UUID)
        (state,stderr)=run_command(cmd, SUB_DIR)
        state = state.rstrip()
    except CalledProcessError as cpe:
        sys.stderr.write("CGHub WSI query for state for %s failed\n" % (NEW_UUID))
        raise cpe

    #if test level is 2 or above, quit before doing anything else
    if test > 1:
        return

    #not submitted yet
    if ( state is None or state == "" ) and not os.path.exists( os.path.join(SUB_DIR,"SUBMIT_DONE") ):
    #if not os.path.exists( os.path.join(SUB_DIR,"SUBMIT_DONE") ):
        try:
            #return True
            if debug:
                cmd = "%s %s/cgsubmit_fixed -s %s -c %s -u %s" % (DEBUG_PYTHON,DEBUG_SCRIPT_DIR,REPO_SERVER,DEBUG_UPLOAD_KEY,NEW_UUID)
            else:
                cmd = "cgsubmit_fixed -c %s -u %s" % (UPLOAD_KEY,NEW_UUID)
            (stdout,stderr)=run_command(cmd, SUB_DIR)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub metadata submission error\n")
            raise cpe

        #write sentinal value
        with open( os.path.join(SUB_DIR,"SUBMIT_DONE"), "w" ) as outf:
            outf.write("metadata submitted finished successfully\n")

    #submitted but manifest file needed for upload is probably gone, recreate by doing a valiadtion only submission (indempotent)
    elif not os.path.exists( os.path.join(SUB_DIR,NEW_UUID,"manifest.xml") ):
        try:
            if debug:
                #return True
                cmd = "%s %s/cgsubmit_fixed -s %s --validate-only -u %s" % (DEBUG_PYTHON,DEBUG_SCRIPT_DIR,REPO_SERVER,NEW_UUID)
            else:
                cmd = "cgsubmit_fixed --validate-only -u %s" % (NEW_UUID)
            (stdout,stderr)=run_command(cmd, SUB_DIR)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub metadata submission manifest recreation error\n")
            raise cpe
        #must also delete any existing gto files
        if os.path.exists(os.path.join(SUB_DIR,NEW_UUID,"%s.gto" % NEW_UUID)) or os.path.exists(os.path.join(SUB_DIR,NEW_UUID,"%s.gto.progress" % NEW_UUID)):
            try:
                run_command("rm %s" % (os.path.join(SUB_DIR,NEW_UUID,"*.gto*")))
            except CalledProcessError as cpe:
                sys.stderr.write("CGHub gto deletion error\n")
                raise cpe

    #try to upload if in the right (or non-existent) state
    if state is None or state == "" or state == "submitted" or state == "uploading":
        try:
            if debug:
                #return True
                cmd = "%s/gtupload -c %s -u %s/manifest.xml -vv 2>%s/upload.stderr.log" % (DEBUG_SCRIPT_DIR,DEBUG_UPLOAD_KEY,NEW_UUID,SUB_DIR)
            else:
                cmd = "gtupload -c %s -u %s/manifest.xml -vv 2>%s/upload.stderr.log" % (UPLOAD_KEY,NEW_UUID,SUB_DIR)
            (stdout,stderr)=run_command(cmd, SUB_DIR)
        except CalledProcessError as cpe:
            sys.stderr.write("CGHub file upload error, check error log %s/upload.stderr.log\n" % SUB_DIR)
            raise cpe
    elif state != "live":
        sys.stderr.write("not in a submitting/uploading state, but also not live, CHECK THIS ONE\n")
        raise CalledProcessError(1,"state not live: %s" % (state))

    #finally finish by renaming working dir
    #print "finishing with the rename"
    os.rename(SUB_DIR,FIN_DIR)

def cghub_submit_both(params):
    cghub_submit(UUID=params['normal_id'],
        NEW_UUID=params['new_normal_id'],
        NORMAL_UUID=params['normal_id'],
        NEW_NORMAL_UUID=params['new_normal_id'],
        ORIG_BAM_FILE=params['normal_bam'],
        BAM_FILE=params['normal_merged'],
        MD5=params['normal_merged'] + ".md5",
        UPLOAD_KEY=UPLOAD_KEY,
        mode="normal",
        params=params,
        test=params.get('test',0))

    cghub_submit(UUID=params['tumor_id'],
        NEW_UUID=params['new_tumor_id'],
        NORMAL_UUID=params['tumor_id'],
        NEW_NORMAL_UUID=params['new_tumor_id'],
        ORIG_BAM_FILE=params['tumor_bam'],
        BAM_FILE=params['tumor_merged'],
        MD5=params['tumor_merged'] + ".md5",
        UPLOAD_KEY=UPLOAD_KEY,
        mode="tumor",
        params=params,
        test=params.get('test',0))

    return []



#STEPS=[realigned_bam_check,create_pawg_metadata,cgsubmit,gtupload]
#STEPS=[cghub_submit_normal,cghub_submit_tumor] #can't do it in parallel because cgsubmit creates a fixed temp directory
STEPS=[cghub_submit_both]
RESUME=True
STORE=False
IMAGE="pcap_tools"

def main():
    #TEST_UUID='251916ec-f78e-4eae-99fe-ff802e3ce2fe'
    #this uuid is REAL tcga data, so only upload to production
    TEST_UUID='9b6cd038-dee8-47b3-bd30-9a361a1f39ae'
    path = "/pod/home/cwilks/p/output/%s" % TEST_UUID
    BAM_FILE='/pod/home/cwilks/p/output/%s/%s.bam' % (TEST_UUID,TEST_UUID)
    mode = 'normal'

    params = {}
    params["debug_path"] = path
    #params[rg1+":aligned_bam"] = "/pod/home/cwilks/p/output/%s/out_%s.bam" % (TEST_UUID,rg1)
    params["%s_merged_timing" % mode] = "%s/PAWG.%s.bam_merge_timing.txt" % (path,TEST_UUID)
    params["%s_download_timing" % mode] = "%s/%s_download_timing.txt" % (path,TEST_UUID)
    params["%s_merged_metrics" % mode] = "%s/%s.markdup.metrics" % (path,TEST_UUID)
    params["%s:aligned_bam_dir" % mode] = "%s/%s" % (path,mode)
    params["%s:stats_dir" % mode] = "%s/%s" % (path,mode)

    cghub_submit(UUID='%s' % TEST_UUID,
                 NEW_UUID = str(uuid.uuid4()),
                 #NEW_UUID='a04a7f49-d962-4b05-a668-ff10b973eca1',
                 #NEW_UUID='2e610b0d-0ac5-4e51-a46a-11b55955b097',
                 #NEW_UUID='d3afc141-bd34-41a5-bbab-4a65c3b0ec27',
                 #NEW_UUID='c48a703e-48bd-4a6a-8948-c64f87c9cb82',
                 #NEW_UUID='c401159c-75ac-411a-bc74-4b9eaae5fd56',
                 #NEW_UUID='d3d3aa8d-fab4-43e4-83c9-6102cdaf4ab1',
                 #NEW_UUID='b8acbf32-0867-488a-a4d9-984a33345536',
                 #NEW_UUID='e62e9cf6-c04c-4a3b-bf27-df43845ff6c7',
                 #NEW_UUID='266cec33-e253-41c9-9e1a-2a00205acd0a',
                 NORMAL_UUID='a0963407-05e7-4c84-bfe0-34aacac08eed',
                 NEW_NORMAL_UUID='97112394-e3e6-4bf4-b4a6-6251fd80c711',
                 ORIG_BAM_FILE='/pod/home/cwilks/p/input/%s/%s.bam' % (TEST_UUID,TEST_UUID),
                 BAM_FILE=BAM_FILE,
                 MD5='/pod/home/cwilks/p/output/%s/%s.bam.md5' % (TEST_UUID,TEST_UUID),
                 #QC_STATS_FILE='/pod/home/cwilks/p/output/%s/%s.bam.bas' % (TEST_UUID,TEST_UUID),
                 UPLOAD_KEY=UPLOAD_KEY,
                 mode=mode,
                 params=params,
                 test=True,
                 debug=True)


if __name__ == '__main__':
    main()
