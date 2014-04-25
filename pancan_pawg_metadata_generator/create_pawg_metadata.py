#!/usr/bin/env python
#dump all metadata with given state to individual uuid names directories
#and then updates analysis.xml with PAWG required fields

#needs to do some merging of different metadata to get final product acceptable to PanCan, ICGC, and CGHub
#1) uses a template analysis.xml with the PanCan realignment project specific metadata, which is global for all realignments, most of the additional ICGC metadata is also captured in the template analysis.xml

#2) it then stitches in the required RUN_LABELs and TARGETS elements from the original metadata analysis.xml

#3) finally it replaced the FILES block with the new checksum and filename for the realigned bam file



import ConfigParser
from optparse import OptionParser
import os
import sys
import re
#import CGHWSI
import lxml.etree as xp
import datetime
import subprocess
from subprocess import Popen
import uuid
import CGHWSI

parser=OptionParser()
parser.add_option("-u", action="store",type='string',dest="analysis_id",help="REQUIRED: original analysis_id (uuid) of the BAM to be submitted")
parser.add_option("-c", action="store",type='string',dest="checksum",help="REQUIRED: MD5 checksum of BAM to be submitted")
parser.add_option("-f", action="store",type='string',dest="filename",help="REQUIRED: filename of BAM to be submitted, must be prefixed with \"PAWG.\"")
parser.add_option("-p", action="store",type='string',dest="path_to_bam_file",default="./",help="OPTIONAL: path to BAM to be submitted, if not in the analysis_id directory")
parser.add_option("-i", action="store",type='string',dest="info_file",default=None,help="OPTIONAL: info file to be added to analysis attributes")
parser.add_option("-n", action="store",type='string',dest="new_uuid",default=None,help="OPTIONAL: new uuid")

(options,args) = parser.parse_args()
ANALYSIS_ID=options.analysis_id
CHECKSUM=options.checksum
FILENAME=options.filename
INFOFILE=options.info_file
NEW_UUID=options.new_uuid

if ANALYSIS_ID is None or CHECKSUM is None or FILENAME is None:
    sys.stderr.write("MUST submit: the original TCGA source BAM's analysis_id(uuid), MD5 checksum, and a filename\n")
    sys.exit(-1)

PATH_TO_BAM=options.path_to_bam_file
if options.path_to_bam_file=="./":
    PATH_TO_BAM="%s/%s" % (options.path_to_bam_file,ANALYSIS_ID)

def run_command(command=str):
    print "Running: %s" % (command)
    run=Popen(["-c",command],stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    (stdout,stderr)=run.communicate()
    if run.returncode != 0:
        for line in stderr:
            print "ERROR:\t"+line.rstrip()
        sys.exit(-1)
    return (stdout,stderr)

def extract_pipeline_sections_from_bam_header(filename):
    #@PG ID:bwa  PN:bwa  VN:0.7.7-r441   CL:/opt/ICGC/bin/bwa mem -p -T 0 -R @RG\tID:BI:H12TD_1\tCN:BI\tPL:ILLUMINA\tPM:Illumina HiSeq 2000\tLB:WGS:BI:Solexa-173202\tPI:0\tSM:d8d5585d-32cd-4ac4-b410-a4122a17a558\tPU:BI:H12TDADXX130815_1_AATGTTCT\tDT:2013-08-15T04:00:00Z -t 1 /pancanfs/reference/genome.fa.gz -
    #(stdout,stderr)=run_command(["samtools","view","-H",filename])
    id_matcher = re.compile("ID:([^\t]+)")
    pn_matcher = re.compile("PN:([^\t]+)")
    cl_matcher = re.compile("CL:([^\t]+)")
    version_matcher = re.compile("VN:([^\t]+)")
    matchers = [["STEP_INDEX",id_matcher],["PROGRAM",pn_matcher],["VERSION",version_matcher],["NOTES",cl_matcher]]

    (stdout,stderr)=run_command("samtools view -H %s | grep @PG" % (filename))
    print stdout
    #report=stdout.split("\n")
    previous_step_idx="N/A"
    pipe_sections=[]
    for line in stdout.split("\n"):
        if len(line) < 1:
            continue
        line=line.rstrip()
        #convert what are supposed to be tabs to real tabs
        line=line.replace("\\t","\t")
        e=xp.Element("PIPE_SECTION")
        index=0
        for matcher_ in matchers:
            (tag,matcher)=matcher_
            m=matcher.search(line)
            if(m != None):
                t=xp.SubElement(e,tag)
                t.text=(m.group(1))
                #put in the previous pointer tag if this is the first (STEP_INDEX) tag for this PIPE_SECTION
                if index == 0:
                    t2=xp.SubElement(e,"PREV_STEP_INDEX")
                    t2.text=(previous_step_idx)
                    previous_step_idx=t.text
                index = index + 1
        pipe_sections.append(e)
    #foreach ps in pipe_sections
    return pipe_sections
                     


def process_analysis_xml(original_uuid,filename,checksum,path,info_file):
    f=filename.split(r'.')
    data_block_name='.'.join(f[:len(f)-1])

    parser = xp.XMLParser(remove_blank_text=True)
    tree_orig=xp.parse("./%s/analysis.xml" % (original_uuid),parser)
    root_orig=tree_orig.getroot()
    
    tree_new=xp.parse("%s/analysis.pawg_template.xml"%(os.path.dirname(__file__)),parser)
    root_new=tree_new.getroot()
    now=datetime.datetime.today().isoformat() 
    #need to:
    #0) update the ANALYSIS attributes:
    analysis_=root_new.find("ANALYSIS")
    analysis_.set('alias',filename) 
    analysis_.set('analysis_date',now) 
    #1) update RUN_LABELS so we're consistent with the RUN metadata (the sequencing isn't changing)
    run_labels=root_new.find("ANALYSIS/ANALYSIS_TYPE/REFERENCE_ALIGNMENT/RUN_LABELS")
    run_labels.clear()
    for run_label in root_orig.iter('RUN'):
       run_labels.append(run_label)  
    #2) update TARGETS so we're consistent with the sample metadata (thats not changing)
    targets = root_new.find('ANALYSIS/TARGETS')
    targets.clear()
    for target in root_orig.iter('TARGET'):
       targets.append(target) 
    #identifiers=root_orig.find("ANALYSIS/TARGETS/IDENTIFIERS")
    #targets.append(identifiers) 
    #3) update data_block names with filename sans extension
    data_block = root_new.find('ANALYSIS/DATA_BLOCK')
    data_block.set('name',data_block_name)
    for seq in root_new.iter('SEQUENCE'):
        seq.set('data_block_name',data_block_name) 
    #4) update FILES block with specific file info
    file = root_new.find('ANALYSIS/DATA_BLOCK/FILES/FILE')
    file.set('checksum',checksum)
    file.set('filename',filename)
    #5) update PIPELINE section:
    pipe_sections=extract_pipeline_sections_from_bam_header("%s/%s" % (path,filename))
    pipeline=root_new.find("ANALYSIS/ANALYSIS_TYPE/REFERENCE_ALIGNMENT/PROCESSING/PIPELINE")
    pipeline.clear()
    for pipe_section in pipe_sections:
       pipeline.append(pipe_section)
    
    #6) (optional) update ICGC specific ANALYSIS_ATTRIBUTES
    
    attributes=root_new.find("ANALYSIS/ANALYSIS_ATTRIBUTES")
    if info_file is not None:
        info_hash = {'original_analysis_id' : str(original_uuid)}
        with open(info_file) as h:
            for line in h:
                res = re.search(r'^([^:]+):(.*)$', line)
                if res:
                    info_hash[res.group(1)] = res.group(2)
        for key, value in info_hash.items():
            e=xp.Element("ANALYSIS_ATTRIBUTE")
            k=xp.SubElement(e,"TAG")
            k.text=key
            k=xp.SubElement(e,"VALUE")
            k.text=value
            attributes.append(e)

    #final: write out new analysis.xml
    st=xp.tostring(root_new,pretty_print=True)
    
    afout=open("./%s/analysis.new.xml" % (original_uuid),"w")
    afout.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    afout.write(st+"\n") 
    afout.close()


def main():
    sys.stdout.write("Processing uuid: %s\n" % ANALYSIS_ID)
    data=CGHWSI.retrieve_analysis_attributes_for_uuid(ANALYSIS_ID)
    error=CGHWSI.split_analysis_attributes(data,ANALYSIS_ID)
    run_command("rsync -av %s/analysis.xml %s/analysis.xml.orig" % (ANALYSIS_ID,ANALYSIS_ID))
    process_analysis_xml(ANALYSIS_ID,FILENAME,CHECKSUM,PATH_TO_BAM,INFOFILE)
    run_command("cat %s/analysis.new.xml | egrep -v -e '<!--' > %s/analysis.xml" % (ANALYSIS_ID,ANALYSIS_ID))
    if os.path.exists( os.path.join(ANALYSIS_ID,FILENAME) ):
        os.unlink( os.path.join(ANALYSIS_ID,FILENAME) )
    run_command("ln -s %s/%s %s/" % (PATH_TO_BAM,FILENAME,ANALYSIS_ID))
 
    #create new uuid and dir:
    if NEW_UUID is not None:
        nuuid = uuid.UUID(NEW_UUID)
    else:
        nuuid = uuid.uuid4()
    run_command("mkdir ./%s" % (nuuid))
    run_command("rsync -av ./%s/*.xml ./%s/" % (ANALYSIS_ID,nuuid))
    run_command("ln -s %s/%s %s/" % (PATH_TO_BAM,FILENAME,nuuid))
    outf = open("./%s/trans.map"%(ANALYSIS_ID),"w")
    outf.write("%s\t%s"%(ANALYSIS_ID,nuuid))
    outf.close()

if __name__ == '__main__':
    main()
