#!/usr/bin/perl
use strict;
use XML::DOM;
use XML::LibXML;
use JSON;
use Time::Piece;

my $parser = new XML::DOM::Parser;

my $analysisF = shift;
my $qcF=shift;

my $metad = {}; 
$metad->{"analysis"} = parse_metadata($analysisF);

#`rsync -av $analysisF.old $analysisF`;
#`rm $analysisF.new`; 
 
open(IN,"<$analysisF");
open(OUT,">$analysisF.new");

my $DONE=undef;

while(my $line=<IN>)
{
  chomp($line);
  if($DONE || ($line !~ /<\/ANALYSIS_ATTRIBUTES>/i && $line !~ /<\/ANALYSIS>/i))
  {
	if($line =~ /bamsort\'+/)
	{
		$line =~ s/\'//g;
	}
	print OUT "$line\n";
	next;
  }

  #info fields
  my ($info_key,$info_str) = generate_submission($metad);
  print OUT "      <ANALYSIS_ATTRIBUTE>
        <TAG>$info_key</TAG>
        <VALUE>" . $info_str . "</VALUE>
        </ANALYSIS_ATTRIBUTE>\n";

  # QC
  print OUT "      <ANALYSIS_ATTRIBUTE>
        <TAG>qc_metrics</TAG>
        <VALUE>" . &getQcResult($qcF) . "</VALUE>
        </ANALYSIS_ATTRIBUTE>\n";
  print OUT "$line\n";
  my $DONE=1;
}
close(IN);
close(OUT);
`mv $analysisF $analysisF.old`;
`rsync -av $analysisF.new $analysisF`;

sub generate_submission
{
  my ($m) = @_;

  # const
  my $t = gmtime;
  my $datetime = $t->datetime();
  # populate refcenter from original BAM submission
  # @RG CN:(.*)
  my $refcenter = "OICR";
  # @CO sample_id
  my $sample_id = "";
  # capture list
  my $sample_uuids = {};
  # current sample_uuid (which seems to actually be aliquot ID, this is sample ID from the BAM header)
  my $sample_uuid = "";
  # @RG SM or @CO aliquoit_id
  my $aliquot_id = "";
  # @RG LB:(.*)
  my $library = "";
  # @RG ID:(.*)
  my $read_group_id = "";
  # @RG PU:(.*)
  my $platform_unit = "";
  # hardcoded
  my $analysis_center = "OICR";
  # @CO participant_id
  my $participant_id = "";
  # hardcoded
  my $bam_file = "";
  # hardcoded
  my $bam_file_checksum = "";

  # these data are collected from all files
  # aliquot_id|library_id|platform_unit|read_group_id|input_url
  my $global_attr = {};

  #print Dumper($m);

  # input info
  my $pi2 = {};

  # this isn't going to work if there are multiple files/readgroups!
  foreach my $file (keys %{$m}) {
    # populate refcenter from original BAM submission
    # @RG CN:(.*)
    # FIXME: GNOS currently only allows: ^UCSC$|^NHGRI$|^CGHUB$|^The Cancer Genome Atlas Research Network$|^OICR$
    ############$refcenter = $m->{$file}{'target'}[0]{'refcenter'};
    $sample_uuid = $m->{$file}{'target'}[0]{'refname'};
    $sample_uuids->{$m->{$file}{'target'}[0]{'refname'}} = 1;
    # @CO sample_id
    my @sample_ids = keys %{$m->{$file}{'analysis_attr'}{'sample_id'}};
    # workaround for updated XML
    if (scalar(@sample_ids) == 0) { @sample_ids = keys %{$m->{$file}{'analysis_attr'}{'submitter_specimen_id'}}; }
    $sample_id = $sample_ids[0];
    # @RG SM or @CO aliquoit_id
    my @aliquot_ids = keys %{$m->{$file}{'analysis_attr'}{'aliquot_id'}};
    # workaround for updated XML
    if (scalar(@aliquot_ids) == 0) { @aliquot_ids = keys %{$m->{$file}{'analysis_attr'}{'submitter_sample_id'}}; }
    $aliquot_id = $aliquot_ids[0];
    # @RG LB:(.*)
    $library = $m->{$file}{'run'}[0]{'data_block_name'};
    # @RG ID:(.*)
    $read_group_id = $m->{$file}{'run'}[0]{'read_group_label'};
    # @RG PU:(.*)
    $platform_unit = $m->{$file}{'run'}[0]{'refname'};
    my $bam_file = $m->{$file}{'file'}[0]{filename};
    # FIXME: GNOS limitation
    # hardcoded
    ########$analysis_center = $refcenter;
    # @CO participant_id
    my @participant_ids = keys %{$m->{$file}{'analysis_attr'}{'participant_id'}};
    if (scalar(@participant_ids) == 0) { @participant_ids = keys %{$m->{$file}{'analysis_attr'}{'submitter_donor_id'}}; }
    $participant_id = $participant_ids[0];
    my $index = 0;
    foreach my $bam_info (@{$m->{$file}{'run'}}) {
      if ($bam_info->{data_block_name} ne '') {
        #print Dumper($bam_info);
        #print Dumper($m->{$file}{'file'}[$index]);
        my $pi = {};
        $pi->{'input_info'}{'donor_id'} = $participant_id;
        $pi->{'input_info'}{'specimen_id'} = $sample_id;
        $pi->{'input_info'}{'target_sample_refname'} = $sample_uuid;
        $pi->{'input_info'}{'analyzed_sample'} = $aliquot_id;
        $pi->{'input_info'}{'library'} = $bam_info->{data_block_name}; # $library;
        $pi->{'input_info'}{'platform_unit'} = $bam_info->{refname};  #$platform_unit;
        $pi->{'read_group_id'} = $bam_info->{read_group_label};	#$read_group_id;
        $pi->{'input_info'}{'analysis_id'} = $m->{$file}{'analysis_id'};
        $pi->{'input_info'}{'bam_file'} = $bam_file;
        push @{$pi2->{'pipeline_input_info'}}, $pi;
      }
      $index++;
    }

  }
  my $str = to_json($pi2);
  return ("pipeline_input_info",$str);
}

sub parse_metadata {
  my ($xml_path) = @_;
  my $doc = $parser->parsefile($xml_path);
  my $m = {};
  $m->{'analysis_id'} = getVal($doc, 'analysis_id');
  $m->{'center_name'} = getVal($doc, 'center_name');
  push @{$m->{'study_ref'}}, getValsMulti($doc, 'STUDY_REF', "refcenter,refname");
  push @{$m->{'run'}}, getValsMulti($doc, 'RUN', "data_block_name,read_group_label,refname");
  push @{$m->{'target'}}, getValsMulti($doc, 'TARGET', "refcenter,refname");
  push @{$m->{'file'}}, getValsMulti($doc, 'FILE', "checksum,filename,filetype");
  $m->{'analysis_attr'} = getAttrs($doc);
  $m->{'experiment'} = getBlock($xml_path, "EXPERIMENT ", "EXPERIMENT");
  $m->{'run_block'} = getBlock($xml_path, "RUN center_name", "RUN");
  return($m);
}

sub getQcResult 
{
  # detect all the QC report files by checking file name pattern
  my $qcF = shift;

  my $ret = { "qc_metrics" => [] };
  open (QC, "<$qcF");
  my @header = split /\t/, <QC>;
  chomp(@header);
  while(my $line = <QC>)
  {
	chomp($line);
    my @data = split /\t/, $line;
    my $qc_metrics = {};
    $qc_metrics->{$_} = shift @data for (@header);
    push @{ $ret->{qc_metrics} }, {"read_group_id" => $qc_metrics->{readgroup}, "metrics" => $qc_metrics};
  }    
  close (QC);

  return to_json $ret;
}

sub getBlock {
  my ($xml_file, $key, $end) = @_;
  my $block = "";
  open IN, "<$xml_file" or die "Can't open file $xml_file\n";
  my $reading = 0;
  while (<IN>) {
    chomp;
    if (/<$key/) { $reading = 1; }
    if ($reading) {
      $block .= "$_\n";
    }
    if (/<\/$end>/) { $reading = 0; }
  }
  close IN;
  return $block;
}

sub getVal {
  my ($node, $key) = @_;
  #print "NODE: $node KEY: $key\n";
  if ($node != undef) {
    if (defined($node->getElementsByTagName($key))) {
      if (defined($node->getElementsByTagName($key)->item(0))) {
        if (defined($node->getElementsByTagName($key)->item(0)->getFirstChild)) {
          if (defined($node->getElementsByTagName($key)->item(0)->getFirstChild->getNodeValue)) {
           return($node->getElementsByTagName($key)->item(0)->getFirstChild->getNodeValue);
          }
        }
      }
    }
  }
  return(undef);
}


sub getAttrs {
  my ($node) = @_;
  my $r = {};
  my $nodes = $node->getElementsByTagName('ANALYSIS_ATTRIBUTE');
  for(my $i=0; $i<$nodes->getLength; $i++) {
	  my $anode = $nodes->item($i);
	  my $tag = getVal($anode, 'TAG');
	  my $val = getVal($anode, 'VALUE');
	  $r->{$tag}{$val}=1;
  }
  return($r);
}

sub getValsWorking {
  my ($node, $key, $tag) = @_;
  my @result;
  my $nodes = $node->getElementsByTagName($key);
  for(my $i=0; $i<$nodes->getLength; $i++) {
	  my $anode = $nodes->item($i);
	  my $tag = $anode->getAttribute($tag);
          push @result, $tag;
  }
  return(@result);
}

sub getValsMulti {
  my ($node, $key, $tags_str) = @_;
  my @result;
  my @tags = split /,/, $tags_str;
  my $nodes = $node->getElementsByTagName($key);
  for(my $i=0; $i<$nodes->getLength; $i++) {
       my $data = {};
       foreach my $tag (@tags) {
         	  my $anode = $nodes->item($i);
	          my $value = $anode->getAttribute($tag);
		  if (defined($value) && $value ne '') { $data->{$tag} = $value; }
       }
       push @result, $data;
  }
  return(@result);
}
