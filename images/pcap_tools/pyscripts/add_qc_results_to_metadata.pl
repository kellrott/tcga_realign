#!/usr/bin/perl
use strict;
use JSON;
use Time::Piece;

my $analysisF = shift;
my $qcF=shift;

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

  # QC
  print OUT "      <ANALYSIS_ATTRIBUTE>
        <TAG>qc_metrics</TAG>
        <VALUE>" . &getQcResult($qcF) . "</VALUE>
        </ANALYSIS_ATTRIBUTE>\n";
  print OUT "$line\n";
  $DONE=1;
}
close(IN);
close(OUT);
`mv $analysisF $analysisF.old`;
`rsync -av $analysisF.new $analysisF`;

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
    push @{ $ret->{qc_metrics} }, ["read_group_id" => $qc_metrics->{readgroup}, "metrics" => $qc_metrics];
  }    
  close (QC);

  return to_json $ret;
}
