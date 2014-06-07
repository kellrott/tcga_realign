#!/usr/bin/env python


import csv
import json

reader = csv.DictReader(open("Proposed_50_T-N_Pairs.tsv"), delimiter="\t")

for row in reader:
	print json.dumps( 
		{
			'id' : row['Donor ID'],
			'endpoint' : row['Normal GNOS endpoint'],
			'normal_id' : row['Normal Analysis ID'],
			'tumor_id' : row['Tumour Analysis ID'],
			'keyfile' : "~/hasuler.key"
		}
	)