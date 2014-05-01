#!/usr/bin/env python

import subprocess
import os
import sys
import argparse

def getMachines():
	out = subprocess.check_output(["qconf", "-sel"])
	return out.split("\n")

def getRemoteList(host, folder):
	try:
		null = open(os.devnull, "w")
		out = subprocess.check_output(["ssh", host, "ls", "-d", "/scratch/tcga_realign_*/%s/*" % (folder) ], stderr=null)
		null.close()
		return out.split("\n")
	except subprocess.CalledProcessError:
		return []

def getPids(host):
	try:
		null = open(os.devnull, "w")
		out = subprocess.check_output(["ssh", host, "grep", "-H", ".", "/scratch/tcga_realign_*.pid/*" ], stderr=null)
		null.close()
		pids = {}
		for line in out.split("\n"):
			tmp = line.split(":")
			if len(tmp) == 2:
				name = os.path.basename(os.path.dirname(tmp[0])).replace(".pid", "").replace("tcga_realign_", "")
				pids[name] = tmp[1]
		return pids
			
	except subprocess.CalledProcessError:
		return {}

def getErrors(host):
	try:
		null = open(os.devnull, "w")
		out = subprocess.check_output(["ssh", host, "ls", "-d", "/scratch/tcga_realign_*.error" ], stderr=null)
		null.close()
		errors = []
		for line in out.split("\n"):
			name = os.path.basename(line).replace(".error", "").replace("tcga_realign_", "")
			errors.append(name)
		return errors
			
	except subprocess.CalledProcessError:
		return []

def touchPS(host, pid):
	try:
		null = open(os.devnull, "w")
		out = subprocess.check_output(["ssh", host, "ps", pid], stderr=null)
		null.close()
		pids = {}
		return True
	except subprocess.CalledProcessError:
		return False

if __name__ == "__main__":
	
	parser = argparse.ArgumentParser()
	parser.add_argument("-c", default=None)
	parser.add_argument("-n", action="append", default=None)
	
	args = parser.parse_args()

	job_location = {}
	job_state = {}	
	flow_state = {}
	machines = getMachines()
	if args.n is not None:
		machines = args.n
	for m in machines:
		input_list = []
		for i in getRemoteList(m, "input"):
			if len(i) and not i.endswith(".gto") and not i.endswith(".partial"):
				name = os.path.basename(i)
				input_list.append( name )
		split_list = []
		for i in getRemoteList(m, "splits"):
			if len(i) and not i.endswith(".partial"):
				name = os.path.basename(i)
				split_list.append( name )
				if name in input_list:
					input_list.remove( name )
		output_list = []
		for i in getRemoteList(m, "output"):
			if len(i) and not i.endswith(".partial"):
				name = os.path.basename(i)
				output_list.append( name )
				if name in input_list:
					input_list.remove(name)
				if name in split_list:
					split_list.remove(name)
		submit_list = []
		for i in getRemoteList(m, "submit"):
			if len(i) and not i.endswith(".partial"):
				name = os.path.basename(i)
				submit_list.append( name )
				if name in input_list:
					input_list.remove(name)
				if name in split_list:
					split_list.remove(name)
				if name in output_list:
					output_list.remove(name)					
	
		for i in input_list + split_list + output_list + submit_list:
			job_location[i] = dict(job_location.get(i, {}), **{m:None})
		
		#print m, input_list, split_list, output_list
		for i in input_list:
			job_state[i] = dict(job_state.get(i,{}), downloaded='ondisk')
			
		for i in split_list:
			job_state[i] = dict(job_state.get(i,{}), split='ondisk')

		for i in output_list:
			job_state[i] = dict(job_state.get(i,{}), aligned='ondisk')
		
		for i in submit_list:
			job_state[i] = dict(job_state.get(i,{}), submitted='ondisk')

		errors = getErrors(m)
		for e in errors:
			flow_state[e] = {'error' : True}

		pids = getPids(m)
		for i in pids:
			t = touchPS(m, pids[i])
			if i not in job_location:
				job_location[i] = {}
			job_location[i][m] = t
		sys.stderr.write(".")
	sys.stderr.write("\n")
	
	for i in job_location:
		for m in job_location[i]:
			if args.c is not None:
				if job_location[i][m] == False:
					if job_state.get(i, {'NA' : None}).keys()[0] == args.c:
						print "ssh %s rm /scratch/tcga_realign_%s.pid" % (m, i) 
			else:
				print i, m, job_location[i][m], ",".join(job_state.get(i, {'NA' : None}).keys()), flow_state.get(i, None)
		
