#!/usr/bin/env python

import os
import json
from glob import glob
import argparse
import imp

def run_pipeline(args):

	params = None
	with open(args.workfile) as handle:
		for line in handle:
			m = json.loads(line)
			if m['id'] == args.id:
				params = m

	if params is None:
		return

	params['outdir'] = os.path.join("data", params['id'])

	modules = glob(os.path.join(os.path.abspath(args.pipeline), "*.py"))
	for m in modules:
		name = os.path.basename(m).replace(".py", "")
		f, name, desc = imp.find_module(name, [os.path.dirname(m)])
		mod = imp.load_module(name, f, name, desc)
		for func in mod.STEPS:
			func(params)

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("pipeline")
	parser.add_argument("workfile")
	parser.add_argument("id")


	args = parser.parse_args()

	print args

	run_pipeline(args)