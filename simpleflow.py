#!/usr/bin/env python


from argparse import ArgumentParser
import sys
import time
import os
import re
import socket
import json
import threading
import pickle
import argparse
from glob import glob
import imp
import shutil
import tempfile
import socket
import logging
import traceback
import subprocess

logging.basicConfig(level=logging.INFO)

def get_modules(args):
    modules = glob(os.path.join(os.path.abspath(args.pipeline), "*.py"))
    modules.sort()
    return modules

def run_pipeline(args):

    params = None
    with open(args.workfile) as handle:
        for line in handle:
            m = json.loads(line)
            if m['id'] == args.id:
                params = m
    params['ncpus'] = args.ncpus
    
    if params is None:
        return

    if not os.path.exists(args.workdir):
        os.mkdir(args.workdir)

    if not os.path.exists(args.outdir):
        os.mkdir(args.outdir)

    iddir = os.path.join(args.workdir, args.id)
    if not os.path.exists(iddir):
        os.mkdir(iddir)

    final_iddir = os.path.join(args.outdir, args.id)
    if not os.path.exists(final_iddir):
        os.mkdir(final_iddir)

    run_info = {
        'host' : socket.gethostname(),
        'workdir' : iddir,
        'pid' : os.getpid()
    }    
    with open(final_iddir + ".json", "w") as handle:
        handle.write(json.dumps(run_info))

    with open(final_iddir + ".pid", "w") as handle:
        handle.write(str(os.getpid()))

    modules = get_modules(args)
    for m in modules:
        name = os.path.basename(m).replace(".py", "")

        workdir = os.path.abspath(os.path.join(args.workdir,args.id, name))
        finaldir = os.path.abspath(os.path.join(args.outdir,args.id, name))

        logging.info("Checking for %s run for %s" % (args.id, name))
        f, m_name, desc = imp.find_module(name, [os.path.dirname(m)])
        mod = imp.load_module(name, f, m_name, desc)

        if not os.path.exists(finaldir) and not os.path.exists(workdir):
            logging.info("Results for %s run for %s not found" % (args.id, name))

            if mod.RESUME:
                params['outdir'] = workdir + ".partial"
                if not os.path.exists(params['outdir']):
                    os.mkdir(params['outdir'])
            else:
                params['outdir'] = tempfile.mkdtemp(dir=iddir, prefix=name)

            files = []
            odir = os.getcwd()
            orig_stdout = sys.stdout
            orig_stderr = sys.stderr
            sys.stdout = open(finaldir + ".stdout", "w")
            sys.stderr = open(finaldir + ".stderr", "w")
            try:
                os.chdir(params['outdir'])
                for func in mod.STEPS:
                    for fname, f in func(params):
                        files.append((fname,f))
            except:
                with open(finaldir + ".error", "w") as err_handle:
                    traceback.print_exc(file=err_handle)
                if not hasattr(mod, "FAIL") or mod.FAIL != 'soft':
                    os.unlink(final_iddir + ".pid")
                    return 1
            sys.stderr = orig_stderr
            sys.stdout = orig_stdout
            os.chdir(odir)

            if mod.STORE:
                shutil.move(params['outdir'], finaldir)
            else:
                shutil.move(params['outdir'], workdir)
            with open(os.path.join(args.outdir, args.id, name + ".output"), "w") as handle:
                handle.write(json.dumps(files))

        else:
            if os.path.exists( finaldir + ".error" ):
                logging.error("Error found for %s run for %s" % (args.id, name))
                if not hasattr(mod, "FAIL") or mod.FAIL != 'soft':
                    return 1
            logging.info("Results found for %s run for %s" % (args.id, name))

        with open(os.path.join(args.outdir, args.id, name + ".output")) as handle:
            txt = handle.read()
            data = json.loads(txt)
            for row in data:
                if mod.STORE:
                    params[row[0]] = os.path.join(finaldir, row[1])
                else:
                    params[row[0]] = os.path.join(workdir, row[1])

        print params
    os.unlink(final_iddir + ".pid")

def run_list(args):    
    with open(args.workfile) as handle:
        for line in handle:
            data = json.loads(line)
            print data['id'] 

def get_job_states(args):    
    id_state = {}
    modules = get_modules(args)

    with open(args.workfile) as handle:
        for line in handle:
            data = json.loads(line)
            id = data['id']
            final_iddir = os.path.join(args.outdir, id)    
            if os.path.exists(final_iddir + ".pid"):
                id_state[id] = "running"
            else:
                in_error = False
                is_complete = True
                complete_count = 0
                for m in modules:
                    name = os.path.basename(m).replace(".py", "")
                    finaldir = os.path.abspath(os.path.join(args.outdir,id, name))
                    
                    if not os.path.exists(finaldir + ".output"):
                        is_complete = False
                    else:
                        complete_count += 1
                    if os.path.exists(finaldir + ".error"):
                        in_error = True
                
                if in_error:
                    id_state[id] = "error"
                else:
                    if is_complete:
                        id_state[id] = "complete"
                    else:
                        if complete_count > 0:
                            id_state[id] = "partial"
                        else:
                            id_state[id] = "ready"
    return id_state
        
def run_submit(args):    
    states = get_job_states(args)
    for i in range(args.n):
        select = None
        for s, v in states.items():
            if v in ["ready", "partial"]:
                select = s
        if select is not None:          
            cmd = "qsub simpleflow.sh %s" % (select)
            if args.test:
                print cmd
            else:
                subprocess.check_call(cmd, shell=True)
            states[select]="running"
        else:
            return
        if i != args.n - 1:
            time.sleep(args.sleep)
    

def run_states(args):        
    states = get_job_states(args)
    for id, state in states.items():
        print "%s\t%s" % (id, state)

def run_scan(args):
    for a in glob( os.path.join(args.outdir, "*.pid") ):
        b = re.sub(".pid$", ".json", a)
        with open(b) as handle:
            for line in handle:
                m = json.loads(line)
                try:
                    subprocess.check_call("ssh %s ps %s" % (m['host'], m['pid']), shell=True)
                except subprocess.CalledProcessError:
                    os.unlink(a)           


def run_resume(args):
    meta_path = os.path.join(args.outdir, args.id + ".json") 
    if os.path.exists(meta_path):
        with open(meta_path) as handle:
            line = handle.read()
            m = json.loads(line)
        for e in glob(os.path.join(args.outdir, args.id, "*.error")):
            os.unlink(e)
        cmd = "qsub -l hostname=%s simpleflow.sh %s" % (m['host'], args.id)
        print cmd
        subprocess.check_call(cmd, shell=True)

def run_build(args):

    if args.image is None:
        images = glob(os.path.join(args.dir, "*"))
    else:
        images = [ os.path.join(args.dir, args.image) ]

    for image_dir in images:
        image_name = os.path.basename(image_dir)
        if args.flush:
            cmd = "sudo docker build --no-cache -t %s %s" % (image_name, image_dir)
        else:
            cmd = "sudo docker build -t %s %s" % (image_name, image_dir)
        subprocess.check_call(cmd, shell=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="subcommand")

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument("--workdir", default="work")
    parser_run.add_argument("--outdir", default="out")
    parser_run.add_argument("--ncpus", default="8")
    parser_run.add_argument("pipeline")
    parser_run.add_argument("workfile")
    parser_run.add_argument("id")
    parser_run.set_defaults(func=run_pipeline)

    parser_list = subparsers.add_parser('list')
    parser_list.add_argument("workfile")
    parser_list.set_defaults(func=run_list)
    
    parser_submit = subparsers.add_parser('submit')
    parser_submit.add_argument("pipeline")
    parser_submit.add_argument("workfile")
    parser_submit.add_argument("--outdir", default="out")
    parser_submit.add_argument("-n", type=int, default=1)    
    parser_submit.add_argument("-s", "--sleep", type=int, default=10)
    parser_submit.add_argument("-t", "--test", action="store_true", default=False)
    
    parser_submit.set_defaults(func=run_submit)
    
    parser_states = subparsers.add_parser('states')
    parser_states.add_argument("pipeline")
    parser_states.add_argument("workfile")
    parser_states.add_argument("--outdir", default="out")
    parser_states.set_defaults(func=run_states)
    
    parser_scan = subparsers.add_parser('scan')
    parser_scan.add_argument("pipeline")
    parser_scan.add_argument("workfile")
    parser_scan.add_argument("--outdir", default="out")
    parser_scan.set_defaults(func=run_scan)
    
    parser_resume = subparsers.add_parser('resume')
    parser_resume.add_argument("pipeline")
    parser_resume.add_argument("workfile")
    parser_resume.add_argument("id")
    parser_resume.add_argument("--outdir", default="out")
    parser_resume.set_defaults(func=run_resume)

    parser_build = subparsers.add_parser('build')
    parser_build.add_argument("--dir", default="images")
    parser_build.add_argument("--image", default=None)
    parser_build.add_argument("-f", "--flush", action="store_true", default=False)
    parser_build.set_defaults(func=run_build)

    
    args = parser.parse_args()
    sys.exit(args.func(args))
