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
import multiprocessing
import tarfile

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

    params_merge = {}

    #make sure the base working directory exists
    if not os.path.exists(args.workdir):
        os.mkdir(args.workdir)

    #make sure the working directory for this job exists
    iddir = os.path.join(args.workdir, args.id)
    if not os.path.exists(iddir):
        os.mkdir(iddir)

    #make sure the base output directory exists
    if not os.path.exists(args.outdir):
        os.mkdir(args.outdir)

    #make sure the output directory for this job exists
    final_iddir = os.path.join(args.outdir, args.id)
    if not os.path.exists(final_iddir):
        os.mkdir(final_iddir)

    #record the job run info
    run_info = {
        'host' : socket.gethostname(),
        'workdir' : iddir,
        'pid' : os.getpid()
    }
    with open(final_iddir + ".json", "w") as handle:
        handle.write(json.dumps(run_info))
    #create the pid file
    with open(final_iddir + ".pid", "w") as handle:
        handle.write(str(os.getpid()))

    #start scanning across the modules
    modules = get_modules(args)
    for m in modules:
        name = os.path.basename(m).replace(".py", "")

        #get the names of the working directory and the final output directory
        workdir = os.path.abspath(os.path.join(args.workdir,args.id, name))
        finaldir = os.path.abspath(os.path.join(args.outdir,args.id, name))

        #load the module code
        logging.info("Checking for %s run for %s" % (args.id, name))
        f, m_name, desc = imp.find_module(name, [os.path.dirname(m)])
        mod = imp.load_module(name, f, m_name, desc)

        #check for existing results
        if not os.path.exists(finaldir) and not os.path.exists(workdir):
            logging.info("Results for %s run for %s not found" % (args.id, name))

            files = []
            try:
                with open(workdir + ".params", "w") as handle:
                    handle.write(json.dumps({"base" : params, "merge" : params_merge}))
                if args.docker and hasattr(mod, "IMAGE"):
                    data_mount = ""
                    if args.data is not None:
                        data_mount = " ".join( "-v %s" % (a) for a in args.data)
                    cmd = "sudo docker run -i --rm -u %s \
-v %s:/pipeline/work \
-v %s:/pipeline/output \
-v %s:/pipeline/simple \
-v %s:/pipeline/code \
%s \
%s \
/pipeline/simple/simplechain.py exec /pipeline/code --workdir /pipeline/work --outdir /pipeline/output %s %s %s" % (
                        os.geteuid(),
                        os.path.abspath(args.workdir),
                        os.path.abspath(args.outdir),
                        os.path.dirname(os.path.abspath(__file__)),
                        os.path.abspath(args.pipeline),
                        data_mount,
                        mod.IMAGE,
                        args.id, name,
                        os.path.join("/pipeline/work/", args.id, name + ".params")
                    )
                else:
                    cmd = "%s exec %s/params" % (__file__, workdir)
                logging.info("Running: %s" % (cmd))
                subprocess.check_call(cmd, shell=True)
            except:
                if not hasattr(mod, "FAIL") or mod.FAIL != 'soft':
                    os.unlink(final_iddir + ".pid")
                    return 1

        else:
            if os.path.exists( finaldir + ".error" ):
                logging.error("Error found for %s run for %s" % (args.id, name))
                if not hasattr(mod, "FAIL") or mod.FAIL != 'soft':
                    return 1
            logging.info("Results found for %s run for %s" % (args.id, name))

        with open(os.path.join(args.outdir, args.id, name + ".output")) as handle:
            txt = handle.read()
            data = json.loads(txt)
            params_merge[name] = {}
            for row in data:
                params_merge[name][row[0]] = row[1]


        print params, params_merge
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
            cmd = "qsub simplechain.sh %s" % (select)
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
        cmd = "qsub -l hostname=%s simplechain.sh %s" % (m['host'], args.id)
        print cmd
        subprocess.check_call(cmd, shell=True)

def run_build(args):

    if args.src is None:
        srcs = glob(os.path.join(args.dir, "*"))
    else:
        srcs = [ os.path.join(args.dir, args.src) ]

    for src_dir in srcs:
        src_name = os.path.basename(src_dir)
        if args.flush:
            cmd = "docker build --no-cache -t %s %s" % (src_name, src_dir)
        else:
            cmd = "docker build -t %s %s" % (src_name, src_dir)
        if not args.skip_sudo:
            cmd = "sudo " + cmd
        subprocess.check_call(cmd, shell=True)
        
        if args.out is not None:
            cmd = "docker save %s > %s.tar" % (src_name, os.path.join(args.out, src_name))
            if not args.skip_sudo:
                cmd = "sudo " + cmd
            subprocess.check_call(cmd, shell=True)

def run_install(args):
    if args.src is None:
        srcs = glob(os.path.join(args.dir, "*"))
    else:
        srcs = [ os.path.join(args.dir, args.src) ]

    for src_tar in srcs:
        src_name = re.sub(r'.tar$', '', os.path.basename(src_tar))
        t = tarfile.TarFile(src_tar)
        meta_str = t.extractfile('repositories').read()
        meta = json.loads(meta_str)
   
        cmd = "docker images --no-trunc"
        if not args.skip_sudo:
            cmd = "sudo " + cmd
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        stdo, stde = proc.communicate()
        tag, tag_value = meta.items()[0]
        rev, rev_value = tag_value.items()[0]
        found = False
        for line in stdo.split("\n"):
            tmp = re.split(r'\s+', line)
            if tmp[0] == tag and tmp[1] == rev and tmp[2] == rev_value:
                found = True
        if not found:
            print "Installing %s" % (src_name)
            cmd = "docker load"
            if not args.skip_sudo:
                cmd = "sudo " + cmd
            cmd = "cat %s | %s" % (src_tar, cmd)
            subprocess.check_call(cmd, shell=True)
        else:
            print "Already Installed: %s" % (src_name)

def func_run(q, func, params):
    out = []
    for fname, f in func(params):
        out.append((fname,f))
    q.put(out)


def run_exec(args):
    logging.info("Starting Exec")
    #get the names of the working directory and the final output directory
    workbasedir = os.path.abspath(os.path.join(args.workdir,args.id))

    with open(args.params) as handle:
        txt = handle.read()
        params_all = json.loads(txt)

    params = params_all['base']
    params_merge = params_all['merge']

    modules = get_modules(args)
    for m in modules:
        name = os.path.basename(m).replace(".py", "")
        logging.info("loading %s" % (name))
        f, m_name, desc = imp.find_module(name, [os.path.dirname(m)])
        mod = imp.load_module(name, f, m_name, desc)
        workdir = os.path.abspath(os.path.join(args.workdir,args.id, name))
        finaldir = os.path.abspath(os.path.join(args.outdir,args.id, name))

        if name == args.module_name:
            #load the module code
            odir = os.getcwd()
            orig_stdout = sys.stdout
            orig_stderr = sys.stderr
            #sys.stdout = open(finaldir + ".stdout", "w")
            #sys.stderr = open(finaldir + ".stderr", "w")
            if mod.RESUME:
                params['outdir'] = workdir + ".partial"
                if not os.path.exists(params['outdir']):
                    os.mkdir(params['outdir'])
            else:
                params['outdir'] = tempfile.mkdtemp(dir=workbasedir, prefix=name)

            files = []
            os.chdir(params['outdir'])
            try:
                flist = None
                if callable(mod.STEPS):
                    flist = mod.STEPS(params)
                else:
                    flist = mod.STEPS

                procs = []
                q = multiprocessing.Queue()
                for func in flist:
                    p = multiprocessing.Process(target=func_run, args=(q,func,params,))
                    p.start()
                    procs.append(p)
                    print "Count",  sum( list( a.is_alive() for a in procs) )
                    while sum( list( a.is_alive() for a in procs) ) >= args.ncpus:
                        time.sleep(1)

                files = []
                for p in procs:
                     o = q.get()
                     files.extend(o)
                     p.join()
            except:
                with open(finaldir + ".error", "w") as err_handle:
                    traceback.print_exc(file=err_handle)
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
            if name in params_merge:
                for ename, evalue in params_merge[name].items():
                    if isinstance(evalue, basestring):
                        if mod.STORE:
                            params[ename] = os.path.join(finaldir, evalue)
                        else:
                            params[ename] = os.path.join(workdir, evalue)
                    else:
                            params[ename] = evalue





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="subcommand")

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument("--workdir", default="work")
    parser_run.add_argument("--outdir", default="out")
    parser_run.add_argument("--ncpus", default="8")
    parser_run.add_argument("--data", action="append")
    parser_run.add_argument("--docker", action="store_true", default=False)

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
    parser_build.add_argument("--dir", default="dockers")
    parser_build.add_argument("--src", default=None)
    parser_build.add_argument("--skip-sudo", action="store_true", default=False)
    parser_build.add_argument("-f", "--flush", action="store_true", default=False)
    parser_build.add_argument("-o", "--out", default=None)   
    parser_build.set_defaults(func=run_build)

    parser_install = subparsers.add_parser('install')
    parser_install.add_argument("--dir", default="images")
    parser_install.add_argument("--src", default=None)
    parser_install.add_argument("--skip-sudo", action="store_true", default=False)
    parser_install.set_defaults(func=run_install)


    parser_exec = subparsers.add_parser('exec')
    parser_exec.add_argument("pipeline")
    parser_exec.add_argument("id")
    parser_exec.add_argument("module_name")
    parser_exec.add_argument("params")
    parser_exec.add_argument("--workdir", default="work")
    parser_exec.add_argument("--outdir", default="out")
    parser_exec.add_argument("--ncpus", type=int, default=8)
    parser_exec.set_defaults(func=run_exec)


    args = parser.parse_args()
    sys.exit(args.func(args))
