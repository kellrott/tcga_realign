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

try:
    from kazoo.client import KazooClient
except ImportError:
    KazooClient = None


SCRIPT_PATH=os.path.abspath(__file__)

logging.basicConfig(level=logging.INFO)

def get_modules(args):
    modules = glob(os.path.join(os.path.abspath(args.pipeline), "*.py"))
    modules.sort()
    return modules


class Pipeline:
    def __init__(self, base):
        with open(os.path.join(base, "config.json")) as handle:
            txt = handle.read()
            self.data = json.loads(txt)
        self.base = os.path.abspath(base)
        self.name = os.path.basename(self.base)

    def get_modules(self):
        modules = glob(os.path.join(self.base, "*.py"))
        modules.sort()
        return modules


class BlankLease:
    """
    This does nothing, its a stand in if the zookeeper lease isn't avalible
    """
    def __enter__(self):
        return None

    def __exit__(self, type, value, traceback):
        pass

def run_pipeline(args):

    params = None
    with open(args.workfile) as handle:
        params = json.loads(handle.read())
    params['ncpus'] = args.ncpus

    if params is None:
        return

    params_merge = {}

    #make sure the base working directory exists
    if not os.path.exists(args.workdir):
        os.mkdir(args.workdir)

    #make sure the working directory for this job exists
    iddir = os.path.join(args.workdir, params['id'])
    if not os.path.exists(iddir):
        os.mkdir(iddir)

    #make sure the base output directory exists
    if not os.path.exists(args.outdir):
        os.mkdir(args.outdir)

    #make sure the output directory for this job exists
    final_iddir = os.path.join(args.outdir, params['id'])
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

    zk = zookeeper_init(args, False)

    #start scanning across the modules
    modules = get_modules(args)
    for m in modules:
        name = os.path.basename(m).replace(".py", "")
        stage = int(os.path.basename(m).split("_")[0])

        #get the names of the working directory and the final output directory
        workdir = os.path.abspath(os.path.join(args.workdir,params['id'], name))
        finaldir = os.path.abspath(os.path.join(args.outdir,params['id'], name))

        #load the module code
        logging.info("Checking for %s run for %s" % (params['id'], name))
        f, m_name, desc = imp.find_module(name, [os.path.dirname(m)])
        mod = imp.load_module(name, f, m_name, desc)

        #check for existing results
        if not os.path.exists(finaldir) and not os.path.exists(workdir):
            logging.info("Results for %s run for %s not found" % (params['id'], name))
            if args.stages is not None and stage not in args.stages:
                logging.info("Stopping at Stage %s" % (name))
                return 0
            logging.info("Results for %s run for %s not found" % (params['id'], name))

            files = []
            try:
                with open(workdir + ".params", "w") as handle:
                    handle.write(json.dumps({"base" : params, "merge" : params_merge}))

                # is the stage defines a 'CLUSTER_MAX' try to use zookeeper to
                # obtain a lease for the work
                semaphore = BlankLease()
                if zk is not None and args.zbase is not None and hasattr(mod, "CLUSTER_MAX"):
                    semaphore = zk.Semaphore(args.zbase + "/leases/" + name, max_leases=mod.CLUSTER_MAX)

                with semaphore:
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
                            params['id'], name,
                            os.path.join("/pipeline/work/", params['id'], name + ".params")
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
                logging.error("Error found for %s run for %s" % (params['id'], name))
                if not hasattr(mod, "FAIL") or mod.FAIL != 'soft':
                    return 1
            logging.info("Results found for %s run for %s" % (params['id'], name))

        with open(os.path.join(args.outdir, params['id'], name + ".output")) as handle:
            txt = handle.read()
            data = json.loads(txt)
            params_merge[name] = {}
            for row in data:
                params_merge[name][row[0]] = row[1]

        print params, params_merge
    os.unlink(final_iddir + ".pid")


def run_build(args):

    pipeline = Pipeline(args.pipeline)

    srcs = glob(os.path.join(pipeline.base, "dockers", "*"))

    out = os.path.join(pipeline.base, "images")
    if not os.path.exists(out):
        os.mkdir(out)

    for src_dir in srcs:
        src_name = os.path.basename(src_dir)
        if args.flush:
            cmd = "docker build --no-cache -t %s %s" % (src_name, src_dir)
        else:
            cmd = "docker build -t %s %s" % (src_name, src_dir)
        if not args.skip_sudo:
            cmd = "sudo " + cmd
        subprocess.check_call(cmd, shell=True)

        cmd = "docker save %s > %s.tar" % (src_name, os.path.join(out, src_name))
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
    try:
        for fname, f in func(params):
            out.append((fname,f))
        q.put(out)
    except Exception as exc:
        q.put(exc)


def run_exec(args):
    logging.info("Starting Exec")

    with open(args.params) as handle:
        txt = handle.read()
        params_all = json.loads(txt)

    work_id = params_all['id']

    #get the names of the working directory and the final output directory
    workbasedir = os.path.abspath(os.path.join(args.workdir,work_id))


    params = params_all['base']
    params_merge = params_all['merge']

    modules = get_modules(args)
    for m in modules:
        name = os.path.basename(m).replace(".py", "")
        logging.info("loading %s" % (name))
        f, m_name, desc = imp.find_module(name, [os.path.dirname(m)])
        mod = imp.load_module(name, f, m_name, desc)
        workdir = os.path.abspath(os.path.join(args.workdir,work_id, name))
        finaldir = os.path.abspath(os.path.join(args.outdir,work_id, name))

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
                params['outdir'] = tempfile.mkdtemp(dir=workbasedir, prefix=name + "_")

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
                     if isinstance(o, Exception):
                         raise o
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
            with open(os.path.join(args.outdir, work_id, name + ".output"), "w") as handle:
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


def zookeeper_init(args, fail=True):
    if KazooClient is None:
        if fail:
            raise Exception("kazoo not installed")
        else:
            return None
    zk = KazooClient(hosts=args.zookeeper)
    zk.start()
    return zk

def run_queue(args):
    zk = zookeeper_init(args)
    queue = zk.Queue(args.zbase + "queue")

    for job in args.jobs:
        with open(job) as handle:
            txt = handle.read()
            try:
                json.loads(txt)
                print "queuing", txt
                queue.put(txt)
            except ValueError:
                print "Invalid file"
    zk.stop()


def run_queue_list(args):
    zk = zookeeper_init(args)
    for c in zk.get_children(args.zbase + "/queue"):
        txt, info = zk.get(args.zbase + "/queue/" + c)
        data = json.loads(txt)
        print data['id']
    zk.stop()


def run_client(args):
    zk = zookeeper_init(args)

    workdir = tempfile.mkdtemp(dir=args.workdir, prefix="simplechain_client_")
    queue = zk.Queue(args.zbase + "queue")
    while 1:
        item_txt = queue.get()
        if item_txt is None:
            break

        item_data = json.loads(item_txt)
        workfile = os.path.join(workdir, item_data['id'])
        with open(workfile, "w") as handle:
            handle.write(item_txt)
        cmd = [sys.executable, SCRIPT_PATH, "run",
            "-z", args.zookeeper,
            "--zbase", args.zbase,
            "--workdir", os.path.join(workdir, "work"),
            "--outdir", os.path.join(workdir, "out"),
            "--docker",
            "pipeline-name",
            workfile ]
        print " ".join(cmd)
        time.sleep(5)

    shutil.rmtree(workdir)
    zk.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="subcommand")

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument("-z", "--zookeeper", default=None)
    parser_run.add_argument("--workdir", default="work")
    parser_run.add_argument("--docker", action="store_true", default=False)
    parser_run.add_argument("-s", "--stage", dest="stages", action="append", type=int)
    parser_run.add_argument("pipeline")
    parser_run.add_argument("workfile")
    parser_run.set_defaults(func=run_pipeline)

    parser_build = subparsers.add_parser('build')
    parser_build.add_argument("pipeline")
    parser_build.add_argument("--skip-sudo", action="store_true", default=False)
    parser_build.add_argument("-f", "--flush", action="store_true", default=False)
    parser_build.set_defaults(func=run_build)

    parser_install = subparsers.add_parser('install')
    parser_install.add_argument("pipeline")
    parser_install.add_argument("--skip-sudo", action="store_true", default=False)
    parser_install.set_defaults(func=run_install)

    parser_exec = subparsers.add_parser('exec')
    parser_exec.add_argument("pipeline")
    parser_exec.add_argument("module_name")
    parser_exec.add_argument("params")
    parser_exec.add_argument("--workdir", default="work")
    parser_exec.add_argument("--outdir", default="out")
    parser_exec.add_argument("--ncpus", type=int, default=8)
    parser_exec.set_defaults(func=run_exec)

    parser_queue = subparsers.add_parser('queue')
    parser_queue.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_queue.add_argument("pipeline")
    parser_queue.add_argument("jobs", nargs="+")
    parser_queue.set_defaults(func=run_queue)

    parser_queue_list = subparsers.add_parser('queue-list')
    parser_queue_list.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_queue_list.add_argument("pipeline")
    parser_queue_list.set_defaults(func=run_queue_list)

    parser_client = subparsers.add_parser('client')
    parser_client.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_client.add_argument("--workdir", default="/tmp")
    parser_client.add_argument("pipeline")
    parser_client.set_defaults(func=run_client)


    args = parser.parse_args()
    sys.exit(args.func(args))
