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
from StringIO import StringIO

try:
    from kazoo.client import KazooClient
except ImportError:
    KazooClient = None

try:
    import tornado.ioloop
    import tornado.web
except ImportError:
    tornado = None

try:
    import mako.template
except ImportError:
    mako = None

SCRIPT_PATH=os.path.abspath(__file__)
ZOO_BASE="/simplechain/"

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
        self.name = self.data.get('name', os.path.basename(self.base))
        self.ncpus = self.data.get('ncpus', 8)
        self.outdir = os.path.join(self.base, self.data.get('outdir', 'out'))
        self.mount = self.data.get('mount', {})

    def get_modules(self):
        modules = glob(os.path.join(self.base, "*.py"))
        modules.sort()
        return modules


def check_status(zk, pipeline, id):
    if zk is None:
        return True
    node_path = ZOO_BASE + pipeline + "/status/" + id
    if not zk.exists( node_path ):
        return True
    txt, info = zk.get(node_path)
    data = json.loads(txt)
    if 'host' in data and data['host'] is not None and data['host'] != socket.gethostname():
        return False
    return True


def log_status(zk, pipeline, id, workdir, state, stage, params):
    if zk is None:
        return
    meta = json.dumps({
        'host' : socket.gethostname(),
        'pid' : os.getpid(),
        'state' : state,
        'stage' : stage,
        'workdir' : workdir
    })
    zk_path = ZOO_BASE + pipeline + "/status/" + id
    zk.ensure_path(zk_path)
    zk.set(zk_path, meta)

    if stage is not None:
        zk_stage_path = ZOO_BASE + pipeline + "/status/" + id + "/" + stage
        stage_meta = json.dumps({
            'host' : socket.gethostname(),
            'state' : state,
            'params' : params
        })
        zk.ensure_path(zk_stage_path)
        zk.set(zk_stage_path, stage_meta)

def log_worker(zk, pipeline, workdir, job_id):
    st=os.statvfs(workdir)
    host = socket.gethostname()
    meta = json.dumps({
        'host' : host,
        'pid' : os.getpid(),
        'disk' : (st.f_bavail * st.f_frsize),
        'job_id' : job_id
    })
    node_path = ZOO_BASE + pipeline + "/workers/" + host
    if not zk.exists(node_path):
        node_path = zk.create(node_path, ephemeral=True, makepath=True )
    zk.set(node_path, meta)



class BlankLease:
    """
    This does nothing, its a stand in if the zookeeper lease isn't avalible
    """
    def __enter__(self):
        return None

    def __exit__(self, type, value, traceback):
        pass

def run_pipeline(args):

    pipeline = Pipeline(args.pipeline)

    params = None
    with open(args.workfile) as handle:
        params = json.loads(handle.read())
    params['ncpus'] = pipeline.ncpus

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
    if not os.path.exists(pipeline.outdir):
        os.mkdir(pipeline.outdir)

    #make sure the output directory for this job exists
    final_iddir = os.path.join(pipeline.outdir, params['id'])
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

    if not check_status(zk, pipeline=pipeline.name, id=params['id'] ):
        raise Exception("Bad Job Assignment: %s %s" % (pipeline.name, params['id']))

    log_status(zk, pipeline=pipeline.name, id=params['id'], state='loading', workdir=args.workdir, stage=None, params=params)

    #start scanning across the modules
    modules = get_modules(args)
    for m in modules:
        log_worker(zk, pipeline=pipeline.name, workdir=args.workdir, job_id=params['id'])
        name = os.path.basename(m).replace(".py", "")
        stage = int(os.path.basename(m).split("_")[0])

        #get the names of the working directory and the final output directory
        workdir = os.path.abspath(os.path.join(args.workdir,params['id'], name))
        finaldir = os.path.abspath(os.path.join(pipeline.outdir,params['id'], name))

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
                    handle.write(json.dumps({"id" : params['id'], "base" : params, "merge" : params_merge}))

                log_status(zk, pipeline=pipeline.name, id=params['id'], workdir=args.workdir, state='waiting', stage=name, params=params)

                # is the stage defines a 'CLUSTER_MAX' try to use zookeeper to
                # obtain a lease for the work
                semaphore = BlankLease()
                if zk is not None and hasattr(mod, "CLUSTER_MAX"):
                    semaphore = zk.Semaphore(ZOO_BASE + "/" + pipeline.name + "/leases/" + name, max_leases=mod.CLUSTER_MAX)

                with semaphore:
                    log_status(zk, pipeline=pipeline.name, id=params['id'], workdir=args.workdir, state='running', stage=name, params=params)
                    if args.docker and hasattr(mod, "IMAGE"):
                        cmd = [
                            "docker", "run", "--rm", "-u", str(os.geteuid()),
                            "-v", "%s:/pipeline/work" % os.path.abspath(args.workdir),
                            "-v", "%s:/pipeline/output" % os.path.abspath(pipeline.outdir),
                            "-v", "%s:/pipeline/simple" % os.path.dirname(os.path.abspath(__file__)),
                            "-v", "%s:/pipeline/code" % os.path.abspath(args.pipeline)
                        ]
                        for dst, src in pipeline.mount.items():
                            cmd += ["-v", "%s:%s" % (src,dst)]
                        cmd += [
                            mod.IMAGE,
                            "/pipeline/simple/simplechain.py", "/pipeline/code",
                            "exec", "--workdir", "/pipeline/work", name,
                            os.path.join("/pipeline/work/", params['id'], name + ".params")
                        ]
                        if not args.skip_sudo:
                            cmd = ["sudo"] + cmd
                    else:
                        raise Exception("I need to fix this")
                    logging.info("Running: %s" % (" ".join(cmd)))
                    with open(os.path.join(pipeline.outdir,params['id'],name+".stderr"), "w") as stderr_handle:
                        with open(os.path.join(pipeline.outdir,params['id'],name+".stdout"), "w") as stdout_handle:
                            proc = subprocess.Popen(cmd, stderr=stderr_handle, stdout=stdout_handle, close_fds=True)
                            proc.communicate()
                            if proc.returncode != 0:
                                raise Exception("Call Failed: %s" % (cmd))
                            log_status(zk, pipeline=pipeline.name, id=params['id'], workdir=args.workdir, state='complete', stage=name, params=params)
            except:
                traceback.print_exc()
                if not hasattr(mod, "FAIL") or mod.FAIL != 'soft':
                    log_status(zk, pipeline=pipeline.name, id=params['id'], workdir=args.workdir, state='error', stage=name, params=params)
                    os.unlink(final_iddir + ".pid")
                    return 1

        else:
            if os.path.exists( finaldir + ".error" ):
                logging.error("Error found for %s run for %s" % (params['id'], name))
                if not hasattr(mod, "FAIL") or mod.FAIL != 'soft':
                    log_status(zk, pipeline=pipeline.name, id=params['id'], workdir=args.workdir, state='running', stage=name, params=params)
                    return 1
            logging.info("Results found for %s run for %s" % (params['id'], name))

        with open(os.path.join(pipeline.outdir, params['id'], name + ".output")) as handle:
            txt = handle.read()
            data = json.loads(txt)
            params_merge[name] = {}
            for row in data:
                params_merge[name][row[0]] = row[1]

        #print params, params_merge
    log_status(zk, pipeline=pipeline.name, id=params['id'], workdir=args.workdir, state='complete', stage=None, params=params)
    if args.clean:
        shutil.rmtree(os.path.abspath(os.path.join(args.workdir,params['id'])))
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

    pipeline = Pipeline(args.pipeline)
    srcs = glob(os.path.join(pipeline.base, "images", "*"))

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
            logging.info( "Installing %s" % (src_name) )
            cmd = "docker load"
            if not args.skip_sudo:
                cmd = "sudo " + cmd
            cmd = "cat %s | %s" % (src_tar, cmd)
            subprocess.check_call(cmd, shell=True)
        else:
            logging.info( "Already Installed: %s" % (src_name) )

def func_run(q, func, params):
    out = []
    try:
        for fname, f in func(params):
            out.append((fname,f))
        q.put( ('ok', out) )
    except Exception as exc:
        f = StringIO()
        traceback.print_exc(file=f)
        q.put( ('error', f.getvalue(), exc) )


def run_exec(args):
    logging.info("Starting Exec")

    pipeline = Pipeline(args.pipeline)

    with open(args.params) as handle:
        logging.info("Reading %s" % (args.params))
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
        finaldir = os.path.abspath(os.path.join(pipeline.outdir,work_id, name))
        error_message = None
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
                    #print "Count",  sum( list( a.is_alive() for a in procs) )
                    while sum( list( a.is_alive() for a in procs) ) >= args.ncpus:
                        time.sleep(1)

                files = []
                for p in procs:
                     o = q.get()
                     if o[0] == 'error':
                         sys.stderr.write(o[1])
                         error_message = o[1]
                         raise o[2]
                     files.extend(o[1])
                     p.join()
            except:
                with open(finaldir + ".error", "w") as err_handle:
                    if error_message is not None:
                        err_handle.write(error_message)
                    traceback.print_exc(file=err_handle)
                    return 1

            sys.stderr = orig_stderr
            sys.stdout = orig_stdout
            os.chdir(odir)

            if mod.STORE:
                shutil.move(params['outdir'], finaldir)
            else:
                shutil.move(params['outdir'], workdir)
            with open(os.path.join(pipeline.outdir, work_id, name + ".output"), "w") as handle:
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
    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)
    queue = zk.Queue(ZOO_BASE + pipeline.name + "/queue")

    for job in args.jobs:
        with open(job) as handle:
            txt = handle.read()
            try:
                meta = json.loads(txt)
                if not zk.exists(ZOO_BASE + pipeline.name + "/status/" + meta['id']):
                    print "queuing", meta['id']
                    queue.put(txt)
                else:
                    print "already queued", meta['id']
            except ValueError:
                print "Invalid file"
    zk.stop()

def run_reset(args):
    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)
    
    txt, info = zk.get(ZOO_BASE + pipeline.name + "/status/" + args.jobid)
    data = json.loads(txt)
    
    for c in zk.get_children(ZOO_BASE + pipeline.name + "/status/" + args.jobid):
        txt, info = zk.get(ZOO_BASE + pipeline.name + "/status/" + args.jobid + "/" + c)
        print txt
    
    print data
    
    queue = zk.Queue(ZOO_BASE + pipeline.name + "/queue")

def run_delete(args):
    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)    
    for job in args.jobs:
        zk.delete( ZOO_BASE + pipeline.name + "/status/" + job, recursive=True )
    zk.stop()
    

def run_queue_list(args):
    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)
    for c in zk.get_children(ZOO_BASE + pipeline.name + "/queue"):
        txt, info = zk.get(ZOO_BASE + pipeline.name + "/queue/" + c)
        data = json.loads(txt)
        print data['id']
    zk.stop()

def run_status_list(args):
    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)
    for c in zk.get_children(ZOO_BASE + pipeline.name + "/status"):
        txt, info = zk.get(ZOO_BASE + pipeline.name + "/status/" + c)
        data = json.loads(txt)
        print "\t".join( [c, data['state'], str(data['stage']), data['host']] )
    zk.stop()

def run_worker_list(args):
    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)
    for c in zk.get_children(ZOO_BASE + pipeline.name + "/workers"):
        txt, info = zk.get(ZOO_BASE + pipeline.name + "/workers/" + c)
        data = json.loads(txt)
        print "\t".join( [c, str(data['disk'])] )
    zk.stop()

def run_client(args):
    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)

    #make sure the base working directory exists
    if not os.path.exists(args.workdir):
        os.mkdir(args.workdir)
    workdir = tempfile.mkdtemp(dir=args.workdir, prefix="simplechain_client_")
    os.mkdir(os.path.join(workdir, "work"))
    try:
        while 1:
            queue = zk.Queue(ZOO_BASE + pipeline.name + "/queue")
            logging.info( "Getting Queue Item. Queue length: %d" % ( len(queue) ) )
            item_txt = queue.get()
            if item_txt is None:
                break
            item_data = json.loads(item_txt)
            logging.info( "Got item %s" % item_data['id'] )
            workfile = os.path.join(workdir, item_data['id'])
            with open(workfile, "w") as handle:
                handle.write(item_txt)
            cmd = [sys.executable, SCRIPT_PATH, pipeline.base, "run",
                "-z", args.zookeeper,
                "--docker",
                "--workdir", os.path.join(workdir, "work")]
            if args.clean:
                cmd += ['--clean']
            if args.skip_sudo:
                cmd += ['--skip-sudo']
            cmd += [workfile ]
            logging.info( "Running:" + " ".join(cmd) )
            try:
                subprocess.check_call(cmd)
                logging.info( "Request Complete" )
            except:
                logging.info( "Request Error!" )
    finally:
        #if args.clean:
        #    shutil.rmtree(workdir)
        zk.stop()


main_page="""<html>
<head><title>${name}</title></head>
<body>

<a href="#active">Active</a> (${len(active)}) | <a href="#error">Error</a> (${len(error)}) | <a href="#complete">Complete</a> (${len(complete)}) | <a href="#workers">Workers</a> (${len(workers)}) | <a href="#queued">Queued</a> (${len(queued)})

<hr>

<div>
    <div><a id="active"/>Active (${len(active)})</div>
    <table>
        <tr><td>ID</td><td>HOST</td><td>Stage</td><td>State</td><td>Workdir</td></tr>
    % for row in active:
        ${makerow(['id', 'host', 'stage', 'status', 'workdir'], row)}
    % endfor
    </table>
</div>
<hr>
<div>
    <div><a id="error"/>Error (${len(error)})</div>
    <table>
        <tr><td>ID</td><td>HOST</td><td>Stage</td><td>State</td><td>Workdir</td></tr>
    % for row in error:
        ${makerow(['id', 'host', 'stage', 'status', 'workdir'], row)}
    % endfor
    </table>
</div>
<hr>
<div>
    <div><a id="complete"/>Complete (${len(complete)})</div>
    <table>
        <tr><td>ID</td><td>Host</td><td>workdir</td></tr>
    % for row in complete:
        ${makerow(['id', 'host', 'workdir'], row)}
    % endfor
    </table>
</div>
<hr>
<div>
    <div><a id="workers"/>Workers (${len(workers)})</div>
    <table>
        <tr><td>HOST</td><td>DISK</td></tr>
    % for row in workers:
        ${makerow(['host', 'disk'], row)}
    % endfor
    </table>
</div>
<hr>
<div>
    <div><a id="queued"/>Queued (${len(queued)})</div>
    <table>
        <tr><td>ID</td><td>PARAMS</td></tr>
    % for row in queued:
        ${makerow(['id', 'data'], row)}
    % endfor
    </table>
</div>
</body>
</html>
<%def name="makerow(order, row)">
    <tr>
    % for name in order:
        <td>${row[name]}</td>
    % endfor
    </tr>
</%def>
"""

status_page="""
<html>
<head><title>${name}</title></head>
<body>
    <div>ID</div><div>${id}</div>
    <div>Host</div><div>${host}</div>
    <div>State</div><div>${state}</div>
    <div>Current Stage</div><div>${stage}</div>
    <div>Stages</div>
    % for s in stages:
        ${s['name']} (${s['state']})
    % endfor
</body>
</html>
"""

stage_page="""
<html>
<head><title>${name}</title></head>
<body>
    <div>STDOUT</div>
    <pre>
    ${stdout}
    </pre>
    <div>STDEDD</div>
    <pre>
    ${stderr}
    </pre>
</body>
</html>
"""


def run_web(args):
    if tornado is None:
        raise Exception("tornado is not installed")

    if mako is None:
        raise Exception("mako is not installed")

    pipeline = Pipeline(args.pipeline)
    zk = zookeeper_init(args)

    class MainHandler(tornado.web.RequestHandler):
        def get(self):
            active = []
            complete = []
            error = []
            for child in zk.get_children(ZOO_BASE + pipeline.name + "/status"):
                txt, info = zk.get(ZOO_BASE + pipeline.name + "/status/" + child)
                data = json.loads(txt)
                if data.get('state', "na") == "complete":
                    complete.append( {
                        'id' : "<a href='/status/%s'>%s</a>" % (child,child),
                        'status' : data.get('state', 'unknown'),
                        'stage':data.get('stage', 'unknown'),
                        'host':data.get('host', 'unknown'),
                        'workdir' : data.get('workdir', 'unknown'),
                        'data' : json.dumps(data)
                    })                    
                elif data.get('state', "na") == "error":
                    error.append( {
                        'id' : "<a href='/status/%s'>%s</a>" % (child,child),
                        'status' : data.get('state', 'unknown'),
                        'stage':data.get('stage', 'unknown'),
                        'host':data.get('host', 'unknown'),
                        'workdir' : data.get('workdir', 'unknown'),
                        'data' : json.dumps(data)
                    })                   
                else:
                    active.append( {
                        'id' : "<a href='/status/%s'>%s</a>" % (child,child),
                        'status' : data.get('state', 'unknown'),
                        'stage':data.get('stage', 'unknown'),
                        'host':data.get('host', 'unknown'),
                        'workdir' : data.get('workdir', 'unknown'),
                        'data' : json.dumps(data)
                    })
            active.sort(key=lambda x:x['host'])
            workers = []
            for child in zk.get_children(ZOO_BASE + pipeline.name + "/workers"):
                txt, info = zk.get(ZOO_BASE + pipeline.name + "/workers/" + child)
                data = json.loads(txt)
                workers.append( {
                    'host' : data.get('host', 'unknown'),
                    'disk':data.get('disk', 'na')
                })
            workers.sort(key=lambda x:x['host'])

            queued = []
            for child in zk.get_children(ZOO_BASE + pipeline.name + "/queue"):
                txt, info = zk.get(ZOO_BASE + pipeline.name + "/queue/" + child)
                data = json.loads(txt)
                queued.append( {
                    'id' : data['id'],
                    'data' : json.dumps(data)
                })

            self.write(mako.template.Template(main_page).render(name=pipeline.name, queued=queued, error=error, active=active, complete=complete, workers=workers))

    class StatusHandler(tornado.web.RequestHandler):
        def get(self, id):
            txt, info = zk.get(ZOO_BASE + pipeline.name + "/status/" + id)
            data = json.loads(txt)
            stages = []
            for stage_name in zk.get_children(ZOO_BASE + pipeline.name + "/status/" + id):
                txt, info = zk.get(ZOO_BASE + pipeline.name + "/status/" + id + "/" + stage_name)
                stage_data = json.loads(txt)
                stages.append( {'name' : "<a href='%s/%s'>%s</a>" % (id, stage_name, stage_name), 'state' : stage_data['state']})
            self.write(mako.template.Template(status_page).render(name=id, id=id, stages=stages, **data))

    class StageHandler(tornado.web.RequestHandler):
        def get(self, id, stage):

            stdout = "NA"
            stderr = "NA"

            stderr_path = os.path.join(pipeline.outdir,id,stage + ".stderr")
            stdout_path = os.path.join(pipeline.outdir,id,stage + ".stdout")

            if os.path.exists(stderr_path):
                with open(stderr_path) as handle:
                    stderr = handle.read()

            if os.path.exists(stdout_path):
                with open(stdout_path) as handle:
                    stdout = handle.read()

            self.write(mako.template.Template(stage_page).render(name=id, id=id, stderr=stderr, stdout=stdout))


    application = tornado.web.Application([
        (r"/", MainHandler),
        (r"/status/([^/]*)", StatusHandler),
        (r"/status/([^/]*)/(.*)", StageHandler)
    ])

    application.listen(args.port)
    tornado.ioloop.IOLoop.instance().start()




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pipeline")


    subparsers = parser.add_subparsers(title="subcommand")

    parser_run = subparsers.add_parser('run')
    parser_run.add_argument("-z", "--zookeeper", default=None)
    parser_run.add_argument("--workdir", default="work")
    parser_run.add_argument("--docker", action="store_true", default=False)
    parser_run.add_argument("--skip-sudo", action="store_true", default=False)
    parser_run.add_argument("--clean", action="store_true", default=False)
    parser_run.add_argument("-s", "--stage", dest="stages", action="append", type=int)
    parser_run.add_argument("workfile")
    parser_run.set_defaults(func=run_pipeline)

    parser_build = subparsers.add_parser('build')
    parser_build.add_argument("--skip-sudo", action="store_true", default=False)
    parser_build.add_argument("-f", "--flush", action="store_true", default=False)
    parser_build.set_defaults(func=run_build)

    parser_install = subparsers.add_parser('install')
    parser_install.add_argument("--skip-sudo", action="store_true", default=False)
    parser_install.set_defaults(func=run_install)

    parser_exec = subparsers.add_parser('exec')
    parser_exec.add_argument("module_name")
    parser_exec.add_argument("params")
    parser_exec.add_argument("--workdir", default="work")
    parser_exec.add_argument("--ncpus", type=int, default=8)
    parser_exec.set_defaults(func=run_exec)

    parser_queue = subparsers.add_parser('queue')
    parser_queue.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_queue.add_argument("jobs", nargs="+")
    parser_queue.set_defaults(func=run_queue)

    parser_queue_list = subparsers.add_parser('queue-list')
    parser_queue_list.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_queue_list.set_defaults(func=run_queue_list)

    parser_status_list = subparsers.add_parser('status-list')
    parser_status_list.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_status_list.set_defaults(func=run_status_list)

    parser_worker_list = subparsers.add_parser('worker-list')
    parser_worker_list.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_worker_list.set_defaults(func=run_worker_list)

    parser_reset = subparsers.add_parser('reset')
    parser_reset.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_reset.add_argument("jobid")
    parser_reset.set_defaults(func=run_reset)

    parser_delete = subparsers.add_parser('delete')
    parser_delete.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_delete.add_argument("jobs", nargs="+")
    parser_delete.set_defaults(func=run_delete)


    parser_client = subparsers.add_parser('client')
    parser_client.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_client.add_argument("--workdir", default="/tmp")
    parser_client.add_argument("--skip-sudo", action="store_true", default=False)
    parser_client.add_argument("--clean", action="store_true", default=False)
    parser_client.set_defaults(func=run_client)

    parser_web = subparsers.add_parser('web')
    parser_web.add_argument("-z", "--zookeeper", default="127.0.0.1:2181")
    parser_web.add_argument("-p", "--port", type=int, default=8888)
    parser_web.set_defaults(func=run_web)

    args = parser.parse_args()
    sys.exit(args.func(args))
