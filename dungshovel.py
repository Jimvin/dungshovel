#!/usr/bin/python3
import sys
import threading
from queue import Queue
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.exceptions import NoAuthError
import logging
import json
import base64
import getopt

class getData (threading.Thread):
    def __init__(self, id, hosts, opts):
        logging.info("Thread-%d: Started" % id)
        threading.Thread.__init__(self)
        self.hosts = hosts
        self.id = id
        self.opts = opts

    def run(self):
        while self.hosts.empty() == False:
            ip = self.hosts.get()
            logging.info("Thread-%d: Reading from %s" % (self.id, ip))
            zk = KazooClient(hosts="%s:2181" % ip)
            try:
                zk.start()
            except KazooTimeoutError:
                logging.info("Thread-%d: KazooTimeoutError connecting to %s" % (self.id, ip))
                zk.stop()
                next
            if self.opts['recursive']:
                recursive_get(zk, ip, self.opts['basepath'], self.opts)
            else:
                dir_get(zk, ip, self.opts['basepath'], self.opts)
            zk.stop()
            self.hosts.task_done()
        logging.info("Thread-%d: finished" % self.id)


def recursive_get(zk, ip, dirname, opts):
    try:
        child_dirs = zk.get_children(dirname)
        try:
            if opts['get_data']:
                try:
                    data = zk.get(dirname)[0]
                    if data is not None:
                        data = data.decode("unicode-escape")
                    else:
                        data = ""
                    if opts['b64encode_data']:
                        data = base64.b64encode(data)
                except TypeError:
                    data = ""
            else:
                data = ""
            results = {"ip": ip, "znode": dirname, "data": data}
        except TypeError:
            results = {"ip": ip, "znode": dirname, "data": ""}
        except Exception as e:
            sys.stderr.write("Error getting data from %s:%s\n" % (ip, dirname))
            sys.stderr.write(str(e) + "\n")
            results = {"ip": ip, "znode": dirname, "data": ""}

        if len(child_dirs) > 0:
            for subdir in child_dirs:
                if dirname == "/":
                    recursive_get(zk, ip, dirname + subdir, opts)
                else:
                    recursive_get(zk, ip, dirname + "/" + subdir, opts)
        print(json.dumps(results))
    except NoAuthError:
        sys.stderr.write("Authentication error getting data from %s:%s\n" % (ip, dirname))
        results = {"ip": ip, "znode": dirname, "data": ""}
        print(json.dumps(results))
    except:
        e = sys.exc_info()
        sys.stderr.write("recursive_get: %s" % str(e))


def dir_get(zk, ip, dirname, opts):
    try:
        child_dirs = zk.get_children(dirname)
        for subdir in child_dirs:
            data = ""
            if opts['get_data']:
                try:
                    data = zk.get(dirname)[0].decode("unicode-escape")
                    if opts['b64encode_data']:
                        data = base64.b64encode(data)
                except TypeError:
                    data = ""
            if dirname == "/":
                print(json.dumps({"ip": ip, "znode": dirname + subdir, "data": data}))
            else:
                print(json.dumps({"ip": ip, "znode": dirname + "/" + subdir, "data": data}))
    except Exception as e:
        sys.stderr.write("Unable to get directory listing from %s:%s %s\n" % (ip, dirname, str(e)))


def usage():
    print("Usage: dungshovel.py -f <target_file> [-t|--threads <number_of_threads>] [-v] [-d|--data] [-n|--non-recursive] [-b|--basepath <path>]")
    sys.exit(1)


def main():
    # opts contains configuration options we pass into the worker threads
    opts = dict()
    opts['hostfile'] = None         # File containing list of target IP addresses
    opts['num_threads'] = 1         # Number of threads to run, each thread handles on host at a time
    opts['verbose'] = False         # Verbose debug output
    opts['get_data'] = False   # Download data from znodes
    opts['b64encode_data'] = False   # Download data from znodes
    opts['recursive'] = True        # Recursive by default, use -n for non-recursive get
    opts['basepath'] = "/"          # Path to start searching from

    options, remainder = getopt.getopt(sys.argv[1:], 'f:t:vdDnb:h', ['file=', 'threads=', 'verbose', 'data', 'non-recursive', 'basepath', 'help'])
    for opt, arg in options:
        if opt in ('-f', '--file'):
            opts['hostfile'] = arg
        elif opt in ('-t', '--threads'):
            opts['num_threads'] = int(arg)
        elif opt in ('-v', '--verbose'):
            opts['verbose'] = True
        elif opt in ('-d', '--data'):
            opts['get_data'] = True
        elif opt in ('-D'):
            opts['get_data'] = True
            opts['b64encode_data'] = True
        elif opt in ('-n', '--non-recursive'):
            opts['recursive'] = False
        elif opt in ('-b', '--basepath'):
            opts['basepath'] = arg
        elif opt in ('-h', '--help'):
            usage()

    hosts = Queue()
    if opts['hostfile'] is None:
        if len(remainder) == 1:
            hosts.put(remainder[0])
        else:
            usage()
    else:
        with open(opts['hostfile']) as f:
            for host in f:
                hosts.put(host.rstrip())

    if opts['verbose']:
        sys.stderr.write("hostfile   : %s\n" % opts['hostfile'])
        sys.stderr.write("threads    : %d\n" % opts['num_threads'])
        sys.stderr.write("remainder  : %s\n" % remainder)
        sys.stderr.write("recursive  : %s\n" % opts['recursive'])
        sys.stderr.write("basepath  : %s\n" % opts['basepath'])

    t = []
    for i in range(opts['num_threads']):
        thread1 = getData(i, hosts, opts)
        thread1.start()
        t.append(thread1)


if __name__ == '__main__':
    main()

