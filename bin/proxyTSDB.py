#!/usr/bin/python

import logging
import os
import sys
import re
import json
import threading
from optparse import OptionParser
import time
import socket
import SocketServer
import BaseHTTPServer
from logging import FileHandler
import collections
import dirq.QueueSimple
import marshal
import signal
import atexit
import random

# GLOBAL VARIABLES

DEFAULT_LOG = '/var/log/proxyTSDB.log'
DEFAULT_OUT = '/var/log/proxyTSDB.out'
DEFAULT_ERR = '/var/log/proxyTSDB.err'
LOG = logging.getLogger('proxyTSDB')
DEFAULT_DIRQ_PATH = '/var/tmp/proxyTSDB-metrics'

PROXYTSDB_VERSION = '{"version":"","timestamp":""}'
METRICNAME_REGEX = r"^(sys|app|net|db)(\.([-_./a-zA-Z0-9])+)+$"

ALIVE = False
METRIC_QUEUE = None
DISK_METRIC_QUEUE = None

RAM_MAX_SIZE = 0
DISK_MAX_SIZE = 0
DISK_MAX_INODE = 0

USE_IPV6 = False


class MetricReceiver(threading.Thread):
    """Thread for execute HTTP server which receive REST request"""

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self._receiver = MetricServer(('', 4242), MetricRequestHandler, queue)

    def run(self):
        self._receiver.serve_forever()

    def shutdown(self):
        self._receiver.shutdown()


class MetricSender(threading.Thread):
    """class to define base methods of a metricSender"""

    def __init__(self, queue, persistentQueueFileName,
                 persistentQueueMaxElts, sleepTime=60):
        threading.Thread.__init__(self)
        self._sleepTime = sleepTime
        self._sleepEvent = threading.Event()
        self._sleepEvent.clear()
        self._queue = queue
        self._diskQueue = None
        self._alive = True
        if persistentQueueFileName:
            self._diskQueue = dirq.QueueSimple.QueueSimple(
                persistentQueueFileName, granularity=3600)
            self._diskMaxElts = persistentQueueMaxElts

    def run(self):

        self.start_sending()

    def stop(self):
        self._alive = False
        self._sleepEvent.set()

    def start_sending(self):
        global DISK_MAX_SIZE
        while self._alive:
            self._pre_send()
            LOG.info("Sending metrics...")
            curMetric = None
            eName = None
            try:
                try:
                    # Send metric in MemoryQueue
                    while len(self._queue) > 0:
                        curMetric = self._queue.popleft()
                        self.sendMetric(curMetric)
                except IndexError:
                    pass
                except Exception as e:
                    if curMetric:
                        self._queue.appendleft(curMetric)
                    raise e

                try:
                    if self._diskQueue:
                        # If MemoryQueueMetrics has sent,
                        # Send metrics in DiskQueue
                        eName = self._diskQueue.first()
                        while eName:
                            if not self._diskQueue.lock(eName):
                                LOG.warning("Couldn't lock: %s" % eName)
                                eName = self._diskQueue.next()
                                continue
                            try:
                                metrics = marshal.loads(
                                    self._diskQueue.get(eName))
                                for curMetric in metrics:
                                    self.sendMetric(curMetric)
                                del metrics
                            except EOFError:
                                pass
                            self._diskQueue.remove(eName)
                            eName = self._diskQueue.next()

                except Exception as e:
                    if eName and self._diskQueue:
                        self._diskQueue.unlock(eName)
                    raise e

                LOG.info("... done.")
            except Exception as e:
                LOG.error(str(type(e))+" "+str(e))

            # write on disk if it remains metrics in MemoryQueue
            if self._diskQueue:
                curr_metric = None
                while len(self._queue) > 0:
                    metrics = []
                    try:
                        if DISK_MAX_SIZE > 0 and \
                                du(self._diskQueue.path)[0] > (
                                    DISK_MAX_SIZE*1024*1024):
                            curElt = self._diskQueue.first()
                            self._diskQueue.lock(curElt)
                            self._diskQueue.remove(curElt)
                        try:
                            while True:
                                metrics.append(self._queue.popleft())
                                if len(metrics) >= 1000:
                                    self._diskQueue.add(marshal.dumps(metrics))
                                    metrics = []
                        except IndexError:
                            pass
                        if len(metrics) > 0:
                            self._diskQueue.add(marshal.dumps(metrics))
                    except IndexError:
                        pass
                    del metrics

            self._post_send()
            self._purge()
            self._sleepEvent.wait(self._sleepTime)
            self._sleepEvent.clear()

        # Save unsent metrics from memory to disk before shutdown
        if self._diskQueue:
            curr_metric = None
            while len(self._queue) > 0:
                metrics = []
                try:
                    if DISK_MAX_SIZE > 0 and \
                            du(self._diskQueue.path)[0] > (
                                DISK_MAX_SIZE*1024*1024):
                        curElt = self._diskQueue.first()
                        self._diskQueue.lock(curElt)
                        self._diskQueue.remove(curElt)
                    try:
                        while True:
                            metrics.append(self._queue.popleft())
                            if len(metrics) >= 1000:
                                self._diskQueue.add(marshal.dumps(metrics))
                                metrics = []
                    except IndexError:
                        pass
                    if len(metrics) > 0:
                        self._diskQueue.add(marshal.dumps(metrics))
                except IndexError:
                    pass
                del metrics

    def sendMetric(self, metric):
        LOG.info("SENDING: "+str(metric))
        req = self._formatRequest(metric)
        self._send(req)
        LOG.info("SENT!")

    def _pre_send(self):
        pass

    def _post_send(self):
        pass

    def _purge(self):
        global RAM_MAX_SIZE
        while sys.getsizeof(self._queue) >= RAM_MAX_SIZE*1024*1024:
            self._queue.popleft()

        if self._diskQueue is not None:
            self._diskQueue.purge()
            global DISK_MAX_SIZE
            global DISK_MAX_INODE
            if DISK_MAX_SIZE > 0 or DISK_MAX_INODE > 0:
                size, inode = du(self._diskQueue.path)
                while size and inode and (
                        (DISK_MAX_SIZE > 0 and size > DISK_MAX_SIZE*1024*1024)
                        or (DISK_MAX_INODE > 0 and inode > DISK_MAX_INODE)
                        ):
                    curElt = self._diskQueue.first()
                    self._diskQueue.lock(curElt)
                    self._diskQueue.remove(curElt)
                    size = None
                    inode = None
                    size, inode = du(self._diskQueue.path)
            self._diskQueue.purge()

    def _formatRequest(self, metric):
        raise Exception("To Be Defined")

    def _send(request):
        raise Exception("To Be Defined")


class MetricSenderOpenTSDB(MetricSender):
    """sub class of metricSender specific to OpenTSDB"""

    def __init__(self, queue, persistentQueueFileName, persistentQueueMaxElts,
                 host_port, sleepTime=60):
        MetricSender.__init__(self, queue, persistentQueueFileName,
                              persistentQueueMaxElts, sleepTime)
        self._host = host_port[0]
        self._port = host_port[1]
        self.socket = None
        self._reconnect()

    def _reconnect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(1)
        port = random.randint(42000, 43000)
        try:
            self.socket.bind(('', port))
            self.socket.connect((self._host, self._port))
            self.socket.settimeout(5)
        except socket.error:
            pass

    def _formatRequest(self, metric):
        req = "put"
        req += " "+str(metric[0])
        req += " "+str(metric[1])
        req += " "+str(metric[2])
        for tag in metric[3].items():
            req += " "+str(tag[0])+"="+str(tag[1])
        return req

    def _send(self, request):
        self.socket.sendall(request)
        self.socket.sendall("\n")
        LOG.debug("SENT : "+request)

    def _pre_send(self):
        try:
            self.socket.settimeout(1)
            self.socket.sendall("version\n")
            self.socket.recv(1024)
            self.socket.settimeout(5)
        except socket.error:
            LOG.warning("Socket Error, Reconnect to OpenTSDB...")
            self._reconnect()

    def __del__(self):
        self.socket.close()


class MetricHTTPRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def log_request(code=0,size=0):
        pass

    def do_GET(self):
        if re.match(r"/api/version", self.path) is not None:
            self.do_api_version()

    def do_POST(self):
        if re.match(r"/api/put", self.path) is not None:
            self.do_api_put()
        if re.match(r"/api/version", self.path) is not None:
            self.do_api_version()

    def do_api_put(self):
        length = int(self.headers['Content-Length'])
        data = self.rfile.read(length)
        in_json = json.loads(data)
        try:
            if type(in_json) is list:
                metadd = 0
                for curr in in_json :
                    metric = formatMetric(curr)
                    self.server.queue.append(metric)
                    metadd += 1
                self.send_response(200,str(metadd)+' metric(s) added')
                self.end_headers()
            else:
                metric = formatMetric(in_json)
                self.server.queue.append(metric)
                self.send_response(200,'metric '+in_json['metric']+' added')
                self.end_headers()
        except Exception as e:
            LOG.warning(e.message)
            LOG.warning(e)
            self.send_error(500,"Can't add metric: "+e.message)
            self.end_headers()

    def do_api_version(self):
        response = PROXYTSDB_VERSION
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)


class MetricTelnetRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        try:
            while True:
                data = bytearray()
                while True:
                    lim = \
                        (self.request.recv(
                            1024,
                            socket.MSG_PEEK
                        ).find(b"\n") + 1) or 1024
                    b = self.request.recv(lim)
                    if not b:
                        break
                    data += b
                    if data.endswith(b"\n"):
                        break
                data = str(data).strip()
                if not data:
                    break
                LOG.debug(data)
                if re.match(r"put", data) is not None:
                    datas = data.split(" ")
                    while True:
                        try:
                            datas.remove('')
                        except ValueError:
                            break
                    if re.match(METRICNAME_REGEX, datas[1]):
                        tags = {}
                        for tag in datas[4:]:
                            tab = tag.split("=")
                            tags[tab[0]] = tab[1]

                        if not tags:
                            LOG.warning("Metric does not have at least 1 tag : "+str(data))
                            self.request.sendall('ERROR: Needs at least 1 tag : '+datas[1])
                        else:
                            metric = (datas[1], datas[2], datas[3], tags)
                            LOG.debug("ADD: "+str(metric))
                            self.server.queue.append(metric)
                    else:
                        if re.match(r"^tcollector(\.([-_./a-zA-Z0-9])+)+$",datas[1]):
                            LOG.debug("Ignoring tcollector processus itself metrics: "+str(data))
                        else:
                            LOG.warning("Metric TRASHED invalid name : "+str(data))
                            self.request.sendall('ERROR: Metric name (sys|app|net|db).* : '+datas[1])
                elif re.match(r"status", data) is not None:
                    result = "Status:\n"
                    global METRIC_QUEUE
                    if METRIC_QUEUE is not None:
                        result += "RAM Queue elements : " + \
                            str(len(METRIC_QUEUE)) + "\n"
                    global DISK_METRIC_QUEUE
                    if DISK_METRIC_QUEUE is not None:
                        result += "DISK Queue elements : " + \
                            str(DISK_METRIC_QUEUE.count())+"\n"

                    self.request.sendall(result)
                elif re.match(r"version", data) is not None:
                    self.request.sendall(PROXYTSDB_VERSION)
                else:
                    self.request.sendall('ERROR: '+data)
        except socket.error:
            pass


class MetricServer(SocketServer.TCPServer):
    """Server with queue and set of threaded requests"""

    # Decides how threads will act upon termination of the
    # main process
    daemon_threads = False

    def __init__(self, server_address, RequestHandlerClass,
                 queue, bind_and_activate=True):
        self.allow_reuse_address = True
        try:
            global USE_IPV6
            if USE_IPV6:
                self.address_family = socket.AF_INET6
            else:
                self.address_family = socket.AF_INET
        except AttributeError:
            self.address_family = socket.AF_INET

        SocketServer.TCPServer.__init__(self, server_address,
                                        RequestHandlerClass, bind_and_activate)
        self.queue = queue
        self._request_threads = set()

    def process_request_thread(self, request, client_address):
        """Same as in BaseServer but as a thread.
        In addition, exception handling is done here.
        """
        try:
            self.finish_request(request, client_address)
            self.close_request(request)
        except:
            self.handle_error(request, client_address)
            self.close_request(request)
        try:
            self._request_threads.remove(threading.currentThread())
        except KeyError:
            pass

    def process_request(self, request, client_address):
        """Start a new thread to process the request.
        And add it to the thread set"""
        t = threading.Thread(target=self.process_request_thread,
                             args=(request, client_address))
        t.daemon = self.daemon_threads
        self._request_threads.add(t)
        t.start()

    def shutdown(self):
        SocketServer.TCPServer.shutdown(self)
        try:
            while True:
                t = self._request_threads.pop()
                t._Thread__args[0].shutdown(2)  # SHUT_RDRW == 2
                t.join()
        except KeyError:
            pass


class MetricRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        data = self.request.recv(7, SocketServer.socket.MSG_PEEK).strip()
        if re.match(r"(GET|POST)", data) is not None:
            MetricHTTPRequestHandler(self.request,
                                     self.client_address, self.server)
        else:
            MetricTelnetRequestHandler(self.request,
                                       self.client_address, self.server)


def formatMetric(jsonMetric):
    """parse a JSON metric and return a tuple
    of it if it is correctly formated"""

    try:
        jsonMetric['metric']
        jsonMetric['timestamp']
        float(jsonMetric['value'])
        jsonMetric['tags']
    except (TypeError, KeyError):
        LOG.warning("Parse JSON metric")
        raise Exception(
            "Incomplete metric : {'metric': '*', 'timestamp': *, 'value': *, 'tags':{'tag1':'value1,*}}"
        )

    if not re.match(METRICNAME_REGEX, jsonMetric['metric']):
        LOG.warning("Regex metric name")
        raise Exception("Metric name (sys|app|net|db).* : " +
                        jsonMetric['metric'])

    if not jsonMetric['tags']:
        LOG.warning("'tags' field is empty : "+jsonMetric['metric'])
        raise Exception("'tags' field is empty : "+jsonMetric['metric'])

    return (jsonMetric['metric'], jsonMetric['timestamp'], jsonMetric['value'],
            jsonMetric['tags'])


def du(path):
    size = 0
    nbinode = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for d in dirnames:
            dp = os.path.join(dirpath, d)
            size += os.path.getsize(dp)
            nbinode += 1
        for f in filenames:
            fp = os.path.join(dirpath, f)
            size += os.path.getsize(fp)
            nbinode += 1
    return size, nbinode


def setup_logging(logfile=DEFAULT_LOG):
    """Sets up logging and associated handlers."""

    LOG.setLevel(logging.INFO)
    logHandler = FileHandler(logfile, 'a')
    fmt = '%(asctime)s %(name)s[%(process)d] %(levelname)s: %(message)s'
    logHandler.setFormatter(logging.Formatter(fmt))
    LOG.addHandler(logHandler)


def parse_cmdline(argv):
    """Parses the command-line."""
    parser = OptionParser()

    parser.add_option('-D', '--daemonize', dest='daemonize',
                      action='store_true',
                      default=False, help='Run as a background daemon.')
    parser.add_option('-H', '--host', dest='host', default='localhost',
                      metavar='HOST',
                      help='Hostname to use to connect to the TSD.')
    parser.add_option('-P', '--pidfile', dest='pidfile',
                      default='/var/run/proxyTSDB.pid',
                      metavar='FILE', help='Write our pidfile')
    parser.add_option('--loglevel', dest='loglevel', type='str',
                      default='INFO',
                      help='Loglevel for output: '
                           '<CRITICAL|ERROR|WARNING|INFO|DEBUG> (default INFO)')
    parser.add_option('--logfile', dest='logfile', type='str',
                      default=DEFAULT_LOG,
                      help='Filename where logs are written to.')
    parser.add_option('--diskqueuepath', dest='dirq_path', type='str',
                      help='Path where to store the disk persistent queue')
    parser.add_option('--buffertype', dest='buffer_type', type='str',
                      default='RAM', help='<DISK|RAM> (default RAM)')
    parser.add_option('--rammaxsize', dest='ram_max_size', type='int',
                      default=512,
                      help='in MB (0 for unlimited)(default 512)')
    parser.add_option('--diskmaxsize', dest='disk_max_size', type='int',
                      default=1024,
                      help='in MB (0 for unlimited)(default 1024)')
    parser.add_option('--diskmaxinode', dest='disk_max_inode', type='int',
                      default=190000,
                      help='(0 for unlimited)(default 190000)')
    parser.add_option('--sendperiod', dest='sleep_time', type='int',
                      default=30,
                      help='Second to wait between sends (default 30s)')
    parser.add_option('--listen', dest='listen_addr', type='str',
                      default='127.0.0.1',
                      help='Listening Addr (default 127.0.0.1)')
    parser.add_option('--ipv6', dest='use_ipv6', action='store_true',
                      default=False,
                      help='Use enable ipv6 for listening (default no)')

    (options, args) = parser.parse_args(args=argv[1:])
    return (options, args)


def daemonize():
    """Become a background daemon."""
    if os.fork():
        os._exit(0)
    os.chdir("/")
    os.umask(022)
    os.setsid()
    os.umask(0)
    if os.fork():
        os._exit(0)
    stdin = open(os.devnull)
    stdout = open(os.devnull, 'w')
    os.dup2(stdin.fileno(), 0)
    os.dup2(stdout.fileno(), 1)
    os.dup2(stdout.fileno(), 2)
    stdin.close()
    stdout.close()
    os.umask(022)
    for fd in xrange(3, 1024):
        try:
            os.close(fd)
        except OSError:    # This FD wasn't opened...
            pass    # ... ignore the exception.
    sys.stdout = open(DEFAULT_OUT, 'a')
    sys.stderr = open(DEFAULT_ERR, 'a')


def write_pid(pidfile):
    """Write our pid to a pidfile."""

    f = open(pidfile, "w")
    try:
        f.write(str(os.getpid()))
    finally:
        f.close()


def shutdown_signal(signum, frame):
    LOG.warning("shutting down, got signal %d", signum)
    shutdown()


def shutdown():
    global ALIVE
    if not ALIVE:
        return
    ALIVE = False


def main(argv):
    """The main entry point and loop."""

    options, args = parse_cmdline(argv)
    if options.daemonize:
        daemonize()
    setup_logging(options.logfile)
    try:
        LOG.setLevel(logging._levelNames[options.loglevel])
    except KeyError:
        LOG.error("Unknown log level : " + options.loglevel)
    if options.pidfile:
        write_pid(options.pidfile)

    atexit.register(shutdown)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    global RAM_MAX_SIZE
    RAM_MAX_SIZE = options.ram_max_size
    global DISK_MAX_SIZE
    DISK_MAX_SIZE = options.disk_max_size
    global DISK_MAX_INODE
    DISK_MAX_INODE = options.disk_max_inode
    global USE_IPV6
    USE_IPV6 = options.use_ipv6

    # Convert from MB to nb elements
    ram_buff_size = (options.ram_max_size * 1048576)/962
    disk_buff_size = (options.disk_max_size * 1048576)/962

    metricQueue = collections.deque([], ram_buff_size)
    global METRIC_QUEUE
    METRIC_QUEUE = metricQueue

    LOG.debug("Starting Receiver...")
    server = MetricServer((options.listen_addr, 4242),
                          MetricRequestHandler, metricQueue)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start()
    LOG.info("Receiver Started!")

    LOG.debug("Starting Sender...")
    sleep_time = options.sleep_time
    dirq_path = options.dirq_path
    if 'DISK' != options.buffer_type:
        dirq_path = None
    sender = MetricSenderOpenTSDB(metricQueue, dirq_path, disk_buff_size,
                                  (options.host, 4242), sleep_time)
    global DISK_METRIC_QUEUE
    DISK_METRIC_QUEUE = sender._diskQueue
    sender.start()
    LOG.info("Sender Started!")

    global ALIVE
    ALIVE = True
    while ALIVE:
        time.sleep(60)

    LOG.debug("Shutting down -- joining receiver thread...")
    server.shutdown()
    server_thread.join()
    LOG.debug("Joined!")

    LOG.debug("Shutting down -- joining sender thread...")
    sender.stop()
    sender.join()
    LOG.debug("Joined!")

    LOG.info("Exiting")
    sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)
