#!/usr/bin/env python

from __future__ import print_function, division, unicode_literals

import argparse
import os
import sys
import signal
import subprocess
import platform
import tarfile
import shutil
import time
import json
from collections import defaultdict
from random import random

if sys.version_info.major == 3:
    from urllib.request import urlretrieve
else:
    from urllib import urlretrieve

PING_COMMAND = "ping -c 10 %s | tail -1 | awk '{print $4}'"
CURL_COMMAND = "curl -o /dev/null -w '{\\n\"time_namelookup\":  %{time_namelookup},\\n\"time_connect\":  %{time_connect},\\n\"time_appconnect\":  %{time_appconnect},\\n\"time_pretransfer\":  %{time_pretransfer},\\n\"time_redirect\":  %{time_redirect},\\n\"time_starttransfer\":  %{time_starttransfer},\\n\"total\": %{time_total}\\n}\\n' "

######
# Ping
######
def get_pings(hosts):
    print("Pinging hosts...")
    results = {}
    for host in hosts:
        out = subprocess.check_output(PING_COMMAND % host, shell=True)
        try:
            ping_min, ping_avg, ping_max, ping_stddev = [float(s) for s in out.decode("utf8").strip().split("/")]
        except:
            print("ERROR: Failed to ping {}. Please report this error.".format(host))
        else:
            results[host] = {'min': ping_min, 'avg': ping_avg, 'max': ping_max, 'stddev': ping_stddev}
    return results


def curl_stats(url):
    try:
        out = subprocess.check_output(CURL_COMMAND + url, shell=True).decode("utf8")
        return json.loads(out)
    except:
        print("ERROR: Please ensure you are connected to the internet and rerun the script.")
        print("If this persists, please contact the sender.")


def average_by_keys(l):
    by_key = defaultdict(list)
    for d in l:
        for k,v in d.items():
            by_key[k].append(v)
    return {
        "samples": dict(by_key),
        "average": {str(k): sum(v) / len(v) for k, v in by_key.items()}
    }


def curl_and_average(url, samples=10):
    print("Gathering curl stats...")
    curls = []
    for i in range(samples):
        time.sleep(2*random())
        print("Attempt {} of {}".format(i+1, samples))
        curls.append(curl_stats(url))
    return average_by_keys(curls)

class IPFSNode:
    def __init__(self, id, address):
        assert id in address
        self.id = id
        self.address = address


class IPFSFile:
    def __init__(self, url, hash):
        self.url = url
        self.hash = hash

HOSTS = {
    "npfoss.mit.edu": IPFSNode(
        "QmSzpPdWxHMmPDPy8A4igWWwTVPcBHZXueYLzWQsBwHFuU",
        "/ip4/18.18.96.12/tcp/14001/p2p/QmSzpPdWxHMmPDPy8A4igWWwTVPcBHZXueYLzWQsBwHFuU"
    ),
    "p1.mit.edu": IPFSNode(
        "QmX5Q5cF1F1yvcS2QWMXPu1GsP5kVajH6UbLrTBXWHx8HM",
        "/ip4/18.18.248.83/tcp/4001/ipfs/QmX5Q5cF1F1yvcS2QWMXPu1GsP5kVajH6UbLrTBXWHx8HM"
    ),
}

RESIDENTIAL_HOSTS = {
    "lobster.moinnadeem.com": IPFSNode(
        "QmeTPkjcFsgBvAm3K18JbC3kDGNbDZ9cs4fibnDb2Lr8je",
        "/ip4/66.31.16.203/tcp/9701/ipfs/QmeTPkjcFsgBvAm3K18JbC3kDGNbDZ9cs4fibnDb2Lr8je"
    ),
}

FILES = {
    "ipad": IPFSFile(
        "https://www.apple.com/105/media/us/ipad-pro/2020/7be9ce7b-fa4e-4f54-968b-4a7687066ed8/films/feature/ipad-pro-feature-tpl-cc-us-2020_1280x720h.mp4",
        "QmfBsQa4iZRsKkELk7QGP4acN4sX6WfvBvVWT4Tz5yuE24"
    ),
    "iphone": IPFSFile(
        "https://www.apple.com/v/home/f/images/heroes/iphone-se/hero__dvsxv8smkkgi_large.jpg",
        "Qmay7eKcsxZ5UkraAucEGtTsv6LzA5hn3P8JnQQcWaVwcN"
    ),
}

class IPFSClient:
    def __init__(self, ipfs_binary, ipfs_path):
        self.ipfs_binary = ipfs_binary
        self.ipfs_path = ipfs_path
        self.devnull = open(os.devnull, "w")
        self._ensure_path()

    def teardown(self):
        self.kill_daemon()
        self._unset_path()

    def launch_daemon(self):
        if self.daemon_available():
            return
        elif self.daemon_running():
            self.kill_daemon()
        print("Launching daemon...")
        while not self.daemon_running():
            try:
                subprocess.Popen(
                    self._get_command("daemon &"), shell=True, stdout=self.devnull)
            except:
                self.kill_daemon()
        while not self.daemon_available():
            print("Waiting for daemon... [may take a min, do not quit]")
            time.sleep(1)
        print("Success.")

    def daemon_available(self):
        try:
            subprocess.check_call(
                self._get_command("stats bitswap"),
                shell=True,
                stdout=self.devnull,
                stderr=self.devnull,
            )
            return True
        except:
            return False

    def daemon_running(self):
        try:
            return int(subprocess.check_output("pgrep ipfs", shell=True).strip())
        except:
            return False

    def kill_daemon(self):
        pid = self.daemon_running()
        while self.daemon_running():
            print("Killing daemon...")
            try:
                os.kill(pid, signal.SIGKILL)
            except:
                return

    def init(self):
        try:
            subprocess.check_call(
                self._get_command("init"),
                shell=True,
                stdout=self.devnull,
                stderr=self.devnull,
            )
        except:
            pass

    def is_connected(self, ipfs_node):
        addrs = self.check_output("swarm addrs").decode("utf8")
        return ipfs_node.id in addrs

    def ensure_connected(self, ipfs_node):
        if not self.is_connected(ipfs_node):
            self.check_output("swarm connect {}".format(ipfs_node.address))

    def ensure_disconnected(self, ipfs_node):
        if self.is_connected(ipfs_node):
            self.check_output("swarm disconnect {}".format(ipfs_node.address))

    def call(self, command):
        return subprocess.call(self._get_command(command), shell=True)

    def time_get(self, hash):
        return float(self.time("get {}".format(hash)).strip())

    def time(self, command):
        # TIMEFORMAT=%R
        if os.environ.get("TIMEFORMAT") != "%R":
            os.environ["TIMEFORMAT"] = "%R"
        try:
            return subprocess.check_output("(time " + self._get_command(command) + "&> /dev/null ) 2>&1", shell=True, executable='bash').decode("utf8")
        except:
            print("ERROR: Failed to run ipfs command: {}. Please report this error".format(command))
            exit()
        os.environ.pop("TIMEFORMAT", None)
        os.unsetenv("TIMEFORMAT")

    def check_output(self, command):
        try:
            return subprocess.check_output(self._get_command(command), shell=True)
        except:
            print("ERROR: Failed to run ipfs command: {}. Please report this error".format(command))
            exit()

    def _ensure_path(self):
        if os.environ.get("IPFS_PATH") != self.ipfs_path:
            os.environ["IPFS_PATH"] = self.ipfs_path

    def _unset_path(self):
        os.environ.pop("IPFS_PATH", None)
        os.unsetenv("IPFS_PATH")

    def _get_command(self, command):
        return "{} {}".format(self.ipfs_binary, command)

    def get_stats(self, file, hosts, samples=10):
        print("Collecting ipfs stats...")
        gets = []
        tries = 0
        while len(gets) < samples and tries < samples*2:
            tries += 1
            print("Attempt {} out of (min: {}, max: {})".format(tries, samples, samples*2))
            for h in hosts:
                self.ensure_connected(h)
            self.check_output("repo gc")
            if os.path.exists(file.hash):
                os.remove(file.hash)
            t = self.time_get(file.hash)
            if all(self.is_connected(h) for h in hosts):
                gets.append(t)
        if os.path.exists(file.hash):
            os.remove(file.hash)
        return {"tries": tries, "gets": gets, "average": sum(gets) / len(gets)}

class IPFSDownloader:
    BASE_URL = "https://dist.ipfs.io/go-ipfs/v0.5.0/go-ipfs_v0.5.0_"
    FOLDER_NAME = "go-ipfs"
    PATH_TO_FOLDER = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), FOLDER_NAME)

    @classmethod
    def run(cls):
        if os.path.exists(cls.PATH_TO_FOLDER):
            return cls.PATH_TO_FOLDER
        cls.download_and_extract(cls.get_ipfs_download_link())
        return cls.PATH_TO_FOLDER

    @classmethod
    def delete(cls):
        print("Deleting folder...")
        if os.path.exists(cls.PATH_TO_FOLDER):
            shutil.rmtree(cls.PATH_TO_FOLDER)

    @classmethod
    def get_ipfs_download_link(cls):
        return cls.BASE_URL + cls.get_ipfs_download_postfix()

    @staticmethod
    def get_ipfs_download_postfix():
        is_64bits = sys.maxsize > 2 ** 32
        is_arm = "arm" in platform.machine()
        platform_name = sys.platform

        if platform_name == "linux" or platform_name == "linux2":
            # linux
            if is_arm:
                if is_64bits:
                    return "linux-arm64.tar.gz"
                return "linux-arm.tar.gz"

            if is_64bits:
                return "linux-amd64.tar.gz"
            return "linux-386.tar.gz"

        elif platform_name == "darwin":
            # OS X
            if is_64bits:
                return "darwin-amd64.tar.gz"
            return "darwin-386.tar.gz"
        elif platform_name == "win32":
            # Windows...
            sys.exit("Windows is not supported")

    @staticmethod
    def download_and_extract(url):
        print("Downloading...")
        file_tmp = urlretrieve(url, filename=None)[0]
        tar = tarfile.open(file_tmp)
        print("Extracting...")
        tar.extractall()


def main():
    parser = argparse.ArgumentParser(
        description="Experiments. Please contact the sender for questions.")
    parser.add_argument("--host",
                        help="Host a long-running node for the experiment in the background",
                        action="store_true")
    parser.add_argument("--kill",
                        help="Kill the background node",
                        action="store_true")
    parser.add_argument("-d", "--dotipfs",
                        help="/path/to/.ipfs/ (default: .ipfs)")
    parser.add_argument("-a", "--all",
                        help="Test both large and small files",
                        action="store_true")
    args = parser.parse_args()

    if sys.platform == "win32":
        sys.exit("Windows is not supported")

    ipfs_folder = IPFSDownloader.run()
    if args.dotipfs:
        dotipfs = args.dotipfs
    else:
        dotipfs = os.path.join(ipfs_folder, ".ipfs/")
    ipfs = IPFSClient(os.path.join(ipfs_folder, "ipfs"), dotipfs)
    ipfs.init()

    if args.host:
        print(
"""
Thank you for volunteering! You've been selected to host a node in
our experiment.

We expect your computer to remain powered on and connected to the
internet for an extended period of time while we conduct the
experiment in the background.

Please do not tamper with the ./go-ipfs directory or the running ipfs
daemon process.

When you ultimately wish to turn off the experiment, you may run:
./experiment.py --kill
""")

        ipfs.launch_daemon()
        print("Connecting to server...")
        # ipfs.check_output("swarm connect {}".format(HOSTS["npfoss.mit.edu"].address))

        # pin the files
        for f in FILES.values():
            print("pinning {}...".format(f.hash))
            ipfs.check_output("pin add {}".format(f.hash))

        print("\nIMPORTANT: Please copy and send the following back to the experiment leader:")
        ipfs.call("id")

        print("Host is running! Pleae do not close this shell.")

        while True:
            ipfs.launch_daemon()
            time.sleep(60)

    elif args.kill:
        ipfs.kill_daemon()
    else:
        RESULTS = {'VERSION': 'v0.4.0'}
        print("""Thank you for participating! Please remain connected to the
internet, and refrain from streaming videos.""")
        RESULTS['pings'] = {
            'mit': get_pings(HOSTS),
            'residential': get_pings(RESIDENTIAL_HOSTS)
        }
        RESULTS['curl'] = {
            'iphone': curl_and_average(FILES["iphone"].url, samples=5),
        }
        ipfs.launch_daemon()
        for h in HOSTS.values():
            ipfs.ensure_disconnected(h)
        RESULTS['ipfs_residential'] = {
            'iphone': ipfs.get_stats(FILES["iphone"], RESIDENTIAL_HOSTS.values(), samples=10),
        }
        for h in RESIDENTIAL_HOSTS.values():
            ipfs.ensure_disconnected(h)
        RESULTS['ipfs_mit'] = {
            'iphone': ipfs.get_stats(FILES["iphone"], HOSTS.values(), samples=10),
        }
        if args.all:
            print("Running slow tests...")
            RESULTS['curl']['ipad'] = curl_and_average(FILES["ipad"].url, samples=3)
            ipfs.launch_daemon()
            for h in HOSTS.values():
                ipfs.ensure_disconnected(h)
            RESULTS['ipfs_residential']['ipad'] = ipfs.get_stats(FILES["ipad"], RESIDENTIAL_HOSTS.values(), samples=3)
            for h in RESIDENTIAL_HOSTS.values():
                ipfs.ensure_disconnected(h)
            RESULTS['ipfs_mit']['ipad'] = ipfs.get_stats(FILES["ipad"], HOSTS.values(), samples=3)
        print("Writing output...")
        with open('out.json', 'w') as f:
            json.dump(RESULTS, f, indent=4, sort_keys=True)
        print("\nSUCCESS! Please send out.json back to the sender :)")

    ipfs.kill_daemon()
    ipfs.teardown()
    IPFSDownloader.delete()

if __name__ == "__main__":
    main()
