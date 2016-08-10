from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
import paramiko
from paramiko import WarningPolicy

import warnings
import time
import os
import threading


def add(clusterDictionary,config):
    print "Adding Student Accounts to all Hosts in Cluster"
    warnings.simplefilter("ignore")
    paramiko.util.log_to_file("/tmp/paramiko.log")

    for node in clusterDictionary["clusterNodes"]:

        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())
                ssh.connect(node["externalIP"], 22, "root", password=config.get("cape-settings", "ROOT_PW"),
                            timeout=120)
                (stdin, stdout, stderr) = ssh.exec_command("mkdir -p /data/home")
                stderr.readlines()
                stdout.readlines()
                for student in range(1,int(config.get("lab-settings", "NUM_STUDENTS"))+1):
                    studnum = str(student)
                    print "Creating Student Account:"+str(student)

                    (stdin, stdout, stderr) = ssh.exec_command("useradd -s /bin/bash -u "+str(5000+student)+ " -c 'Student Account " + studnum + "' --base-dir /data/home -m student"+studnum)
                    stderr.readlines()
                    stdout.readlines()
                    ssh.exec_command("echo '\\timing on' >> /data/home/student"+studnum+"/.psqlrc")
                    stderr.readlines()
                    stdout.readlines()


                connected = True
            except Exception as e:
                print e
                print node["nodeName"] + ": Attempting SSH Connection"
                time.sleep(3)
                if attemptCount > 40:
                    print "Failing Process"
                    exit()
            finally:
                ssh.close()
