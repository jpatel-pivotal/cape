import os
import time
import warnings

import paramiko
from paramiko import WarningPolicy


def addStudentAccount(clusterNode):
    print "Adding Student Accounts to all Hosts in Cluster"
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, os.environ.get("SSH_USERNAME"), None, pkey=None,key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")), timeout=120)
            for userNum in range(1, int(os.environ.get("NUM_STUDENTS"))):
                userName = os.environ.get("BASE_USERNAME") + str(userNum).zfill(2)
                homeDir = os.environ.get("BASE_HOME") + "/home"
                pw = os.environ.get("BASE_PASSWORD") + str(userNum).zfill(2)
                (stdin, stdout, stderr) = ssh.exec_command(
                    "sudo mkdir -p " + homeDir + ";sudo useradd -b " + homeDir + " -s " + "/bin/bash -m " + userName)
                stderr.readlines()
                stdout.readlines()
                (stdin, stdout, stderr) = ssh.exec_command("sudo sh -c 'echo " + pw + " | passwd --stdin " + userName + "'")
                stderr.readlines()
                stdout.readlines()
                (stdin, stdout, stderr) = ssh.exec_command(
                    "sudo -u "+ userName +" echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
                stdout.readlines()
                stderr.readlines()
                (stdin, stdout, stderr) = ssh.exec_command(
                     "sudo -u "+ userName +"echo 'export MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n' >> ~/.bashrc")
                stdout.readlines()
                stderr.readlines()

            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                print "Failing Process"
                exit()
        finally:
            ssh.close()


def addStudentAccount(clusterNode):
    print "Load Data"
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, os.environ.get("SSH_USERNAME"), None, pkey=None,key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")), timeout=120)
            userName = os.environ.get("BASE_USERNAME") + str(userNum).zfill(2)
            labsDir = os.environ.get("BASE_HOME") + "/labs"
            (stdin, stdout, stderr) = ssh.exec_command(
                "sudo mkdir -p " + homeDir + ";sudo useradd -b " + homeDir + " -s " + "/bin/bash -m " + userName)
            stderr.readlines()
            stdout.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo sh -c 'echo " + pw + " | passwd --stdin " + userName + "'")
            stderr.readlines()
            stdout.readlines()
            (stdin, stdout, stderr) = ssh.exec_command(
                "sudo -u "+ userName +" echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command(
                 "sudo -u "+ userName +"echo 'export MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n' >> ~/.bashrc")
            stdout.readlines()
            stderr.readlines()

            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                print "Failing Process"
                exit()
        finally:
            ssh.close()