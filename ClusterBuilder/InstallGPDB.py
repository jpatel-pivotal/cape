import paramiko
from paramiko import WarningPolicy
import warnings
import requests
import json
import time
import os
import threading
import queries
import socket


def installGPDB(clusterDictionary,downloads):

    threads =[]
    masterNode = {}
    for clusterNode in clusterDictionary["clusterNodes"]:
        if  "master1" in  clusterNode["role"]:
            masterNode = clusterNode
        uncompressFilesThread = threading.Thread(target=uncompressFiles, args=(clusterNode,downloads))
        threads.append(uncompressFilesThread)
        uncompressFilesThread.start()
    for x in threads:
        x.join()

    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        prepFilesThread = threading.Thread(target=prepFiles, args=(clusterNode, ))
        threads.append(prepFilesThread)
        prepFilesThread.start()
    for x in threads:
       x.join()

    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        installBitsThread = threading.Thread(target=installBits, args=(clusterNode, ))
        threads.append(installBitsThread)
        installBitsThread.start()
    for x in threads:
       x.join()

    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        makeDirectoriesThread = threading.Thread(target=makeDirectories, args=(clusterNode,))
        threads.append(makeDirectoriesThread)
        makeDirectoriesThread.start()
    for x in threads:
        x.join()

    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        setPathsThread = threading.Thread(target=setPaths, args=(clusterNode,))
        threads.append(setPathsThread)
        setPathsThread.start()
    for x in threads:
        x.join()



    initDB(masterNode,clusterDictionary["clusterName"])
    verifyInstall(masterNode,clusterDictionary)

def modifyPGHBA(masterNode):
    # Append the local ip of ths host running this tool to pg_hba.conf
    #ipaddress.
    ipAddress = socket.gethostbyname(socket.gethostname())
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)
            print "echo 'host  all gpadmin '+str(ipAddress)+'/32   trust' > $MASTER_DATA_DIRECTORY/pg_hba.conf"
            (stdin, stdout, stderr) = ssh.exec_command("echo 'host  all gpadmin '+str(ipAddress)+'/32   trust' > $MASTER_DATA_DIRECTORY/pg_hba.conf")
            stdout.readlines()
            stderr.readlines()
            connected = True
        except Exception as e:
            print masterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                print "Failing Process"
                exit()
        finally:
            ssh.close()



def verifyInstall(masterNode,clusterDictionary):

    numberSegments = int(clusterDictionary["nodeQty"]) - 3
    totalSegmentDBs = numberSegments * int(clusterDictionary["segmentDBs"])
    # This should login to master and run some checks.
    dbURI = queries.uri(masterNode["externalIP"], port=5432, dbname="template0", user="gpadmin", password=str(os.environ.get("GPADMIN_PW")))
    # Might need to wait on GPDB to come up
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            with queries.Session(dbURI) as session:
                # Check Up segmentDB's vs number there should be.
                connected = True
                upSegments = session.query("SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and status = 'u';")
                print "upSegments:",upSegments
                downSegments = session.query("SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and status = 'd';")
                print "downSegments:",downSegments
                totalSegments =  session.query("SELECT count(*) FROM gp_segment_configuration WHERE content >= 0;")
                print "totalSegments:",totalSegments
                primarySegments = session.query("SELECT count(*) FROM gp_segment_configuration WHERE content >= 0; and role='p';")
                print "primarySegments:",primarySegments
                mirrorSegments = session.query("SELECT count(*) FROM gp_segment_configuration WHERE content >= 0; and role='m';")
                print "mirrorSegments:",mirrorSegments
        except Exception as e:
            print e
            print masterNode["nodeName"] + ": Waiting on Database Connection"
            time.sleep(3)
            if attemptCount > 10:
                print "Failing Process: Please Verify Database Manually."
                exit()

def setPaths(clusterNode):


    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())

            ssh.connect(clusterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)

            (stdin, stdout, stderr) = ssh.exec_command("echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("echo 'export MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n' >> ~/.bashrc")
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






def makeDirectories(clusterNode):
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, str(os.environ.get("SSH_USERNAME")), None, pkey=None,
                        key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")),
                        timeout=120)
            if "master" in clusterNode["role"]:
                (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/master")
                stdout.readlines()
                stderr.readlines()
            else:
                (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/primary")
                stdout.readlines()
                stderr.readlines()
                (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/mirror")
                stdout.readlines()
                stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo chown -R gpadmin: /data")
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








def uncompressFiles(clusterNode,downloads):
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())

            ssh.connect(clusterNode["externalIP"], 22, str(os.environ.get("SSH_USERNAME")), None, pkey=None,key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")), timeout=120)

            for file in downloads:
                if ".zip" in file["NAME"]:
                    (stdin, stdout, stderr) = ssh.exec_command("cd /tmp;unzip ./"+file["NAME"])
                    stdout.readlines()
                    stderr.readlines()
                elif ".gz" in file["NAME"]:
                    (stdin, stdout, stderr) = ssh.exec_command("cd /tmp;tar xvfz ./" + file["NAME"])
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



def prepFiles(clusterNode):

    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, str(os.environ.get("SSH_USERNAME")), None, pkey=None,
                        key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")), timeout=120)

            (stdin, stdout, stderr) = ssh.exec_command("sudo sed -i 's/more <</cat <</g' /tmp/greenplum-db*.bin")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo sed -i 's/agreed=/agreed=1/' /tmp/greenplum-db*.bin")
            stdout.readlines()
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command(
                "sudo sed -i 's/pathVerification=/pathVerification=1/' /tmp/greenplum-db*.bin")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command(
                "sudo sed -i 's/user_specified_installPath=/user_specified_installPath=${installPath}/' /tmp/greenplum-db*.bin")
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


def installBits(clusterNode):
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, str(os.environ.get("SSH_USERNAME")), None, pkey=None,
                        key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")),
                        timeout=120)

            (stdin, stdout, stderr) = ssh.exec_command("sudo /tmp/greenplum-db*.bin")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo chown -R gpadmin: /usr/local/greenplum-db*")
            stdout.readlines()
            stderr.readlines()

            # Go ahead and change owner on Data Disk(s).   Only One now, but change this if more disks are added.
            #(stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/master;sudo chown -R gpadmin: /data")

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

def initDB(clusterNode,clusterName):


# Read the template, modify it, write it to the cluster directory and the master


    with open(os.environ.get("CONFIGS_PATH")+"/gpinitsystem_config.template", 'r+') as gpConfigTemplate:
        gpConfigTemplateData = gpConfigTemplate.read()
        gpConfigTemplateModData = gpConfigTemplateData.replace("%MASTER%", clusterNode["nodeName"])

    with open(os.environ.get("CAPE_HOME")+"/clusterConfigs/"+str(clusterName)+"/gpinitsystem_config", 'w') as gpConfigCluster:
        gpConfigCluster.write(gpConfigTemplateModData)


    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, os.environ.get("SSH_USERNAME"), None, pkey=None,
                        key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")),
                        timeout=120)
            sftp = ssh.open_sftp()
            sftp.put("gpinitsystem_config", "/tmp/gpinitsystem_config.cape")


            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                print "Failing Process"
                exit()
        finally:
            ssh.close()

    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)
            (stdin, stdout, stderr) = ssh.exec_command("source /usr/local/greenplum-db/greenplum_path.sh;gpinitsystem -c /tmp/gpinitsystem_config.cape -a")
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

    print "Greenplum Database Install Complete."