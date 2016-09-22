import os
import threading
import time

import paramiko
from paramiko import WarningPolicy

from LabBuilder import AccessHostPrepare
from LabBuilder import StudentAccounts

def installGPDB(clusterDictionary, downloads):
    print clusterDictionary["clusterName"] + ": Installing Greenplum Database on Cluster"
    threads = []
    masterNode = {}
    for clusterNode in clusterDictionary["clusterNodes"]:
        if "master1" in clusterNode["role"]:
            masterNode = clusterNode
        elif "access" in clusterNode["role"]:
            accessNode = clusterNode
        uncompressFilesThread = threading.Thread(target=uncompressFiles, args=(clusterNode, downloads))
        threads.append(uncompressFilesThread)
        uncompressFilesThread.start()
    for x in threads:
        x.join()

    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        prepFilesThread = threading.Thread(target=prepFiles, args=(clusterNode,))
        threads.append(prepFilesThread)
        prepFilesThread.start()
    for x in threads:
        x.join()

    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        installBitsThread = threading.Thread(target=installBits, args=(clusterNode,))
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




    print clusterDictionary["clusterName"] + ": Database Installation Complete"
    print clusterDictionary["clusterName"] + ": Initializing Greenplum Database"
    initDB(masterNode, clusterDictionary["clusterName"])
    print clusterDictionary["clusterName"] + ": Database Initialization Complete"
    verifyInstall(masterNode, clusterDictionary)
    print clusterDictionary["clusterName"] + ": Installing Machine Learning Capabilities"
    installComponents(masterNode, downloads)
    print clusterDictionary["clusterName"] + ": Machine Learning Install Complete"
    print clusterDictionary["clusterName"] + ": Preparing Access Host "

    # NEED TO MAKE OPTIONAL
    # threads = []
    # for clusterNode in clusterDictionary["clusterNodes"]:
    #     installDSPackagesThread = threading.Thread(target=installDSPackages, args=(clusterNode,))
    #     threads.append(installDSPackagesThread)
    #     installDSPackagesThread.start()
    # for x in threads:
    #     x.join()
    AccessHostPrepare.installComponents(clusterDictionary)
    modifyPHGBA(masterNode, accessNode)
    setGPADMINPW(masterNode)

    # NEEDS TO BE OPTIONAL
    # threads = []
    # for clusterNode in clusterDictionary["clusterNodes"]:
    #     addStudentAccountThread = threading.Thread(target=StudentAccounts.addStudentAccount, args=(clusterNode,))
    #     threads.append(addStudentAccountThread)
    #     addStudentAccountThread.start()
    # for x in threads:
    #     x.join()

    print clusterDictionary["clusterName"] + ": Access Host Install Complete"



def installComponents(masterNode, downloads):
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())

            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)
            (stdin, stdout, stderr) = ssh.exec_command("createlang plpythonu -d template1")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("createlang plpythonu -d gpadmin")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("gppkg -i /tmp/madlib*.gppkg")
            stdout.readlines()
            stderr.readlines()
            ssh.exec_command("$GPHOME/madlib/bin/madpack install -s madlib -p greenplum -c gpadmin@" + masterNode[
                "nodeName"] + "/template1")
            stdout.readlines()
            stderr.readlines()
            ssh.exec_command("$GPHOME/madlib/bin/madpack install -s madlib -p greenplum -c gpadmin@" + masterNode[
                "nodeName"] + "/gpadmin")
            stdout.readlines()
            stderr.readlines()

            connected = True

        except Exception as e:
            print e
            print masterNode["nodeName"] + ": Waiting on Database Connection"
            time.sleep(3)
            if attemptCount > 10:
                print "Failing Process: Please Verify Database Manually."
                exit()


def verifyInstall(masterNode, clusterDictionary):
    numberSegments = int(clusterDictionary["nodeQty"]) - 3
    totalSegmentDBs = numberSegments * int(clusterDictionary["segmentDBs"])
    # This should login to master and run some checks.
    # dbURI = queries.uri(masterNode["externalIP"], port=5432, dbname="template0", user="gpadmin", password=str(os.environ.get("GPADMIN_PW")))
    # Might need to wait on GPDB to come up
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())

            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)

            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and status = 'u';\"")
            upSegments = int((stdout.readlines())[2])
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and status = 'd';\"")
            downSegments = int((stdout.readlines())[2])
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0;\"")
            segments = int(stdout.readlines()[2])
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and role='p';\"")
            primarySegments = int(stdout.readlines()[2])
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and role='m';\"")

            mirrorSegments = int(stdout.readlines()[2])
            stderr.readlines()

            connected = True

            if ((totalSegmentDBs * 2) == upSegments) and (totalSegmentDBs == primarySegments) and (
                totalSegmentDBs == mirrorSegments):
                print clusterDictionary["clusterName"] + ": Greenplum Database Initialization Verified"
            else:
                print clusterDictionary[
                          "clusterName"] + ": Something went wrong with the Database initialization, please verify manually"

        except Exception as e:
            print e
            print masterNode["nodeName"] + ": Waiting on Database Connection"
            time.sleep(3)
            if attemptCount > 10:
                print "Failing Process: Please Verify Database Manually."
                exit()

####  WOULD RATHER THIS GO IN ACCESS HOST PREPARE AND THEN RENAME IT TO ENABLEDATASCIENCE
#####

def installDSPackages(clusterNode):
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())

            ssh.connect(clusterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)

            # FIX FOR GENSIM
            (stdin, stdout, stderr) = ssh.exec_command("echo -e 'import sys\nsys.setdefaultencoding(\"utf-8\")' >> /usr/local/greenplum-db-4.3.9.1/ext/python/lib/python2.6/site-packages/sitecustomize.py")
            stdout.readlines()
            stderr.readlines()

            # INSTALL PIP FOR GP-PYTHON
            (stdin, stdout, stderr) = ssh.exec_command("wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py;python /tmp/get-pip.py --no-cache-dir")
            stdout.readlines()
            stderr.readlines()

            # INSTALL NUMPY
            (stdin, stdout, stderr) = ssh.exec_command("pip install numpy==1.9.3 -U --no-cache-dir")
            stdout.readlines()
            stderr.readlines()

            # INSTALL SCIPY
            (stdin, stdout, stderr) = ssh.exec_command("export CXX=/usr/bin/g++;pip install scipy==0.18.0 -U --no-cache-dir")
            stdout.readlines()
            stderr.readlines()

            # INSTALL SCIKIT-LEARN
            (stdin, stdout, stderr) = ssh.exec_command("pip install scikit-learn==0.17.1 -U --no-cache-dir")
            stdout.readlines()
            stderr.readlines()

            # INSTALL nltk
            (stdin, stdout, stderr) = ssh.exec_command("pip install nltk==3.1 -U --no-cache-dir")
            stdout.readlines()
            stderr.readlines()

            # INSTALL GENSIM
            (stdin, stdout, stderr) = ssh.exec_command("cp /usr/lib64/python2.6/lib-dynload/bz2.so /usr/local/greenplum-db-4.3.9.1/ext/python/lib/python2.6/lib-dynload/bz2.so")
            stdout.readlines()
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command("pip install gensim -U --no-cache-dir --no-dependencies")
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
###

def setPaths(clusterNode):
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())

            ssh.connect(clusterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)

            (stdin, stdout, stderr) = ssh.exec_command(
                "echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command(
                "echo 'export MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n' >> ~/.bashrc")
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


def cleanUp(clusterDictionary):
    print "cleanUp"


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


def setGPADMINPW(masterNode):
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)
            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"alter user gpadmin with password '" + str(os.environ.get("GPADMIN_PW")) + "';\"")

            # (stdin, stdout, stderr) = ssh.exec_command("alter user gpadmin with password '"+str(os.environ.get("GPADMIN_PW"))+ "';")
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


def modifyPHGBA(masterNode, accessNode):
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ.get("GPADMIN_PW")), timeout=120)
            (stdin, stdout, stderr) = ssh.exec_command(
                "echo 'host all gpadmin " + accessNode['internalIP'] + "/0 md5' >> /data/master/gpseg-1/pg_hba.conf")
            stdout.readlines()
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command("gpstop -a -r")
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


def uncompressFiles(clusterNode, downloads):
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())

            ssh.connect(clusterNode["externalIP"], 22, str(os.environ.get("SSH_USERNAME")), None, pkey=None,
                        key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")), timeout=120)

            for file in downloads:
                if ".zip" in file["NAME"]:
                    (stdin, stdout, stderr) = ssh.exec_command("cd /tmp;unzip ./" + file["NAME"])
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
            # (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/master;sudo chown -R gpadmin: /data")

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


def initDB(clusterNode, clusterName):
    # Read the template, modify it, write it to the cluster directory and the master


    with open(os.getcwd()+"../../templates/gpinitsystem_config.template", 'r+') as gpConfigTemplate:
        gpConfigTemplateData = gpConfigTemplate.read()
        gpConfigTemplateModData = gpConfigTemplateData.replace("%MASTER%", clusterNode["nodeName"])

    with open(os.environ.get("CAPE_HOME") + "/clusterConfigs/" + str(clusterName) + "/gpinitsystem_config",
              'w') as gpConfigCluster:
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
            (stdin, stdout, stderr) = ssh.exec_command(
                "source /usr/local/greenplum-db/greenplum_path.sh;gpinitsystem -c /tmp/gpinitsystem_config.cape -a")
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
