import os
import sys
import threading
import time
import traceback
import paramiko
import json
import logging
from paramiko import WarningPolicy

from LabBuilder import AccessHostPrepare

def installGPDB(clusterDictionary, downloads):
    print clusterDictionary["clusterName"] + ": Installing Greenplum Database on Cluster"
    logging.debug('Installing GPDB with Dictionary: ' + json.dumps(clusterDictionary))
    threads = []
    masterNode = {}
    accessNode = {}
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
    logging.info(clusterDictionary["clusterName"] + ': Database Installation Complete')
    print clusterDictionary["clusterName"] + ": Initializing Greenplum Database"
    initDB(masterNode, clusterDictionary["clusterName"])
    print clusterDictionary["clusterName"] + ": Database Initialization Complete"
    verifyInstall(masterNode, clusterDictionary)
    print clusterDictionary["clusterName"] + ": Installing Machine Learning Capabilities"
    installComponents(masterNode, downloads)
    print clusterDictionary["clusterName"] + ": Machine Learning Install Complete"

    # NEED TO MAKE OPTIONAL
    # threads = []
    # for clusterNode in clusterDictionary["clusterNodes"]:
    #     installDSPackagesThread = threading.Thread(target=installDSPackages, args=(clusterNode,))
    #     threads.append(installDSPackagesThread)
    #     installDSPackagesThread.start()
    # for x in threads:
    #     x.join()

    if accessNode:
        print clusterDictionary["clusterName"] + ": Preparing Access Host "
        AccessHostPrepare.installComponents(clusterDictionary)
        print clusterDictionary["clusterName"] + ": Access Host Install Complete"
        #modifyPHGBA(masterNode)
        modifyPHGBA(accessNode)
        print clusterDictionary["clusterName"] + ": Access Host configured to connect Complete"

    setGPADMINPW(masterNode)


def installComponents(masterNode, downloads):
    logging.info('installComponents Started on: ' + str(masterNode["nodeName"]))
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + str(masterNode["nodeName"]))
            logging.debug('SSH IP: ' + masterNode["externalIP"] +
                          ' User: gpadmin')
            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ["GPADMIN_PW"]), timeout=120)
            (stdin, stdout, stderr) = ssh.exec_command("createlang plpythonu -d template1")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            (stdin, stdout, stderr) = ssh.exec_command("createlang plpythonu -d gpadmin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            (stdin, stdout, stderr) = ssh.exec_command("gppkg -i /tmp/madlib*.gppkg")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            ssh.exec_command("$GPHOME/madlib/bin/madpack install -s madlib -p greenplum -c gpadmin@" + masterNode[
                "nodeName"] + "/template1")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            ssh.exec_command("$GPHOME/madlib/bin/madpack install -s madlib -p greenplum -c gpadmin@" + masterNode[
                "nodeName"] + "/gpadmin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            connected = True

        except Exception as e:
            print e
            print masterNode["nodeName"] + ": Waiting on Database Connection"
            time.sleep(3)
            if attemptCount > 10:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process: Please Verify Database Manually."
                exit()
    logging.info('installComponents Completed on: ' + str(masterNode["nodeName"]))

def verifyInstall(masterNode, clusterDictionary):
    logging.info('verifyInstall Started on: ' + str(masterNode["nodeName"]))
    numberSegments = int(clusterDictionary["segmentCount"])
    logging.debug('SegCount: ' + str(numberSegments))
    totalSegmentDBs = numberSegments * int(clusterDictionary["segmentDBs"])
    logging.debug('TotalSegmentDBs in CLuster: ' + str(totalSegmentDBs))
    #totalSegmentDBs = numberSegments * int(os.environ.get("SEGMENTDBS"))



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
            logging.debug('Connecting to Node: ' + str(masterNode["nodeName"]))
            logging.debug('SSH IP: ' + masterNode["externalIP"] +
                          ' User: gpadmin')
            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ["GPADMIN_PW"]), timeout=120)
            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"SELECT version() ;\"")
            return_code = stdout.channel.recv_exit_status()
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            if return_code != 0:
                print("Returned: " + str(return_code))
                sys.exit("Failed to connect to Database using psql: Please Verify Database Manually.")
            else:
                print (masterNode["nodeName"] + ": Performing detailed database verification")
                (stdin, stdout, stderr) = ssh.exec_command(
                    "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and status = 'u';\"")
                upSegments = int((stdout.readlines())[2])
                logging.debug('UpSegements: ' + str(upSegments))
                logging.debug(stderr.readlines())

                (stdin, stdout, stderr) = ssh.exec_command(
                    "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and status = 'd';\"")
                downSegments = int((stdout.readlines())[2])
                logging.debug('downSegments: ' + str(downSegments))
                logging.debug(stderr.readlines())

                (stdin, stdout, stderr) = ssh.exec_command(
                    "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0;\"")
                segments = int(stdout.readlines()[2])
                logging.debug('segments: ' + str(segments))
                logging.debug(stderr.readlines())

                (stdin, stdout, stderr) = ssh.exec_command(
                    "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and role='p';\"")
                primarySegments = int(stdout.readlines()[2])
                logging.debug('primarySegments: ' + str(primarySegments))
                logging.debug(stderr.readlines())

                (stdin, stdout, stderr) = ssh.exec_command(
                    "psql -c \"SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 and role='m';\"")

                mirrorSegments = int(stdout.readlines()[2])
                logging.debug('mirrorSegments: ' + str(mirrorSegments))
                logging.debug(stderr.readlines())

                connected = True

                if ((totalSegmentDBs * 2) == upSegments) and (totalSegmentDBs == primarySegments) and (
                    totalSegmentDBs == mirrorSegments):
                    print clusterDictionary["clusterName"] + ": Greenplum Database Initialization Verified"
                    logging.info('verifyInstall Completed on: ' + str(masterNode["nodeName"]))
                else:
                    print clusterDictionary[
                          "clusterName"] + ": Something went wrong with the Database initialization, please verify manually"
                    logging.info('GPDB Primary and Mirror Counts do not match. Failing to Verify!')

        except Exception as e:
            print e
            print traceback.print_exc()
            print masterNode["nodeName"] + ": Waiting on Database Connection"
            time.sleep(3)
            if attemptCount > 10:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process: Please Verify Database Manually."
                exit()


def installDSPackages(clusterNode):
    logging.info('installDSPackages Started on: ' + str(clusterNode["nodeName"]))
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + str(clusterNode["nodeName"]))
            logging.debug('SSH IP: ' + clusterNode["externalIP"] +
                          ' User: gpadmin')
            ssh.connect(clusterNode["externalIP"], 22, "gpadmin", str(os.environ["GPADMIN_PW"]), timeout=120)

            # FIX FOR GENSIM
            logging.info('Gensim fix')
            (stdin, stdout, stderr) = ssh.exec_command("echo -e 'import sys\nsys.setdefaultencoding(\"utf-8\")' >> /usr/local/greenplum-db-4.3.9.1/ext/python/lib/python2.6/site-packages/sitecustomize.py")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # INSTALL PIP FOR GP-PYTHON
            logging.info('Installing pip')
            (stdin, stdout, stderr) = ssh.exec_command("wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py;python /tmp/get-pip.py --no-cache-dir")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # INSTALL NUMPY
            logging.info('Installing numpy')
            (stdin, stdout, stderr) = ssh.exec_command("pip install numpy==1.9.3 -U --no-cache-dir")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # INSTALL SCIPY
            logging.info('Installing scipy')
            (stdin, stdout, stderr) = ssh.exec_command("export CXX=/usr/bin/g++;pip install scipy==0.18.0 -U --no-cache-dir")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # INSTALL SCIKIT-LEARN
            logging.info('Installing scikit-learn')
            (stdin, stdout, stderr) = ssh.exec_command("pip install scikit-learn==0.17.1 -U --no-cache-dir")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # INSTALL nltk
            logging.info('Installing nltk')
            (stdin, stdout, stderr) = ssh.exec_command("pip install nltk==3.1 -U --no-cache-dir")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # INSTALL GENSIM
            logging.info('Installing gensim')
            (stdin, stdout, stderr) = ssh.exec_command("cp /usr/lib64/python2.6/lib-dynload/bz2.so /usr/local/greenplum-db-4.3.9.1/ext/python/lib/python2.6/lib-dynload/bz2.so")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            (stdin, stdout, stderr) = ssh.exec_command("pip install gensim -U --no-cache-dir --no-dependencies")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())


            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('installDSPackages Completed on: ' + str(clusterNode["nodeName"]))
###

def setPaths(clusterNode):
    logging.info('setPaths Started on: ' + str(clusterNode["nodeName"]))
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + str(clusterNode["nodeName"]))
            logging.debug('SSH IP: ' + clusterNode["externalIP"] +
                          ' User: gpadmin')
            ssh.connect(clusterNode["externalIP"], 22, "gpadmin", str(os.environ["GPADMIN_PW"]), timeout=120)
            logging.info('Setting up bashrc')
            (stdin, stdout, stderr) = ssh.exec_command(
                "echo 'source /usr/local/greenplum-db/greenplum_path.sh\n' >> ~/.bashrc")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            if 'yes' in os.environ["RAID0"]:
                logging.info('Set MASTER_DATA_DIRECTORY to /data1/master/gpseg-1')
                (stdin, stdout, stderr) = ssh.exec_command(
                    "echo 'export MASTER_DATA_DIRECTORY=/data1/master/gpseg-1\n' >> ~/.bashrc")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
            else:
                logging.info('Set MASTER_DATA_DIRECTORY to /data/disk1/master/gpseg-1')
                (stdin, stdout, stderr) = ssh.exec_command(
                    "echo 'export MASTER_DATA_DIRECTORY=/data/disk1/master/gpseg-1\n' >> ~/.bashrc")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('setPaths Completed on: ' + str(clusterNode["nodeName"]))


def cleanUp(clusterDictionary):
    print "cleanUp"


def makeDirectories(clusterNode):
    logging.info('makeDirectories Started on: ' + str(clusterNode["nodeName"]))
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + clusterNode["nodeName"])
            logging.debug('SSH IP: ' + clusterNode["externalIP"] + ' User: ' +
                          os.environ["SSH_USERNAME"] + ' Key: ' +
                          str(os.environ["CONFIGS_PATH"]) +
                          str(os.environ["SSH_KEY"]))
            ssh.connect(clusterNode["externalIP"], 22, str(os.environ["SSH_USERNAME"]), None, pkey=None,
                        key_filename=str(os.environ["CONFIGS_PATH"]) + str(os.environ["SSH_KEY"]),
                        timeout=120)
            if "master" in clusterNode["role"]:
                if 'yes' in os.environ["RAID0"]:
                    logging.info('Making /data1/master')
                    (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data1/master")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                    logging.info('Settings permissions for gpadmin on /data1/*')
                    (stdin, stdout, stderr) = ssh.exec_command("sudo chown -R gpadmin: /data1")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                else:
                    logging.info('Making /data/disk1/master as we have no RAID0 set')
                    (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/disk1/master")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                    logging.info('Settings permissions for gpadmin on /data/disk1/*')
                    (stdin, stdout, stderr) = ssh.exec_command("sudo chown -R gpadmin: /data/disk1/")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
            else:
                numDisks = os.environ["DISK_QTY"]
                logging.debug('numDisks: ' + str(numDisks))
                segDBs = os.environ["SEGMENTDBS"]
                logging.debug('segmentdbs: ' + str(segDBs))
                if 'yes' in os.environ["RAID0"]:
                    logging.info('Making primary and mirror top level dirs and setting permissions with RAID0')
                    for diskNum in range(1, int(numDisks) + 1):
                        (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data1/primary")
                        logging.debug(stdout.readlines())
                        logging.debug(stderr.readlines())
                        (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data1/mirror")
                        logging.debug(stdout.readlines())
                        logging.debug(stderr.readlines())
                    (stdin, stdout, stderr) = ssh.exec_command("sudo chown -R gpadmin: /data1")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                else:
                    logging.info('Making primary and mirror top level dirs and setting permissions with no RAID0')
                    for diskNum in range(1, int(numDisks) + 1):
                        (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/disk"+str(diskNum)+"/primary")
                        logging.debug(stdout.readlines())
                        logging.debug(stderr.readlines())
                        (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/disk"+str(diskNum)+"/mirror")
                        logging.debug(stdout.readlines())
                        logging.debug(stderr.readlines())
                    (stdin, stdout, stderr) = ssh.exec_command("sudo chown -R gpadmin: /data")
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())

            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('makeDirectories Completed on: ' + str(clusterNode["nodeName"]))


def setGPADMINPW(masterNode):
    logging.info('setGPADMINPW Started on: ' + str(masterNode["nodeName"]))
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + str(masterNode["nodeName"]))
            logging.debug('SSH IP: ' + masterNode["externalIP"] +
                          ' User: gpadmin')
            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ["GPADMIN_PW"]), timeout=120)
            logging.info('Setting gpadmin password in DB')
            (stdin, stdout, stderr) = ssh.exec_command(
                "psql -c \"alter user gpadmin with password '" + str(os.environ["GPADMIN_PW"]) + "';\"")

            # (stdin, stdout, stderr) = ssh.exec_command("alter user gpadmin with password '"+str(os.environ.get("GPADMIN_PW"))+ "';")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            connected = True
        except Exception as e:
            print e
            print traceback.print_exc()
            print masterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('setGPADMINPW Started on: ' + str(masterNode["nodeName"]))


def modifyPHGBA(masterNode):
    logging.info('modifyPHGBA Started on: ' + str(masterNode["nodeName"]))
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + str(masterNode["nodeName"]))
            logging.debug('SSH IP: ' + masterNode["externalIP"] +
                          ' User: gpadmin')
            ssh.connect(masterNode["externalIP"], 22, "gpadmin", str(os.environ["GPADMIN_PW"]), timeout=120)
            #logging.info('allowing access')
            #(stdin, stdout, stderr) = ssh.exec_command(
             #   "echo 'host all gpadmin " + accessNode['internalIP'] + "/0 md5' >> /data/master/gpseg-1/pg_hba.conf")
            #logging.debug(stdout.readlines())
            #logging.debug(stderr.readlines())
            logging.info('Restarting DB')
            (stdin, stdout, stderr) = ssh.exec_command("gpstop -a -r")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            connected = True
        except Exception as e:
            print masterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('modifyPHGBA Completed on: ' + str(masterNode["nodeName"]))


def uncompressFiles(clusterNode, downloads):
    logging.info('uncompressFiles Started on: ' + str(clusterNode["nodeName"]))
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + clusterNode["nodeName"])
            logging.debug('SSH IP: ' + clusterNode["externalIP"] + ' User: ' +
                          os.environ["SSH_USERNAME"] + ' Key: ' +
                          str(os.environ["CONFIGS_PATH"]) +
                          str(os.environ["SSH_KEY"]))
            ssh.connect(clusterNode["externalIP"], 22, str(os.environ["SSH_USERNAME"]), None, pkey=None,
                        key_filename=str(os.environ["CONFIGS_PATH"]) + str(os.environ["SSH_KEY"]), timeout=120)
            logging.info('Unzipping files')
            for file in downloads:
                if ".zip" in file["NAME"]:
                    (stdin, stdout, stderr) = ssh.exec_command("cd /tmp;unzip ./" + file["NAME"])
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                elif ".gz" in file["NAME"]:
                    (stdin, stdout, stderr) = ssh.exec_command("cd /tmp;tar xvfz ./" + file["NAME"])
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())

            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('uncompressFiles Completed on: ' + str(clusterNode["nodeName"]))


def prepFiles(clusterNode):
    logging.info('prepFiles Started on: ' + str(clusterNode["nodeName"]))
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + clusterNode["nodeName"])
            logging.debug('SSH IP: ' + clusterNode["externalIP"] + ' User: ' +
                          os.environ["SSH_USERNAME"] + ' Key: ' +
                          str(os.environ["CONFIGS_PATH"]) +
                          str(os.environ["SSH_KEY"]))
            ssh.connect(clusterNode["externalIP"], 22, str(os.environ["SSH_USERNAME"]), None, pkey=None,
                        key_filename=str(os.environ["CONFIGS_PATH"]) + str(os.environ["SSH_KEY"]), timeout=120)
            logging.info('Preparing GPDB Install Binary')
            (stdin, stdout, stderr) = ssh.exec_command("sudo sed -i 's/more <</cat <</g' /tmp/greenplum-db*.bin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            (stdin, stdout, stderr) = ssh.exec_command("sudo sed -i 's/agreed=/agreed=1/' /tmp/greenplum-db*.bin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            (stdin, stdout, stderr) = ssh.exec_command(
                "sudo sed -i 's/pathVerification=/pathVerification=1/' /tmp/greenplum-db*.bin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            (stdin, stdout, stderr) = ssh.exec_command(
                "sudo sed -i 's/user_specified_installPath=/user_specified_installPath=${installPath}/' /tmp/greenplum-db*.bin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('prepFiles Completed on: ' + str(clusterNode["nodeName"]))


def installBits(clusterNode):
    logging.info('installBits Started on: ' + str(clusterNode["nodeName"]))
    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1

            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + clusterNode["nodeName"])
            logging.debug('SSH IP: ' + clusterNode["externalIP"] + ' User: ' +
                          os.environ["SSH_USERNAME"] + ' Key: ' +
                          str(os.environ["CONFIGS_PATH"]) +
                          str(os.environ["SSH_KEY"]))
            ssh.connect(clusterNode["externalIP"], 22, str(os.environ["SSH_USERNAME"]), None, pkey=None,
                        key_filename=str(os.environ["CONFIGS_PATH"]) + str(os.environ["SSH_KEY"]),
                        timeout=120)
            logging.info('Installing GPDB')
            (stdin, stdout, stderr) = ssh.exec_command("sudo /tmp/greenplum-db*.bin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            (stdin, stdout, stderr) = ssh.exec_command("sudo chown -R gpadmin: /usr/local/greenplum-db*")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # Go ahead and change owner on Data Disk(s).   Only One now, but change this if more disks are added.
            # (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p /data/master;sudo chown -R gpadmin: /data")
            # I believe these need to be commented as the command got commented
            # stdout.readlines()
            # stderr.readlines()

            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.info('installBits Completed on: ' + str(clusterNode["nodeName"]))


def initDB(clusterNode, clusterName):
    logging.info('initDB Started on: ' + str(clusterNode["nodeName"]))
    # Read the template, modify it, write it to the cluster directory and the master

    ## BUILD DATA DIRECTORY AND MIRROR DIRECTORY
    # SHOULD BE #SEGDB ENTRIES ACROSS # DRIVES.
    logging.debug('CAPE_HOME: ' + str(os.environ["CAPE_HOME"]))
    logging.debug('Current Dir: ' + os.getcwd())
    numDisks = os.environ["DISK_QTY"]
    logging.debug('numDisks: ' + str(numDisks))
    segDBs = os.environ["SEGMENTDBS"]
    logging.debug('segDBs: ' + str(segDBs))
    diskBase = os.environ["BASE_HOME"]
    logging.debug('diskBase: ' + diskBase)
    dataDirectories = ""
    mirrorDirectories = ""

    if int(numDisks) > 1:
        # Spread Primaries and mirrors across all drives
        segDBDirs = (int(segDBs)/2)
        logging.debug('segDBDirs: '+ str(segDBDirs))
    else:
        # We only have one drive so no spreading of primaries and mirrors
        segDBDirs = int(segDBs)
        logging.debug('segDBDirs: ' + str(segDBDirs))

    for diskNum in range(1, int(numDisks)+1):
        for segNum in range(1, int(segDBDirs)+1):
            if 'yes' in os.environ["RAID0"]:
                dataDirectories = dataDirectories + "/data1/primary "
                mirrorDirectories = mirrorDirectories + "/data1/mirror "
            else:
                dataDirectories = dataDirectories + diskBase+"/disk" +\
                    str(diskNum) + "/primary "
                mirrorDirectories = mirrorDirectories + diskBase+"/disk" +\
                    str(diskNum)+"/mirror "
    logging.debug('dataDirectories: ' + dataDirectories)
    logging.debug('mirrorDirectories: ' + mirrorDirectories)

    with open(str(os.environ["CAPE_HOME"])+"/templates/gpinitsystem_config.template", 'r+') as gpConfigTemplate:
        gpConfigTemplateData = gpConfigTemplate.read()
        gpConfigTemplateModData = gpConfigTemplateData.replace("%MASTER%", clusterNode["nodeName"])
        gpConfigDirectoriesData = gpConfigTemplateModData.replace("declare -a DATA_DIRECTORY=(/data/primary /data/primary)","declare -a DATA_DIRECTORY=("+dataDirectories+")")
        gpConfigTemplateData  = gpConfigDirectoriesData.replace("declare -a MIRROR_DATA_DIRECTORY=(/data/mirror /data/mirror)","declare -a MIRROR_DATA_DIRECTORY=("+mirrorDirectories+")")


    with open(os.environ["CAPE_HOME"] + "/clusterConfigs/" + str(clusterName) + "/gpinitsystem_config",
              'w') as gpConfigCluster:
        gpConfigCluster.write(gpConfigTemplateData)
    logging.debug('Wrote File: ' + str(os.environ["CAPE_HOME"]) + "/clusterConfigs/" + str(clusterName) + "/gpinitsystem_config")

    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + clusterNode["nodeName"])
            logging.debug('SSH IP: ' + clusterNode["externalIP"] + ' User: ' +
                          os.environ["SSH_USERNAME"] + ' Key: ' +
                          str(os.environ["CONFIGS_PATH"]) +
                          str(os.environ["SSH_KEY"]))
            ssh.connect(clusterNode["externalIP"], 22, os.environ["SSH_USERNAME"], None, pkey=None,
                        key_filename=str(os.environ["CONFIGS_PATH"]) + str(os.environ["SSH_KEY"]),
                        timeout=120)
            logging.info('Uploading gpinitsystem_config.cape file')
            sftp = ssh.open_sftp()
            sftp.put("gpinitsystem_config", "/tmp/gpinitsystem_config.cape", confirm=True)

            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
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
            logging.debug('Connecting to Node: ' + str(clusterNode["nodeName"]))
            logging.debug('SSH IP: ' + clusterNode["externalIP"] +
                          ' User: gpadmin')
            ssh.connect(clusterNode["externalIP"], 22, "gpadmin", str(os.environ["GPADMIN_PW"]), timeout=120)
            #
            # Adding gpssh-exkeys here for now
            # We need to figure out the key exchange and then remove this step
            #
            # (stdin, stdout, stderr) = ssh.exec_command(
            #     "source /usr/local/greenplum-db/greenplum_path.sh;gpssh-exkeys -f /tmp/workers")
            # stdout.readlines()
            # stderr.readlines()
            logging.info('Starting DB init')
            (stdin, stdout, stderr) = ssh.exec_command(
                "source /usr/local/greenplum-db/greenplum_path.sh;gpinitsystem -c /tmp/gpinitsystem_config.cape -a")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            connected = True
        except Exception as e:
            print clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 1:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing InitDB Process"
                exit()
        finally:
            ssh.close()
            logging.info('initDB Completed on: ' + str(clusterNode["nodeName"]))
