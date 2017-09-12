import os
import sys
import threading
import time
import warnings
import traceback
import paramiko
import logging
import json
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from paramiko import WarningPolicy
from paramiko import AutoAddPolicy


def buildServers(clusterDictionary):
    logging.debug('buildServers Started')
    warnings.simplefilter("ignore")
    logging.debug('SimpleFilter for Warnings: ignore')
    print clusterDictionary["clusterName"] + ": Cluster Creation Started"
    logging.info(clusterDictionary["clusterName"] +
                 ": Cluster Creation Started")

    # ADD ACTUAL CHECKING FOR EXISTING CLUSTER....THIS WAS JUST FOR TEST

    try:
        if not os.path.exists(str(os.environ["CAPE_HOME"]) +
                              "/clusterConfigs/" +
                              clusterDictionary["clusterName"]):
            os.makedirs(str(os.environ["CAPE_HOME"]) +
                        "/clusterConfigs/" + clusterDictionary["clusterName"])
            logging.debug("Created: /clusterConfigs/" +
                          clusterDictionary["clusterName"])

    except OSError:
        if "test" in clusterDictionary["clusterName"]:
            print "Testing Mode"
            timestamp = str(time.time()).split('.')[0]
            clusterDictionary["clusterName"] = clusterDictionary["clusterName"] + timestamp
            os.makedirs("./clusterConfigs/" + clusterDictionary["clusterName"])
        else:
            print "Cluster Name already exists."
            logging.error("Cluster Name already exists. Exiting!")
    try:
        clusterPath = str(os.environ["CAPE_HOME"]) + "/clusterConfigs/" + clusterDictionary["clusterName"]
        logging.debug("ClusterPath: " + str(clusterPath))

        clusterNodes = []
        logging.debug('Will create ' + str(clusterDictionary["nodeQty"]) +
                      ' Nodes with Info below:')
        logging.debug('SVC_ACCOUNT: ' + str(os.environ["SVC_ACCOUNT"]))
        logging.debug('CONFIGS_PATH: '+ str(os.environ["CONFIGS_PATH"]))
        logging.debug('SVC_ACCOUNT_KEY: ' + str(os.environ["SVC_ACCOUNT_KEY"]))
        logging.debug('PROJECT: ' + str(os.environ["PROJECT"]))
        logging.debug('DATACENTER: ' + str(os.environ["ZONE"]))
        logging.debug('DISK_TYPE: ' + str(os.environ["DISK_TYPE"]))
        logging.debug('SERVER_TYPE: ' + str(os.environ["SERVER_TYPE"]))

        ComputeEngine = get_driver(Provider.GCE)
        driver = ComputeEngine(os.environ["SVC_ACCOUNT"], str(os.environ["CONFIGS_PATH"]) + str(os.environ["SVC_ACCOUNT_KEY"]), project=str(os.environ["PROJECT"]), datacenter=str(os.environ["ZONE"]))
        gce_disk_struct = [
            {
                "kind": "compute#attachedDisk",
                "boot": True,
                "autoDelete": True,

                'initializeParams': {
                    "sourceImage": "/projects/centos-cloud/global/images/" + str(os.environ["IMAGE"]),
                    "diskSizeGb": 100,
                    "diskStorageType": str(os.environ["DISK_TYPE"]),
                    "diskType": "/compute/v1/projects/" + str(os.environ["PROJECT"]) + "/zones/" + str(
                        os.environ["ZONE"]) + "/diskTypes/" + str(os.environ["DISK_TYPE"])
                },
            }

        ]
        sa_scopes = [{'scopes': ['compute', 'storage-full']}]
        print clusterDictionary["clusterName"] + ": Creating " + str(clusterDictionary["nodeQty"]) + " Nodes"
        nodes = driver.ex_create_multiple_nodes(base_name=clusterDictionary["clusterName"],
                                                size=str(os.environ["SERVER_TYPE"]), image=None,
                                                number=int(clusterDictionary["nodeQty"]),
                                                location=str(os.environ["ZONE"]),
                                                ex_network='default', ex_tags=None, ex_metadata=None, ignore_errors=True,
                                                use_existing_disk=False, poll_interval=2, external_ip='ephemeral',
                                                ex_service_accounts=sa_scopes, timeout=180, description=None,
                                                ex_can_ip_forward=None, ex_disks_gce_struct=gce_disk_struct,
                                                ex_nic_gce_struct=None, ex_on_host_maintenance=None,
                                                ex_automatic_restart=None)

        print clusterDictionary["clusterName"] + ": Cluster Nodes Created in Google Cloud"
        logging.info(clusterDictionary["clusterName"] + ": Cluster Nodes Created in Google Cloud")
        print clusterDictionary["clusterName"] + ": Cluster Configuration Started"
        logging.info(clusterDictionary["clusterName"] + ": Cluster Configuration Started")

        threads = []
        buildFSTAB(clusterDictionary, int(os.environ["DISK_QTY"]))
        for nodeCnt in range(int(clusterDictionary["nodeQty"])):
            nodeName = clusterDictionary["clusterName"] + "-" + str(nodeCnt).zfill(3)
            clusterNode = {}

        #### THIS SECTION CAN BE MODIFIED TO TAKE A VARIABLE AND MOUNT MULTIPLE DISKS INSTEAD OF 1
        ####  MOUNTS SHOULD GO UNDER /DATA AND BE DATA1,DATA2,DATAN
        ####  THIS MEANS THE 1 DISK USE CASE SHOULD BE MOUNTED THE SAME WAY.
            for diskNum in range(1,int(os.environ["DISK_QTY"])+1):
                volume = driver.create_volume(os.environ["DISK_SIZE"], nodeName + "-data-disk-"+str(diskNum), None, None,
                                              None, False, "pd-standard")

                clusterNode["nodeName"] = nodeName
                clusterNode["dataVolume"] = str(volume)

                node = driver.ex_get_node(nodeName)
                driver.attach_volume(node, volume, device=None, ex_mode=None, ex_boot=False, ex_type=None, ex_source=None,
                                     ex_auto_delete=True, ex_initialize_params=None, ex_licenses=None, ex_interface=None)
            clusterNode["externalIP"] = str(node).split(",")[3].split("'")[1]
            clusterNode["internalIP"] = str(node).split(",")[4].split("'")[1]
            print "     " + nodeName + ": External IP: " + clusterNode["externalIP"]
            print "     " + nodeName + ": Internal IP: " + clusterNode["internalIP"]
            if is_master(nodeName):
                with open('/tmp/gpdb-master-host-ip.txt', 'w') as fh:
                    fh.write(clusterNode["externalIP"])
            logging.debug('Created Node: ' + str(clusterNode))

            prepThread = threading.Thread(target=prepServer, args=(clusterDictionary,clusterNode, nodeCnt))
            clusterNodes.append(clusterNode)
            threads.append(prepThread)
            prepThread.start()
        for x in threads:
            x.join()
        print clusterDictionary["clusterName"] + ": Cluster Configuration Complete"
        logging.info(clusterDictionary["clusterName"] + ": Cluster Configuration Complete")
        clusterDictionary["clusterNodes"] = clusterNodes
        logging.debug('ClusterNodes: ' + json.dumps(clusterDictionary["clusterNodes"]))
        getNodeFQDN(clusterDictionary)
        logging.debug(json.dumps(clusterDictionary))
        hostsFiles(clusterDictionary)
        keyShare(clusterDictionary)
        logging.debug('buildServers Completed')
    except Exception as e:
        logging.debug('Exception: ' + str(e.__class__))
        logging.debug('Exception: ' + str(e))
        # look at how we can incorporate this into logging exceptions
        # exc_type, exc_value, exc_traceback = sys.exc_info()
        # logging.debug(traceback.print_exception(exc_type, exc_value,
        #              exc_traceback))
        logging.debug(traceback.print_exc())
        logging.debug('Failed')
        print "Failing Process"
        sys.exit('\n\nBuildServers Failed')

def is_master(nodeName):
    """ ghetto, i know"""
    return nodeName.endswith('-000')


def buildFSTAB(clusterDictionary,diskCNT):
    logging.debug('buildFSTAB Started with ' + str(diskCNT) + ' Drives')
    clusterPath = str(os.environ["CAPE_HOME"]) + "/clusterConfigs/" + clusterDictionary["clusterName"]
    currentPath = os.getcwd()
    logging.debug('Current Dir: ' + currentPath)
    os.chdir(clusterPath)
    logging.debug('Changed Dir to: ' + clusterPath)
    with open("fstab.cape", "w") as fstabFile:
        fstabFile.write("######  CAPE ENTRIES #######\n")
        fstabFile.write("/swapfile    swap     swap    defaults     0 0\n")
        if os.environ["RAID0"] == "no":
            logging.info('Configuring with no RAID as RAID0=' + str(os.environ["RAID0"]))
            for disk in range(1,diskCNT+1):
                fstabFile.write("LABEL=data"+str(disk)+ "   /data/disk"+str(disk) + "   xfs rw,noatime,nodiratime,nobarrier,inode64,allocsize=16m 0 0\n")
        else:
            logging.info('Configuring with RAID0 as RAID0=' + str(os.environ["RAID0"]))
            if diskCNT < 8:
                logging.debug('Creating fstab for 1 volume')
                fstabFile.write("/dev/md1" + " /data1" + "   xfs rw,noatime,nodiratime,nobarrier,inode64,allocsize=16m 0 0\n")
            else:
                logging.debug('Creating fstab for 2 volumes')
                fstabFile.write("/dev/md1" + " /data1" + "   xfs rw,noatime,nodiratime,nobarrier,inode64,allocsize=16m 0 0\n")
                fstabFile.write("/dev/md2" + " /data2" + "   xfs rw,noatime,nodiratime,nobarrier,inode64,allocsize=16m 0 0\n")
    logging.info('Wrote fstab file')
    os.chdir(currentPath)
    logging.debug('Changed Dir to: ' + currentPath)
    logging.debug('buildFSTAB Completed')




def prepServer(clusterDictionary,clusterNode, nodeCnt):
    logging.debug('prepServer Started on '+clusterNode["nodeName"])
    warnings.simplefilter("ignore")
    logging.debug('SimpleFilter for Warnings: ignore')
    paramiko.util.log_to_file("/tmp/paramiko.log")
    nodeName = clusterNode["nodeName"]

    # Set Server Role
    if os.environ["STANDBY"] == "yes" and os.environ["ACCESS"] == "yes":
        logging.debug('STANDBY and ACCESS are yes')
        if (nodeCnt) == 0:
            clusterNode["role"] = "access"
            clusterDictionary["accessCount"] += 1
        elif (nodeCnt) == 1:
            clusterNode["role"] = "master1"
            clusterDictionary["masterCount"] += 1
        elif (nodeCnt) == 2:
            clusterNode["role"] = "master2"
            clusterDictionary["masterCount"] += 1
        else:
            clusterNode["role"] = "worker"
            clusterDictionary["segmentCount"] += 1
    elif os.environ["STANDBY"] == "no" and os.environ["ACCESS"] == "no":
        logging.debug('STANDBY and ACCESS are no')
        if (nodeCnt) == 0:
            clusterNode["role"] = "master1"
            clusterDictionary["masterCount"] += 1
        else:
            clusterNode["role"] = "worker"
            clusterDictionary["segmentCount"] += 1
    logging.debug('Role set')

    connected = False
    attemptCount = 0
    while not connected:
        try:
            logging.debug('Current Dir: ' + str(os.getcwd()))
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Connecting to Node: ' + clusterNode["nodeName"])
            logging.debug('SSH IP: ' + clusterNode["externalIP"] + ' User: ' +
                          os.environ["SSH_USERNAME"] + ' Key: ' +
                          str(os.environ["CONFIGS_PATH"]) +
                          str(os.environ["SSH_KEY"]))
            ssh.connect(clusterNode["externalIP"], 22, os.environ["SSH_USERNAME"], None, pkey=None,
                        key_filename=(str(os.environ["CONFIGS_PATH"]) + str(os.environ["SSH_KEY"])), timeout=120)
            sftp = ssh.open_sftp()
            sftp.put('./templates/sysctl.conf.cape', '/tmp/sysctl.conf.cape', confirm=True)
            logging.debug('Put: ./templates/sysctl.conf.cape')
            sftp.put('./clusterConfigs/' + clusterDictionary["clusterName"]+ '/fstab.cape', '/tmp/fstab.cape', confirm=True)
            logging.debug('Put: ./clusterConfigs/' +
                          clusterDictionary["clusterName"] + '/fstab.cape')
            sftp.put('./templates/limits.conf.cape', '/tmp/limits.conf.cape', confirm=True)
            logging.debug('Put: ./templates/limits.conf.cape')
            sftp.put('./scripts/prepareHost.sh', '/tmp/prepareHost.sh', confirm=True)
            logging.debug('Put: ./scripts/prepareHost.sh')

            time.sleep(10)

            logging.debug('Creating user root')
            (stdin, stdout, stderr) = ssh.exec_command("sudo echo " + os.environ["ROOT_PW"] + " | sudo passwd --stdin root")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            logging.debug('Making prepareHost executable and running it')
            ssh.exec_command("sudo chmod +x /tmp/prepareHost.sh")
            (stdin, stdout, stderr) = ssh.exec_command("/tmp/prepareHost.sh " + str(os.environ["DISK_QTY"]) + " " + str(os.environ["RAID0"]) +" &> /tmp/prepareHost.log")
            logging.debug('Starting prepareHost script on ' + clusterNode["nodeName"])
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            homeDir = os.environ["BASE_HOME"] + "/home"
            logging.debug('Adding user gpadmin with homdir path: ' + homeDir)
            (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p " + homeDir + ";sudo useradd -b " + homeDir + " -s " + "/bin/bash -m gpadmin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            logging.debug('Setting gpadmin password')
            (stdin, stdout, stderr) = ssh.exec_command("sudo echo " + os.environ["GPADMIN_PW"] + " | sudo passwd --stdin gpadmin")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())

            # could change to node.reboot
            print clusterNode["nodeName"]+": Rebooting"
            logging.debug(clusterNode["nodeName"] + ': Rebooting')
            (stdin, stdout, stderr) = ssh.exec_command("sudo reboot")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            connected = True
        except Exception as e:
            print "     " + nodeName + ": Attempting SSH Connection"
            time.sleep(3)

            if attemptCount > 40:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Cluster Creation FAILED')
                print "CLUSTER CREATION FAILED:   Cleanup and Retry"
                exit()
        finally:
            ssh.close()
            logging.debug('prepServer Completed on '+clusterNode["nodeName"])
    return


def hostsFiles(clusterDictionary):
    logging.debug('hostFiles Started')
    clusterPath = str(os.environ["CAPE_HOME"]) + "/clusterConfigs/" + clusterDictionary["clusterName"]
    os.chdir(clusterPath)
    logging.debug('Changed Dir to: ' + os.getcwd())
    with open("hosts", "w") as hostsFile:
        hostsFile.write("######  CAPE ENTRIES #######\n")
        for node in clusterDictionary["clusterNodes"]:
            hostsFile.write(node["internalIP"] + "  " + node["nodeName"] + "  " + node["FQDN"] + "\n")
    logging.debug('Wrote hosts file')

    with open("workers", "w") as workersFile:
        with open("allhosts", "w") as allhostsFile:
            for node in clusterDictionary["clusterNodes"]:
                if "master1" in node["role"]:
                    allhostsFile.write(node["nodeName"] + "\n")
                elif "master2" in node["role"]:
                    allhostsFile.write(node["nodeName"] + "\n")
                elif "access" in node["role"]:
                    allhostsFile.write(node["nodeName"] + "\n")
                else:
                    workersFile.write(node["nodeName"] + "\n")
                    allhostsFile.write(node["nodeName"] + "\n")
    logging.debug('Wrote allhosts and workers file')
    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        logging.debug('Starting uploadThread for: ' + str(clusterNode["nodeName"]))
        uploadThread = threading.Thread(target=hostFileUpload, args=(clusterNode,))
        threads.append(uploadThread)
        uploadThread.start()
    for x in threads:
        x.join()
    logging.debug('hostFiles Completed')


def verifyCluster(clusterDictionary):
    print "Verifying Cluster"
    # Check /etc/hosts
    # SSH to each host and ping 000
    # check df -k for Data Drive


def keyShare(clusterDictionary):
    # NEED TO THREAD THE KEY SHARE TAKES WAY TOO LONG
    logging.debug('keyShare Started')
    warnings.simplefilter("ignore")
    logging.debug('SimpleFilter for Warnings: ignore')
    paramiko.util.log_to_file("/tmp/paramiko.log")


    for node in clusterDictionary["clusterNodes"]:
        logging.debug("Working on Node: " + str(node["nodeName"]))

        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(AutoAddPolicy())
                logging.debug('Connecting to Node: ' + str(node["nodeName"]))
                logging.debug('SSH IP: ' + node["externalIP"] +
                              ' User: gpadmin')
                ssh.connect(node["externalIP"], 22, "gpadmin", password=str(os.environ["GPADMIN_PW"]), timeout=120)
                logging.debug('Generating id_rsa')
                (stdin, stdout, stderr) = ssh.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
                logging.debug(stdout.readlines())
                logging.debug(stderr.readlines())
                logging.debug('Configure SSH settings')
                (stdin, stdout, stderr) = ssh.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
                logging.debug(stdout.readlines())
                logging.debug(stderr.readlines())
                for node1 in clusterDictionary["clusterNodes"]:
                    # Explicitly writing exit so ssh session does not hang
                    logging.debug("exchange key ssh from " + str(node["nodeName"]) + " to " + str(node1["nodeName"]) + " using internal IP")
                    (stdin, stdout, stderr) = ssh.exec_command("sshpass -p " + os.environ["GPADMIN_PW"] + "  ssh gpadmin@" + node1["internalIP"]+ " -o StrictHostKeyChecking=no")
                    stdin.write('exit \n')
                    stdin.flush()
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                    logging.debug("exchange key ssh from " + str(node["nodeName"]) + " to " + str(node1["nodeName"]) + " using FQDN")
                    (stdin, stdout, stderr) = ssh.exec_command("sshpass -p " + os.environ["GPADMIN_PW"] + "  ssh gpadmin@" + node1["FQDN"]+ " -o StrictHostKeyChecking=no")
                    stdin.write('exit \n')
                    stdin.flush()
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                    logging.debug("exchange key ssh-copy-id from " + str(node["nodeName"]) + " to " + str(node1["nodeName"]))
                    (stdin, stdout, stderr) = ssh.exec_command("sshpass -p " + os.environ["GPADMIN_PW"] + "  ssh-copy-id  gpadmin@" + node1["nodeName"])
                    stdin.write('exit \n')
                    stdin.flush()
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())

                ssh.close()
                logging.debug('Connecting to Node: ' + str(node["nodeName"]))
                logging.debug('SSH IP: ' + node["externalIP"] +
                              ' User: root')
                ssh.connect(node["externalIP"], 22, "root", password=str(os.environ["ROOT_PW"]),
                            timeout=120)
                logging.debug('Generating id_rsa')
                (stdin, stdout, stderr) = ssh.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
                logging.debug(stdout.readlines())
                logging.debug(stderr.readlines())
                logging.debug('Configure SSH settings')
                ssh.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
                for node1 in clusterDictionary["clusterNodes"]:
                    # explicitly writing exit to stdin so ssh session does not hang
                    logging.debug("exchange key ssh from " + str(node["nodeName"]) + " to " + str(node1["nodeName"]) + " using internal IP")
                    (stdin, stdout, stderr) = ssh.exec_command("sshpass -p " + os.environ["ROOT_PW"] + "  ssh root@" + node1["internalIP"]+ " -o StrictHostKeyChecking=no" )
                    stdin.write('exit \n')
                    stdin.flush()
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                    logging.debug("exchange key ssh from " + str(node["nodeName"]) + " to " + str(node1["nodeName"]) + " using FQDN")
                    (stdin, stdout, stderr) = ssh.exec_command("sshpass -p " + os.environ["ROOT_PW"] + "  ssh root@" + node1["FQDN"]+ " -o StrictHostKeyChecking=no" )
                    stdin.write('exit \n')
                    stdin.flush()
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())
                    logging.debug("exchange key ssh-copy-id from " + str(node["nodeName"]) + " to " + str(node1["nodeName"]))
                    (stdin, stdout, stderr) = ssh.exec_command(
                        "sshpass -p " + os.environ["ROOT_PW"] + "  ssh-copy-id  root@" + node1[
                            "nodeName"])
                    stdin.write('exit \n')
                    stdin.flush()
                    logging.debug(stdout.readlines())
                    logging.debug(stderr.readlines())

                connected = True
            except Exception as e:
                print e
                print traceback.print_exc()
                print node["nodeName"] + ": Attempting SSH Connection"
                time.sleep(3)
                print ("attempt Count: " + str(attemptCount) + "/40")
                if attemptCount > 40:
                    logging.debug('Exception: ' + str(e))
                    logging.debug(traceback.print_exc())
                    logging.debug('Failed')
                    print "Failing Process"
                    exit()
            finally:
                ssh.close()
                logging.debug('keyShare Completed')


def hostFileUpload(clusterNode):
    logging.debug('hostFileUpload Started')
    warnings.simplefilter("ignore")
    logging.debug('SimpleFilter for Warnings: ignore')
    paramiko.util.log_to_file("/tmp/paramiko.log")

    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            logging.debug('Current Dir: ' + os.getcwd())
            logging.debug('Connecting to Node: ' + clusterNode["nodeName"])
            logging.debug('SSH IP: ' + clusterNode["externalIP"] + ' User: ' +
                          os.environ["SSH_USERNAME"] + ' Key: ' +
                          str(os.environ["CONFIGS_PATH"]) +
                          str(os.environ["SSH_KEY"]))
            ssh.connect(clusterNode["externalIP"], 22, os.environ["SSH_USERNAME"], None, pkey=None,
                        key_filename=(str(os.environ["CONFIGS_PATH"]) + str(os.environ["SSH_KEY"])), timeout=120)

            sftp = ssh.open_sftp()
            sftp.put("hosts", "/tmp/hosts", confirm=True)
            logging.debug('Put hosts file')
            sftp.put("allhosts", "/tmp/allhosts", confirm=True)
            logging.debug('Put allhosts file')
            sftp.put("workers", "/tmp/workers", confirm=True)
            logging.debug('Put Workers File')

            (stdin, stdout, stderr) = ssh.exec_command("sudo sh -c 'cat /tmp/hosts >> /etc/hosts'")
            logging.debug(stdout.readlines())
            logging.debug(stderr.readlines())
            connected = True
        except Exception as e:
            # print e
            print "     " + clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 40:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.debug('hostFileUpload Completed')


def getNodeFQDN(clusterDictionary):
    logging.debug('getNodeFQDN Started')
    warnings.simplefilter("ignore")
    logging.debug('SimpleFilter for Warnings: ignore')
    paramiko.util.log_to_file("/tmp/paramiko.log")

    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            for node in clusterDictionary["clusterNodes"]:
                logging.debug('Connecting to ' + node["nodeName"])
                logging.debug('SSH IP: ' + node["externalIP"] +
                              ' User: gpadmin')
                ssh.connect(node["externalIP"], 22, "gpadmin", password=str(os.environ["GPADMIN_PW"]), timeout=120)
                (stdin, stdout, stderr) = ssh.exec_command("hostname -f ")
                fqdn = stdout.read()
                logging.debug(stdout.readlines())
                logging.debug(stderr.readlines())
                node["FQDN"] = fqdn.strip()
                logging.debug('FQDN set: ' + str(node["FQDN"]))
            connected = True
        except Exception as e:
            # print e
            # print traceback.print_exc()
            print "     " + node["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 40:
                logging.debug('Exception: ' + str(e))
                logging.debug(traceback.print_exc())
                logging.debug('Failed')
                print "Failing Process"
                exit()
        finally:
            ssh.close()
            logging.debug('getNodeFQDN Completed')
