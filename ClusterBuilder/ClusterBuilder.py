import os
import threading
import time
import warnings

import paramiko
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from paramiko import WarningPolicy


def buildServers(clusterDictionary):
    warnings.simplefilter("ignore")
    print clusterDictionary["clusterName"] + ": Cluster Creation Started"

    # ADD ACTUAL CHECKING FOR EXISTING CLUSTER....THIS WAS JUST FOR TEST

    try:
        if not os.path.exists(clusterDictionary["clusterName"]):
            os.makedirs("./clusterConfigs/" + clusterDictionary["clusterName"])

    except OSError:
        if "test" in clusterDictionary["clusterName"]:
            print "Testing Mode"
            timestamp = str(time.time()).split('.')[0]
            clusterDictionary["clusterName"] = clusterDictionary["clusterName"] + timestamp
            os.makedirs("./clusterConfigs/" + clusterDictionary["clusterName"])
        else:
            print "Cluster Name already exists."
    clusterPath = "./clusterConfigs/" + clusterDictionary["clusterName"]

    clusterNodes = []

    ComputeEngine = get_driver(Provider.GCE)
    driver = ComputeEngine(os.environ.get("SVC_ACCOUNT"),str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SVC_ACCOUNT_KEY")),project=str(os.environ.get("PROJECT")), datacenter=str(os.environ.get("ZONE")))
    gce_disk_struct = [
        {
            "kind": "compute#attachedDisk",
            "boot": True,
            "autoDelete": True,

            'initializeParams': {
                "sourceImage": "/projects/centos-cloud/global/images/" + str(os.environ.get("IMAGE")),
                "diskSizeGb": 40,
                "diskStorageType": str(os.environ.get("DISK_TYPE")),
                "diskType": "/compute/v1/projects/" + str(os.environ.get("PROJECT")) + "/zones/" + str(
                    os.environ.get("ZONE")) + "/diskTypes/" + str(os.environ.get("DISK_TYPE"))
            },
        }

    ]
    sa_scopes = [{'scopes': ['compute', 'storage-full']}]
    print clusterDictionary["clusterName"] + ": Creating " + str(clusterDictionary["nodeQty"]) + " Nodes"
    nodes = driver.ex_create_multiple_nodes(base_name=clusterDictionary["clusterName"],
                                            size=str(os.environ.get("SERVER_TYPE")), image=None,
                                            number=int(clusterDictionary["nodeQty"]),
                                            location=str(os.environ.get("ZONE")),
                                            ex_network='default', ex_tags=None, ex_metadata=None, ignore_errors=True,
                                            use_existing_disk=False, poll_interval=2, external_ip='ephemeral',
                                            ex_service_accounts=None, timeout=180, description=None,
                                            ex_can_ip_forward=None, ex_disks_gce_struct=gce_disk_struct,
                                            ex_nic_gce_struct=None, ex_on_host_maintenance=None,
                                            ex_automatic_restart=None)

    print clusterDictionary["clusterName"] + ": Cluster Nodes Created in Google Cloud"
    print clusterDictionary["clusterName"] + ": Cluster Configuration Started"

    threads = []
    buildFSTAB(clusterDictionary, int(os.environ.get("DISK_QTY")))
    for nodeCnt in range(int(clusterDictionary["nodeQty"])):
        nodeName = clusterDictionary["clusterName"] + "-" + str(nodeCnt).zfill(3)
        clusterNode = {}

        #### THIS SECTION CAN BE MODIFIED TO TAKE A VARIABLE AND MOUNT MULTIPLE DISKS INSTEAD OF 1
        ####  MOUNTS SHOULD GO UNDER /DATA AND BE DATA1,DATA2,DATAN
        ####  THIS MEANS THE 1 DISK USE CASE SHOULD BE MOUNTED THE SAME WAY.

        for diskNum in range(1,int(os.environ.get("DISK_QTY"))+1):
            volume = driver.create_volume(os.environ.get("DISK_SIZE"), nodeName + "-data-disk-"+str(diskNum), None, None,
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

        prepThread = threading.Thread(target=prepServer, args=(clusterDictionary,clusterNode, nodeCnt))
        clusterNodes.append(clusterNode)
        threads.append(prepThread)
        prepThread.start()
    for x in threads:
        x.join()
    print clusterDictionary["clusterName"] + ": Cluster Configuration Complete"
    clusterDictionary["clusterNodes"] = clusterNodes
    hostsFiles(clusterDictionary)
    keyShare(clusterDictionary)

def buildFSTAB(clusterDictionary,diskCNT):
    clusterPath = "./clusterConfigs/" + clusterDictionary["clusterName"]
    currentPath = os.getcwd()
    os.chdir(clusterPath)
    with open("fstab.cape", "w") as fstabFile:
        fstabFile.write("######  CAPE ENTRIES #######\n")
        for disk in range(1,diskCNT+1):
            fstabFile.write("LABEL=data"+str(disk)+ "   /data/disk"+str(disk) + "   xfs rw,noatime,inode64,allocsize=16m 0 0\n")
    os.chdir(currentPath)




def prepServer(clusterDictionary,clusterNode, nodeCnt):
    warnings.simplefilter("ignore")
    paramiko.util.log_to_file("/tmp/paramiko.log")
    nodeName = clusterNode["nodeName"]

    # Set Server Role
    if os.environ.get("STANDBY") == "yes" and os.environ.get("ACCESS") == "yes":
        if (nodeCnt) == 0:
            clusterNode["role"] = "access"
        elif (nodeCnt) == 1:
            clusterNode["role"] = "master1"
        elif (nodeCnt) == 2:
            clusterNode["role"] = "master2"
        else:
            clusterNode["role"] = "worker"
    elif os.environ.get("STANDBY") == "no" and os.environ.get("ACCESS") == "no":
        if (nodeCnt) == 0:
            clusterNode["role"] = "master1"
        else:
            clusterNode["role"] = "worker"

    connected = False
    attemptCount = 0
    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, os.environ.get("SSH_USERNAME"), None, pkey=None,
                        key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")), timeout=120)
            sftp = ssh.open_sftp()
            sftp.put('./templates/sysctl.conf.cape', '/tmp/sysctl.conf.cape', confirm=True)
            sftp.put('./clusterConfigs/' + clusterDictionary["clusterName"]+ '/fstab.cape', '/tmp/fstab.cape', confirm=True)
            sftp.put('./templates/limits.conf.cape', '/tmp/limits.conf.cape', confirm=True)
            sftp.put('./scripts/prepareHost.sh', '/tmp/prepareHost.sh', confirm=True)

            time.sleep(10)


            (stdin, stdout, stderr) = ssh.exec_command("sudo echo " + os.environ.get("ROOT_PW") + " | sudo passwd --stdin root")
            stdout.readlines()
            stderr.readlines()
            ssh.exec_command("sudo chmod +x /tmp/prepareHost.sh")
            (stdin, stdout, stderr) = ssh.exec_command("/tmp/prepareHost.sh " + os.environ.get("DISK_QTY") + " &> /tmp/prepareHost.log")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir /data;sudo mount -a")
            stdout.readlines()
            stderr.readlines()
            homeDir = os.environ.get("BASE_HOME") + "/home"
            (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir -p " + homeDir + ";sudo useradd -b " + homeDir + " -s " + "/bin/bash -m gpadmin")
            stdout.readlines()
            stderr.readlines()

            (stdin, stdout, stderr) = ssh.exec_command("sudo echo " + os.environ.get("GPADMIN_PW") + " | sudo passwd --stdin gpadmin")
            stdout.readlines()
            stderr.readlines()

            # could change to node.reboot
            print clusterNode["nodeName"]+": Rebooting"
            (stdin, stdout, stderr) = ssh.exec_command("sudo reboot")
            stdout.readlines()
            stderr.readlines()
            connected = True
        except Exception as e:
            print "     " + nodeName + ": Attempting SSH Connection"
            time.sleep(3)

            if attemptCount > 40:
                print "CLUSTER CREATION FAILED:   Cleanup and Retry"
                exit()
        finally:
            ssh.close()
    return


def hostsFiles(clusterDictionary):
    clusterPath = "./clusterConfigs/" + clusterDictionary["clusterName"]
    os.chdir(clusterPath)
    with open("hosts", "w") as hostsFile:
        hostsFile.write("######  CAPE ENTRIES #######\n")
        for node in clusterDictionary["clusterNodes"]:
            hostsFile.write(node["internalIP"] + "  " + node["nodeName"] + "\n")

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
    threads = []
    for clusterNode in clusterDictionary["clusterNodes"]:
        uploadThread = threading.Thread(target=hostFileUpload, args=(clusterNode,))
        threads.append(uploadThread)
        uploadThread.start()
    for x in threads:
        x.join()


def verifyCluster(clusterDictionary):
    print "Verifying Cluster"
    # Check /etc/hosts
    # SSH to each host and ping 000
    # check df -k for Data Drive


def keyShare(clusterDictionary):
    # NEED TO THREAD THE KEY SHARE TAKES WAY TOO LONG

    warnings.simplefilter("ignore")
    paramiko.util.log_to_file("/tmp/paramiko.log")

    # client.connect(clusterNode["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH, timeout=120)

    for node in clusterDictionary["clusterNodes"]:

        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())

                ssh.connect(node["externalIP"], 22, "gpadmin", password=str(os.environ.get("GPADMIN_PW")), timeout=120)
                (stdin, stdout, stderr) = ssh.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
                #(stdin, stdout, stderr) = ssh.exec_command("sudo rm -f /etc/yum.repos.d/CentOS-SCL*;sudo yum clean all")
                #stderr.readlines()
                #stdout.readlines()
                # (stdin, stdout, stderr) = ssh.exec_command("sudo rm -f /etc/yum.repos.d/CentOS-SCL*;sudo yum clean all;sudo yum install -y epel-release;sudo yum install -y sshpass git")
                # stderr.readlines()
                # stdout.readlines()
                ssh.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
                for node1 in clusterDictionary["clusterNodes"]:
                    (stdin, stdout, stderr) = ssh.exec_command("sshpass -p " + os.environ.get("GPADMIN_PW") + "  ssh gpadmin@" + node1["nodeName"]+ " -o StrictHostKeyChecking=no" )
                    (stdin, stdout, stderr) = ssh.exec_command("sshpass -p " + os.environ.get("GPADMIN_PW") + "  ssh-copy-id  gpadmin@" + node1["nodeName"])
                    stderr.readlines()
                    stdout.readlines()

                ssh.close()
                ssh.connect(node["externalIP"], 22, "root", password=str(os.environ.get("ROOT_PW")),
                            timeout=120)
                (stdin, stdout, stderr) = ssh.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
                stderr.readlines()
                stdout.readlines()
                ssh.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
                for node1 in clusterDictionary["clusterNodes"]:
                    (stdin, stdout, stderr) = ssh.exec_command(
                        "sshpass -p " + os.environ.get("ROOT_PW") + "  ssh-copy-id  root@" + node1[
                            "nodeName"])
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


def hostFileUpload(clusterNode):
    warnings.simplefilter("ignore")
    paramiko.util.log_to_file("/tmp/paramiko.log")

    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, os.environ.get("SSH_USERNAME"), None, pkey=None,
                        key_filename=str(os.environ.get("CONFIGS_PATH")) + str(os.environ.get("SSH_KEY")), timeout=120)

            sftp = ssh.open_sftp()
            sftp.put("hosts", "/tmp/hosts")
            sftp.put("allhosts", "/tmp/allhosts")
            sftp.put("workers", "/tmp/workers")
            (stdin, stdout, stderr) = ssh.exec_command("sudo sh -c 'cat /tmp/hosts >> /etc/hosts'")
            connected = True
        except Exception as e:
            # print e
            print "     " + clusterNode["nodeName"] + ": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 40:
                print "Failing Process"
                exit()
        finally:
            ssh.close()
