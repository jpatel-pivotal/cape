from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
import paramiko
from paramiko import WarningPolicy
import warnings
import time
import os
import threading

def buildServers(clusterDictionary,config):
    warnings.simplefilter("ignore")
    print clusterDictionary["clusterName"] + ": Cluster Creation Started"

    if not os.path.exists(clusterDictionary["clusterName"]):
        os.makedirs("./dockercfg/"+clusterDictionary["clusterName"])
    clusterPath = "./dockercfg/" + clusterDictionary["clusterName"]


    clusterNodes=[]
    ComputeEngine = get_driver(Provider.GCE)
    driver = ComputeEngine(config.get("gce-settings","SVC_ACCOUNT"), config.get("gce-settings","SVC_ACCOUNT_KEY"),project=config.get("gce-settings","PROJECT"), datacenter=config.get("gce-settings","ZONE"))
    sa_scopes = [{'scopes': ['compute', 'storage-full']}]
    print clusterDictionary["clusterName"] + ": Creating "+ str(clusterDictionary["nodeQty"]) + " Nodes"
    nodes = driver.ex_create_multiple_nodes(clusterDictionary["clusterName"], config.get("gce-settings","SERVER_TYPE"), config.get("gce-settings","IMAGE"),
                                            int(clusterDictionary["nodeQty"]), config.get("gce-settings","ZONE"),
                                            ex_network='default', ex_tags=None, ex_metadata=None, ignore_errors=True,
                                            use_existing_disk=True, poll_interval=2, external_ip='ephemeral',
                                            ex_disk_type='pd-standard', ex_disk_auto_delete=True,
                                            ex_service_accounts=None,
                                            timeout=180, description=None, ex_can_ip_forward=None,
                                            ex_disks_gce_struct=None,
                                            ex_nic_gce_struct=None, ex_on_host_maintenance=None,
                                            ex_automatic_restart=None)



    print clusterDictionary["clusterName"] + ": Cluster Nodes Created in Google Cloud"
    print clusterDictionary["clusterName"] + ": Cluster Configuration Started"

    #THREAD THIS
    threads=[]
    for nodeCnt in range(int(clusterDictionary["nodeQty"])):
        nodeName = clusterDictionary["clusterName"] + "-" + str(nodeCnt).zfill(3)
        clusterNode = {}

        volume = driver.create_volume(config.get("gce-settings", "DISK_SIZE"), nodeName + "-data-disk", None, None,
                                      None, False, "pd-standard")

        clusterNode["nodeName"] = nodeName
        clusterNode["dataVolume"] = str(volume)

        node = driver.ex_get_node(nodeName)
        driver.attach_volume(node, volume, device=None, ex_mode=None, ex_boot=False, ex_type=None, ex_source=None,
                             ex_auto_delete=True, ex_initialize_params=None, ex_licenses=None, ex_interface=None)



        clusterNode["externalIP"] = str(node).split(",")[3].split("'")[1]
        clusterNode["internalIP"] = str(node).split(",")[4].split("'")[1]
        print "     " + nodeName + ": Server Prep Phase Started"
        print "     " + nodeName + ": External IP: " + clusterNode["externalIP"]
        print "     " + nodeName + ": Internal IP: " + clusterNode["internalIP"]
        prepThread = threading.Thread(target=prepServer, args=(clusterNode,config,nodeCnt))
        clusterNodes.append(clusterNode)
        threads.append(prepThread)
        prepThread.start()
    for x in threads:
        x.join()
    print clusterDictionary["clusterName"] + ": Cluster Configuration Complete"
    clusterDictionary["clusterNodes"]=clusterNodes
    hostsFiles(clusterDictionary,config)
    keyShare(clusterDictionary,config)



def prepServer(clusterNode,config,nodeCnt):
    warnings.simplefilter("ignore")
    paramiko.util.log_to_file("/tmp/paramiko.log")
    nodeName = clusterNode["nodeName"]


    # Set Server Role

    if (nodeCnt) == 0:
        clusterNode["role"] = "access"
    elif (nodeCnt) == 1:
        clusterNode["role"] = "master1"
    elif (nodeCnt) == 2:
        clusterNode["role"] = "master2"
    else:
        clusterNode["role"] = "worker"
    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, config.get("gce-settings","SSH_USERNAME"), None, pkey=None, key_filename=config.get("gce-settings","SSH_KEY_PATH"),timeout=120)
            sftp = ssh.open_sftp()
            sftp.put('./configs/sysctl.conf.cape', '/tmp/sysctl.conf.cape',confirm=True)
            sftp.put('./configs/fstab.cape', '/tmp/fstab.cape',confirm=True)
            sftp.put('./configs/limits.conf.cape', '/tmp/limits.conf.cape',confirm=True)
            sftp.put('./scripts/prepareHost.sh', '/tmp/prepareHost.sh',confirm=True)
            time.sleep(10)



            gpadminPW = config.get("cape-settings","GPADMIN_PW")
            rootPW = config.get("cape-settings","ROOT_PW")

            (stdin, stdout, stderr) = ssh.exec_command("sudo echo "+ gpadminPW + " | sudo passwd --stdin gpadmin")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo echo "+ rootPW + " | sudo passwd --stdin root")
            stdout.readlines()
            stderr.readlines()

            ssh.exec_command("sudo chmod +x /tmp/prepareHost.sh")
            (stdin, stdout, stderr) = ssh.exec_command("/tmp/prepareHost.sh &> /tmp/prepareHost.log")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir /data")
            stdout.readlines()
            stderr.readlines()
            (stdin, stdout, stderr) = ssh.exec_command("sudo reboot")
            stdout.readlines()
            stderr.readlines()
            connected = True
        except Exception as e:
            #print e
            print "     " +nodeName + ": Attempting SSH Connection"
            time.sleep(3)

            if attemptCount > 40:
                print "CLUSTER CREATION FAILED:   Cleanup and Retry"
                exit()
        finally:
            ssh.close()
    print "     " +nodeName + ": Server Prep Phase Completed"
    return



def hostsFiles(clusterDictionary,config):
    print clusterDictionary["clusterName"] + ": Creating /etc/hosts for Cluster Nodes"
    clusterPath = "./dockercfg/" + clusterDictionary["clusterName"]
    os.chdir(clusterPath)
    with open ("hosts","w") as hostsFile:
        hostsFile.write("######  CAPE ENTRIES #######\n")
        for node in clusterDictionary["clusterNodes"]:
            hostsFile.write(node["internalIP"]+"  "+node["nodeName"]+"\n")

    with open ("workers","w") as workersFile:
        with open("allhosts", "w") as allhostsFile:
            for node in clusterDictionary["clusterNodes"]:
                if "master1" in node["role"] :
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
        uploadThread = threading.Thread(target=hostFileUpload, args=(clusterNode,config))
        threads.append(uploadThread)
        uploadThread.start()
    for x in threads:
        x.join()
    print "Upload Complete"


def verifyCluster(clusterDictionary,config):
    print "Verifying Cluster"
    # Check /etc/hosts
    # SSH to each host and ping 000
    # check df -k for Data Drive



def keyShare(clusterDictionary,config):

    # NEED TO THREAD THE KEY SHARE TAKES WAY TOO LONG

    print "Running hostPrep"
    print "Creating Data Directories and Sharing gpadmin keys across Cluster for passwordless ssh"
    warnings.simplefilter("ignore")
    paramiko.util.log_to_file("/tmp/paramiko.log")



    #client.connect(clusterNode["externalIP"], 22, SSH_USERNAME, None, pkey=None, key_filename=SSH_KEY_PATH, timeout=120)

    for node in clusterDictionary["clusterNodes"]:

        connected = False
        attemptCount = 0
        while not connected:
            try:
                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())
                ssh.connect(node["externalIP"], 22, "gpadmin", password=config.get("cape-settings","GPADMIN_PW"), timeout=120)
                (stdin, stdout, stderr) = ssh.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
                stderr.readlines()
                stdout.readlines()
                (stdin, stdout, stderr) = ssh.exec_command("sudo rm -f /etc/yum.repos.d/CentOS-SCL*;sudo yum clean all;sudo yum install -y epel-release;sudo yum install -y sshpass git")
                stderr.readlines()
                stdout.readlines()
                ssh.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
                for node1 in clusterDictionary["clusterNodes"]:
                    (stdin, stdout, stderr) = ssh.exec_command(
                        "sshpass -p " + config.get("cape-settings","GPADMIN_PW") + "  ssh-copy-id  gpadmin@" + node1["nodeName"])
                    stderr.readlines()
                    stdout.readlines()

                ssh.close()
                ssh.connect(node["externalIP"], 22, "root", password=config.get("cape-settings", "ROOT_PW"),
                            timeout=120)
                print "     " + node["nodeName"] + ": Configuring Node"
                (stdin, stdout, stderr) = ssh.exec_command("echo -e  'y\n'|ssh-keygen -f ~/.ssh/id_rsa -t rsa -N ''")
                stderr.readlines()
                stdout.readlines()
                ssh.exec_command("echo 'Host *\nStrictHostKeyChecking no' >> ~/.ssh/config;chmod 400 ~/.ssh/config")
                for node1 in clusterDictionary["clusterNodes"]:
                    (stdin, stdout, stderr) = ssh.exec_command(
                        "sshpass -p " + config.get("cape-settings", "ROOT_PW") + "  ssh-copy-id  root@" + node1[
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



def hostFileUpload(clusterNode,config):
    warnings.simplefilter("ignore")
    paramiko.util.log_to_file("/tmp/paramiko.log")

    connected = False
    attemptCount = 0

    while not connected:
        try:
            attemptCount += 1
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(WarningPolicy())
            ssh.connect(clusterNode["externalIP"], 22, config.get("gce-settings", "SSH_USERNAME"), None,pkey=None, key_filename=config.get("gce-settings", "SSH_KEY_PATH"), timeout=120)
            print clusterNode["externalIP"]
            print config.get("gce-settings", "SSH_USERNAME")
            print config.get("gce-settings", "SSH_KEY_PATH")
            
            sftp = ssh.open_sftp()
            sftp.put("hosts", "/tmp/hosts")
            sftp.put("allhosts", "/tmp/allhosts")
            sftp.put("workers", "/tmp/workers")
            ssh.exec_command("sudo sh -c 'cat /tmp/hosts >> /etc/hosts'")
            connected = True
        except Exception as e:
            #print e
            print "     " + clusterNode["nodeName"]+": Attempting SSH Connection"
            time.sleep(3)
            if attemptCount > 40:
                print "Failing Process"
                exit()
        finally:
            ssh.close()


#  wget -nv http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.1.2.1/ambari.repo -O /etc/yum.repos.d/ambari.repo
#  yum install ambari-server
#  ambari-server setup


