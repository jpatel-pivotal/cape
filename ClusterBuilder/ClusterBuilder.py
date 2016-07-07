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
    print "ClusterBuilder:BuildServers"
    clusterNodes=[]
    ComputeEngine = get_driver(Provider.GCE)
    driver = ComputeEngine(config.get("gce-settings","SVC_ACCOUNT"), config.get("gce-settings","SVC_ACCOUNT_KEY"),project=config.get("gce-settings","PROJECT"), datacenter=config.get("gce-settings","ZONE"))
    sa_scopes = [{'scopes': ['compute', 'storage-full']}]

    print "Creating "+ str(clusterDictionary["nodeQty"]) + " Cluster Nodes"
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

    #THREAD THIS

    print "Attaching Storage to the Cluster Nodes"
    for nodeCnt in range(int(clusterDictionary["nodeQty"])):
        clusterNode = {}
        nodeName = clusterDictionary["clusterName"] + "-" + str(nodeCnt).zfill(3)
        print nodeName + ": Creating Data Disk Volume"
        volume = driver.create_volume(config.get("gce-settings","DISK_SIZE"), nodeName + "-data-disk", None, None, None, False, "pd-standard")
        clusterNode["nodeName"] = nodeName
        clusterNode["dataVolume"] = str(volume)
        print nodeName + ": Attaching Disk Volume"
        node = driver.ex_get_node(nodeName)
        driver.attach_volume(node, volume, device=None, ex_mode=None, ex_boot=False, ex_type=None, ex_source=None,
                             ex_auto_delete=True, ex_initialize_params=None, ex_licenses=None, ex_interface=None)


        clusterNode["externalIP"] = str(node).split(",")[3].split("'")[1]
        clusterNode["internalIP"] = str(node).split(",")[4].split("'")[1]

        print nodeName + ": External IP: " + clusterNode["externalIP"]
        print nodeName + ": Internal IP: " + clusterNode["internalIP"]
        # Set Server Role

        if (nodeCnt) == 0:
            clusterNode["role"] = "etl"
        elif (nodeCnt) == 1:
            clusterNode["role"] = "master"
        elif (nodeCnt) == 2:
            clusterNode["role"] = "standby"
        else:
            clusterNode["role"] = "worker"

        clusterNodes.append(clusterNode)
        clusterDictionary["nodeInfo"]=clusterNodes

        #Upload scripts to setup each host

        print nodeName + ": Prepping Host"

        connected = False
        attemptCount = 0

        while not connected:
            try:
                #paramiko.util.log_to_file('paramiko.log')
                #paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)

                attemptCount += 1
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(WarningPolicy())
                ssh.connect(clusterNode["externalIP"], 22, config.get("gce-settings","SSH_USERNAME"), None, pkey=None, key_filename=config.get("gce-settings","SSH_KEY_PATH"),timeout=120)
                sftp = ssh.open_sftp()

                print nodeName + ": Uploading Server Configuration Files"

                sftp.put('./configs/sysctl.conf.cape', '/tmp/sysctl.conf.cape',confirm=True)
                sftp.put('./configs/fstab.cape', '/tmp/fstab.cape',confirm=True)
                sftp.put('./configs/limits.conf.cape', '/tmp/limits.conf.cape',confirm=True)
                sftp.put('./scripts/prepareHost.sh', '/tmp/prepareHost.sh',confirm=True)


                ssh.exec_command("sudo chmod +x /tmp/prepareHost.sh")
                print nodeName + ": Running /tmp/prepareHost.sh"
                (stdin, stdout, stderr) = ssh.exec_command("/tmp/prepareHost.sh &> /tmp/prepareHost.log")
                stdout.readlines()
                stderr.readlines()
                (stdin, stdout, stderr) = ssh.exec_command("sudo mkdir /data")
                stdout.readlines()
                stderr.readlines()
                print  nodeName + ": Rebooting to make System Config Changes"
                ssh.close()
                #time.sleep(30)
                driver.reboot_node(node)
                connected = True
            except Exception as e:
                #print e
                print nodeName + ": Attempting SSH Connection"
                time.sleep(3)

                if attemptCount > 40:
                    print "Failing Process"
                    exit()
            finally:
                ssh.close()



    return clusterDictionary

