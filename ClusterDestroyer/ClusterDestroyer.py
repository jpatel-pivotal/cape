import os
import sys
import warnings
import traceback
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from libcloud.compute.base import Node as node
from libcloud.compute.drivers.dummy import DummyNodeDriver



def destroyServers(clusterDictionary):
    #
    # This method destroys a cluster without checking on state of the nodes
    #
    warnings.simplefilter("ignore")
    print clusterDictionary["clusterName"] + ": Destroy Cluster Started"
    try:

        ComputeEngine = get_driver(Provider.GCE)
        driver = ComputeEngine(os.environ.get("SVC_ACCOUNT"),
                               str(os.environ.get("CONFIGS_PATH")) +
                               str(os.environ.get("SVC_ACCOUNT_KEY")),
                               project=str(os.environ.get("PROJECT")),
                               datacenter=str(os.environ.get("ZONE")))
        nodeList = []

        nodes = driver.list_nodes(ex_zone=str(os.environ.get("ZONE")))
        for node in nodes:
            if clusterDictionary["clusterName"] in node.name:
                nodeList.append(node)
                # print node
                # print node.extra


        delnodes = driver.ex_destroy_multiple_nodes(nodeList, ignore_errors=True,
                                                   destroy_boot_disk=False,
                                                   poll_interval=2, timeout=180)
        if (len(nodeList)) != (len(delnodes)):
            print clusterDictionary["clusterName"] + ": We may have deleted \
                more nodes than expected!! "
            print delnodes
        else:
            print clusterDictionary["clusterName"] + ": Destroyed Nodes"
            for i in range(0, int(len(nodeList))):
                print("\t" + nodeList[i].name, delnodes[i])


    except Exception as e:
        print e
        print traceback.print_exc()
        sys.exit("destroyServers Failed")
