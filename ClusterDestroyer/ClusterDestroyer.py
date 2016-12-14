import os
import sys
import warnings
import traceback
import logging
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
        driver = ComputeEngine(os.environ["SVC_ACCOUNT"],
                               str(os.environ["CONFIGS_PATH"]) +
                               str(os.environ["SVC_ACCOUNT_KEY"]),
                               project=str(os.environ["PROJECT"]),
                               datacenter=str(os.environ["ZONE"]))
        nodeList = []

        nodes = driver.list_nodes(ex_zone=str(os.environ["ZONE"]))
        for node in nodes:
            if clusterDictionary["clusterName"] in node.name:
                nodeList.append(node)
                # print node
                # print node.extra

        logging.info('Deleting nodes that have ' +
                     clusterDictionary["clusterName"] +
                     ' in their name.')
        delnodes = driver.ex_destroy_multiple_nodes(nodeList, ignore_errors=True,
                                                   destroy_boot_disk=False,
                                                   poll_interval=2, timeout=180)
        if (len(nodeList)) != (len(delnodes)):
            print clusterDictionary["clusterName"] + ": We may have deleted \
                more nodes than expected!! "
            logging.debug('nodeList: ' + str(len(nodeList)))
            logging.debug('delnodes: '_ str(len(delnodes)))
            print delnodes
        else:
            print clusterDictionary["clusterName"] + ": Destroyed Nodes"
            for i in range(0, int(len(nodeList))):
                print "\t" + nodeList[i].name + ": " + str(delnodes[i])
                logging.debug(nodeList[i].name + ": " + str(delnodes[i])


    except Exception as e:
        print e
        print traceback.print_exc()
        sys.exit("destroyServers Failed")
