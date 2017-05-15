import sys
import os
import warnings
import traceback
import logging
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from libcloud.compute.base import Node as node


def destroyServers(clusterDictionary):
    #
    # This method destroys a cluster without checking on state of the nodes
    #
    warnings.simplefilter("ignore")
    print clusterDictionary["clusterName"] + ": Destroy Cluster Started"
    try:

        logging.debug('SVC_ACCOUNT: ' + str(os.environ["SVC_ACCOUNT"]))
        logging.debug('CONFIGS_PATH: ' + str(os.environ["CONFIGS_PATH"]))
        logging.debug('SVC_ACCOUNT_KEY: ' + str(os.environ["SVC_ACCOUNT_KEY"]))
        logging.debug('PROJECT: ' + str(os.environ["PROJECT"]))
        logging.debug('ZONE: ' + str(os.environ["ZONE"]))
        ComputeEngine = get_driver(Provider.GCE)
        driver = ComputeEngine(os.environ["SVC_ACCOUNT"],
                               str(os.environ["CONFIGS_PATH"]) +
                               str(os.environ["SVC_ACCOUNT_KEY"]),
                               project=str(os.environ["PROJECT"]),
                               datacenter=str(os.environ["ZONE"]))
        nodeList = []
        delNames = []
        # get a list of nodes from the Cloud Provider
        nodes = driver.list_nodes(ex_zone=str(os.environ["ZONE"]))
        numNodes = int(clusterDictionary["nodeQty"])
        # Create a list of node names to delete
        for i in range(0, int(numNodes)):
            delNames.append(clusterDictionary["clusterName"] + "-00" + str(i))

        logging.info('List of all Node Names to delete: {0}'.format(",".join(delNames)))

        nodeList = [node for x in delNames for node in nodes if x in node.name]
        logging.debug("Nodes being deleted {0}".format(nodeList))

        delnodes = driver.ex_destroy_multiple_nodes(nodeList,
                                                    ignore_errors=True,
                                                    destroy_boot_disk=False,
                                                    poll_interval=2,
                                                    timeout=180)
        if (len(nodeList)) != (len(delnodes)):
            print clusterDictionary["clusterName"] + ": We may have deleted \
                more nodes than expected!! "
            logging.debug('nodeList: ' + str(len(nodeList)))
            logging.debug('delnodes: ' + str(len(delnodes)))
            print delnodes
        else:
            print clusterDictionary["clusterName"] + ": Destroyed Nodes"
            for i in range(0, int(len(nodeList))):
                print "\t" + nodeList[i].name + ": " + str(delnodes[i])
                logging.debug(nodeList[i].name + ": " + str(delnodes[i]))

    except Exception as e:
        print e
        print traceback.print_exc()
        logging.debug('Exception: ' + str(e))
        logging.debug(traceback.print_exc())
        sys.exit("destroyServers Failed")
