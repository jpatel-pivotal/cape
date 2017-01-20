import os
import warnings
import traceback
import logging
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider



def checkServerState(clusterDictionary):
    # This method checks if each node in the cluster is in RUNNING State
    # The most basic check is all we are doing here
    # We are using this before we destroy the cluster
    warnings.simplefilter("ignore")
    print clusterDictionary["clusterName"] + ": Querying Cluster State Started"
    try:
        if os.environ["PROVIDER"] == 'GCP':
            ComputeEngine = get_driver(Provider.GCE)
            driver = ComputeEngine(os.environ["SVC_ACCOUNT"],
                                   str(os.environ["CONFIGS_PATH"]) +
                                   str(os.environ["SVC_ACCOUNT_KEY"]),
                                   project=str(os.environ["PROJECT"]),
                                   datacenter=str(os.environ["ZONE"]))
        if os.environ["PROVIDER"] == 'AWS':
            cls = get_driver(Provider.EC2)
            driver = cls(ACCESS_ID, SECRET_KEY, region="us-west-1")
        nodes = driver.list_nodes(ex_zone=str(os.environ["ZONE"]))
        if not nodes:
            print "Did not find any nodes for that cluster!"
            logging.info('Did not find any nodes for that cluster')
        else:
            for node in nodes:
                if clusterDictionary["clusterName"] in node.name:
                    print "\t" + node.name + ": " + node.state
                    logging.debug(node.name + ": " + node.state)

    except Exception as e:
        print e
        print traceback.print_exc()
        logging.debug('Exception: ' + str(e))
        logging.debug(traceback.print_exc())
