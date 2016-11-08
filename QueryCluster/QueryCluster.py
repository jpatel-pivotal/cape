import os
import time
import warnings
import traceback
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider



def checkServerState(clusterDictionary):
    # This methid checks if each node in the cluster is in RUNNING State
    # The most basic check is all we are doing here
    # We are using this before we destroy the cluster
    warnings.simplefilter("ignore")
    print clusterDictionary["clusterName"] + ": Querying Cluster State Started"
    try:
        print(os.getenv('CONFIGS_PATH'))
        ComputeEngine = get_driver(Provider.GCE)
        driver = ComputeEngine(os.environ.get("SVC_ACCOUNT"),
                               str(os.environ.get("CONFIGS_PATH")) +
                               str(os.environ.get("SVC_ACCOUNT_KEY")),
                               project=str(os.environ.get("PROJECT")),
                               datacenter=str(os.environ.get("ZONE")))

        nodes = driver.list_nodes(ex_zone=str(os.environ.get("ZONE")))
        if not nodes:
            print "Did not find any nodes for that cluster!"
        else:
            for node in nodes:
                if clusterDictionary["clusterName"] in node.name:
                    print "\t" + node.name + ": " + node.state

    except Exception as e:
        print e
        print traceback.print_exc()
