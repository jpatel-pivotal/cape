import argparse
import datetime
import os
import sys

from dotenv import load_dotenv

from ClusterBuilder import ClusterBuilder
from ClusterBuilder import InstallGPDB
from ClusterBuilder import SoftwareDownload
from ClusterDestroyer import ClusterDestroyer
from QueryCluster import QueryCluster
from LabBuilder import StudentAccounts


def cliParse():
    VALID_ACTION = ["create", "destroy", "query", "stage", "dbctl"]
    parser = argparse.ArgumentParser(description='Cluster Automation for Pivotal Education')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_create = subparsers.add_parser("create", help="Create a Cluster")
    parser_destroy = subparsers.add_parser("destroy", help="Destroy a Cluster")
    parser_query = subparsers.add_parser("query", help="Query a Cluster")
    parser_stage = subparsers.add_parser("stage", help="Stage a Cluster")
    parser_gpdb = subparsers.add_parser("gpdb", help="Start/Stop, get state of GPDB")

    parser_create.add_argument("--type", dest='type', action="store",
                               help="Type of cluster to be create (gpdb/hdb/vanilla", required=True)
    parser_create.add_argument("--name", dest='clustername', action="store", help="Name of Cluster to be Created",
                               required=True)
    parser_create.add_argument("--nodes", dest='nodes', default=1, action="store", help="Number of Nodes to be Created",
                               required=True)

    parser_create.add_argument("-v", dest='verbose', action='store_true', required=False)

    parser_create.add_argument("--config", dest='config', default='./configs/config.env', action="store", help="Config.env file",
                               required=False)

    #parser_create.add_argument("-l", dest='lab', action='store_true', required=False,
    #                         help="Include Lab creation in Cluster Buildout")

    parser_stage.add_argument("--name", dest='clustername', action="store", help="Name of Cluster to be Staged",
                              required=True)
    parser_query.add_argument("--name", dest='clustername', action="store", help="Name of Cluster to be Queried",
                              required=True)
    parser_query.add_argument("--nodes", dest='nodes', default=1, action="store", help="Number of Nodes to be Queried",
                               required=True)
    parser_query.add_argument("--config", dest='config', default='./configs/config.env', action="store", help="Config.env file",
                               required=False)
    # Adding in type as an optinoal arg for now. to be used in the future
    parser_query.add_argument("--type", dest='type', action="store",
                               help="Type of cluster to be create (gpdb/hdb/vanilla", required=False)
    parser_gpdb.add_argument("--clustername", dest='clustername', action="store", help="Name of Cluster to be Staged",
                             required=True)

    parser_gpdb.add_argument("--action", dest='action', action="store", help="start/stop/state", required=True)

    parser_destroy.add_argument("--name", dest='clustername', action="store",help="Name of Cluster to be Deleted",required=True)

    parser_destroy.add_argument("--config", dest='config', default='./configs/config.env', action="store", help="Config.env file",
                               required=False)
    parser_destroy.add_argument("--nodes", dest='nodes', default=1, action="store", help="Number of Nodes to be Created",
                               required=True)
    # Adding in type as an optinoal arg for now. to be used in the future
    parser_destroy.add_argument("--type", dest='type', action="store",
                               help="Type of cluster to be create (gpdb/hdb/vanilla", required=False)

    args = parser.parse_args()

    clusterDictionary = {}

    if (args.subparser_name == "create"):
        clusterDictionary["clusterName"] = args.clustername
        clusterDictionary["nodeQty"] = args.nodes
        clusterDictionary["clusterType"] = "pivotal-" + args.type
        clusterDictionary["segmentDBs"] = os.environ.get("SEGMENTDBS")
        clusterDictionary["masterCount"] = 0
        clusterDictionary["accessCount"] = 0
        clusterDictionary["segmentCount"] = 0
        if (args.config):
            print "Loading Configuration"
            load_dotenv(args.config)
            os.environ["CONFIGS_PATH"] = os.path.dirname(args.config) + '/'

        if (args.type == "vanilla"):
            ClusterBuilder.buildServers(clusterDictionary)
        elif (args.type == "gpdb"):
            print clusterDictionary["clusterName"] + ": Creating a Greenplum Database Cluster"
            ClusterBuilder.buildServers(clusterDictionary)
            downloads = SoftwareDownload.downloadSoftware(clusterDictionary)
            InstallGPDB.installGPDB(clusterDictionary, downloads)
        elif (args.type == "hdb"):
            print "HDB Builder"
            ClusterBuilder.buildServers(clusterDictionary)
            SoftwareDownload.downloadSoftware(clusterDictionary)
            #StudentAccounts.add(clusterDictionary)

            # if (args.verbose == True):
            #     clusterNodes = ClusterBuilder.buildServers(config)
            #     #createCluster(clusterDictionary,False)  #These are opposite because  the logging value is quiet_stdout so True is no logging
            #
            # else:
            #     ClusterBuilder.buildServers(clusterDictionary,config)
            #
            #    # createCluster(clusterDictionary,True)

            # elif (args.subparser_name == "destroy"):
            #     clusterDictionary["clusterName"] = args.clustername
            #     print "Not Yet Implemented"
            # elif (args.subparser_name == "query"):
            #     clusterInfo = queryCluster(args.clustername)
            #
            #
            # elif (args.subparser_name == "stage"):
            #     clusterInfo = queryCluster(args.clustername)
            #     # TEMPORARY    REMOVE!!!
            #     # with open("./" + clusterInfo["name"] + "/clusterInfo.json") as clusterInfoFile:
            #     #     clusterInfo = json.load(clusterInfoFile)
            #     #downloadSoftware(clusterDictionary, "pivotal-gpdb")
            #
            #     stageCluster(clusterInfo)
            #
            # elif (args.subparser_name == "gpdb"):
            #     clusterName = (args.clustername)
            #     with open("./" + clusterName + "/clusterInfo.json") as clusterInfoFile:
            #         clusterInfo = json.load(clusterInfoFile)
            #     Users.gpControl(clusterInfo,args.action)
    elif (args.subparser_name == "query"):
        clusterDictionary["clusterName"] = args.clustername
        clusterDictionary["nodeQty"] = args.nodes
        if (args.config):
            print "Loading Configuration"
            load_dotenv(args.config)
            os.environ["CONFIGS_PATH"] = os.path.dirname(args.config) + '/'
        print clusterDictionary["clusterName"] + ": Querying Nodes in a Cluster"
        QueryCluster.checkServerState(clusterDictionary)
    elif (args.subparser_name == "destroy"):
        clusterDictionary["clusterName"] = args.clustername
        clusterDictionary["nodeQty"] = args.nodes
        if (args.config):
            print "Loading Configuration"
            load_dotenv(args.config)
            os.environ["CONFIGS_PATH"] = os.path.dirname(args.config) + '/'
        print clusterDictionary["clusterName"] + ": Destroying Cluster"
        ClusterDestroyer.destroyServers(clusterDictionary)


if __name__ == '__main__':
    startTime = datetime.datetime.today()
    print  "Start Time: ", startTime
    os.environ["CAPE_HOME"] = os.getcwd()
    cliParse()
    stopTime = datetime.datetime.today()
    print  "Cluster " + sys.argv[1] + " Completion Time: ", stopTime
    print  "Elapsed Time: ", stopTime - startTime
