import argparse
import warnings
import os
import ConfigParser
from ClusterBuilder import ClusterBuilder
from ClusterBuilder import SoftwareDownload
from LabBuilder import StudentAccounts
from os.path import join, dirname
from dotenv import load_dotenv
from ClusterBuilder import InstallGPDB


def cliParse():
    VALID_ACTION = ["create","destroy","query","stage","dbctl"]
    parser = argparse.ArgumentParser(description='Cluster Automation for Pivotal Education')
    subparsers = parser.add_subparsers(help='sub-command help', dest="subparser_name")
    parser_create = subparsers.add_parser("create", help="Create a Cluster")
    parser_destroy = subparsers.add_parser("destroy", help="Destroy a Cluster")
    parser_query = subparsers.add_parser("query", help="Query a Cluster")
    parser_stage = subparsers.add_parser("stage", help="Stage a Cluster")
    parser_gpdb = subparsers.add_parser("gpdb", help="Start/Stop, get state of GPDB")

    parser_create.add_argument("--type", dest='type', action="store",help="Type of cluster to be create (gpdb/hdb/vanilla",required=True)
    parser_create.add_argument("--name", dest='clustername', action="store",help="Name of Cluster to be Created",required=True)
    parser_create.add_argument("--nodes", dest='nodes', default=1, action="store", help="Number of Nodes to be Created",required=True)
    parser_create.add_argument("-v", dest='verbose', action='store_true',required=False)

    parser_stage.add_argument("--name", dest='clustername', action="store",help="Name of Cluster to be Staged",required=True)
    parser_query.add_argument("--name", dest='clustername', action="store",help="Name of Cluster to be Staged",required=True)

    parser_gpdb.add_argument("--clustername", dest='clustername', action="store",help="Name of Cluster to be Staged",required=True)

    parser_gpdb.add_argument("--action", dest='action', action="store",help="start/stop/state",required=True)


    # parser_destroy.add_argument("--clustername", dest='clustername', action="store",help="Name of Cluster to be Deleted",required=True)


    args = parser.parse_args()

    clusterDictionary = {}
    if (args.subparser_name == "create"):
        clusterDictionary["clusterName"] = args.clustername
        clusterDictionary["nodeQty"] = args.nodes
        clusterDictionary["clusterType"] = "pivotal-" + args.type
        if (args.type == "vanilla"):
            ClusterBuilder.buildServers(clusterDictionary)
        elif (args.type == "gpdb"):
            print "GPDB Builder"
            ClusterBuilder.buildServers(clusterDictionary)
            downloads = SoftwareDownload.downloadSoftware(clusterDictionary)
            InstallGPDB.installGPDB(clusterDictionary,downloads)
        elif (args.type == "hdb"):
            print "HDB Builder"
            ClusterBuilder.buildServers(clusterDictionary)
            SoftwareDownload.downloadSoftware(clusterDictionary)
            StudentAccounts.add(clusterDictionary)

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







if __name__ == '__main__':
    dotenv_path = "./configs/config.env"
    load_dotenv(dotenv_path)
    os.environ["CAPE_HOME"]=os.getcwd()
    os.environ["CONFIGS_PATH"]=os.getcwd()+"/configs/"
    cliParse()