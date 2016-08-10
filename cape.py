import argparse
import warnings
import os
import ConfigParser
from ClusterBuilder import ClusterBuilder
from ClusterBuilder import SoftwareDownload

#create -type GPDB/HAWQ/NoDB -name <name> -nodes 8 -pivnet <pivnetid>

def configParse():
    config = ConfigParser.ConfigParser()
    config.read("config.ini")
    # SERVER_TYPE = config.get("settings","SERVER_TYPE")
    # IMAGE = config.get("settings","IMAGE")
    # ZONE =config.get("settings","ZONE")
    # DISK_TYPE = config.get("settings","DISK_TYPE")
    # PROJECT =config.get("settings","PROJECT")
    # GPADMIN_PW = config.get("settings","GPADMIN_PW")
    # SSH_USERNAME = config.get("settings","SSH_USERNAME")
    # SSH_KEY_PATH = config.get("settings","SSH_KEY_PATH")
    # KEY = config.get("settings","KEY")
    # SVC_ACCOUNT = config.get("settings","SVC_ACCOUNT")
    # DISK_SIZE = config.get("settings","DISK_SIZE")
    # MADLIB=config.get("settings","MADLIB")
    # PLR=config.get("settings","PLR")
    # GP_LOADER=config.get("settings","GP_LOADER")
    # PIVNET_APIKEY=config.get("settings","PIVNET_APIKEY")
    # BASE_USERNAME=config.get("settings","BASE_USERNAME")
    # BASE_PASSWORD=config.get("settings","BASE_PASSWORD")
    # BASE_HOME=config.get("settings","BASE_HOME")
    # NUM_USERS =config.get("settings","NUM_USERS")
    # LABS_PATH=config.get("settings","LABS_PATH")
    # #PIVNET_USERNAME=config.get("settings","PIVNET_USERNAME")
    # #PIVNET_PASSWORD=config.get("settings","PIVNET_PASSWORD")
    # GPDB_URL=config.get("settings","GPDB_URL")
    # MADLIB_URL=config.get("settings","MADLIB_URL")
    # PLR_URL=config.get("settings","PLR_URL")
    # GPLOADER_URL=config.get("settings","GPLOADER_URL")
    # MADLIB_VERSION=config.get("settings","MADLIB_VERSION")
    return config



def cliParse(config):

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
            ClusterBuilder.buildServers(clusterDictionary, config)
        elif (args.type == "gpdb"):
            print "GPDB Builder"
            ClusterBuilder.buildServers(clusterDictionary, config)
            SoftwareDownload.downloadSoftware(clusterDictionary,config)

        elif (args.type == "hdb"):
            print "Not Yet Implemented"

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
    cliParse(configParse())