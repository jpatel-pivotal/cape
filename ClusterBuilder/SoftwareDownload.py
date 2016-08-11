import paramiko
from paramiko import WarningPolicy
import warnings
import requests
import json
import time
import os

def downloadSoftware(clusterDictionary):
    print clusterDictionary["clusterType"]
    warnings.simplefilter("ignore")
    package = clusterDictionary["clusterType"]
    headers = {"Authorization": "Token "+ os.environ.get("PIVNET_APIKEY")}

    # FIND PRODUCT
    response = requests.get("https://network.pivotal.io/api/v2/products")
    res = json.loads(response.text)
    for product in res["products"]:
        if package in product["slug"]:
            releasesURL = product["_links"]["releases"].get("href")
            productId = product["id"]
    response = requests.get(releasesURL)
    res = json.loads(response.text)
    # GET LATEST RELEASE
    latest=0
    for versions in res["releases"]:
        versionInt =  int(str(versions["version"]).replace(".",""))
        if (versionInt > latest):
            latest=versionInt;
            latestVersion = versions

            # # GET DOWNLOAD URL AND FILENAME

    getURL = "https://network.pivotal.io/api/v2/products/"+package+"/releases/"+str(latestVersion["id"])
    response = requests.get(getURL,headers=headers)
    responseJSON = json.loads(response.text)
    downloads=[]


#HDB 2.0 Software
#HDB 2.0 MADlib - Machine Learning
#HDB 2.0 Language Extensions
#JDBC/ODBC Drivers

#4.3.8.1 Greenplum Database Sandbox Virtual Machines
#Greenplum Command Center
#Greenplum Support
#Greenplum DataDirect ODBC and JDBC Drivers
#4.3.5+ Partner Connector
#4.3.5+ Analytics
#4.3.5+ Encryption
#4.3.6+ Language extensions
#4.3.8.1 Loaders
#4.3.8.1 Connectivity
#4.3.8.1 Clients
#4.3.8.1 Database Server


    for fileInfo in responseJSON["file_groups"]:
        downloadFile={}
        if "pivotal-gpdb" in clusterDictionary["clusterType"]:
            if "Database Server" in fileInfo["name"]:
                for file in fileInfo["product_files"]:
                    if "Red Hat Enterprise Linux 5, 6" in file["name"]:
                        downloadFile["URL"] = file["_links"]["download"].get("href")
                        downloadFile["NAME"] = str(file["aws_object_key"]).split("/")[2]
                        downloads.append(downloadFile)
            elif "Loader" in fileInfo["name"]:
                for file in fileInfo["product_files"]:
                    if "Red Hat Enterprise Linux x86_64" in file["name"]:
                        downloadFile["URL"] = file["_links"]["download"].get("href")
                        downloadFile["NAME"] = str(file["aws_object_key"]).split("/")[2]
                        downloads.append(downloadFile)
            elif "Analytics" in fileInfo["name"]:
                for file in fileInfo["product_files"]:
                    if os.environ.get("MADLIB_VERSION") in file["name"]:
                        downloadFile["URL"] = file["_links"]["download"].get("href")
                        downloadFile["NAME"] = str(file["aws_object_key"]).split("/")[2]
                        downloads.append(downloadFile)
            elif "Language extensions" in fileInfo["name"]:
                for file in fileInfo["product_files"]:
                    if "PL/R Extension for RHEL" in file["name"]:
                        downloadFile["URL"] = file["_links"]["download"].get("href")
                        downloadFile["NAME"] = str(file["aws_object_key"]).split("/")[2]
                        downloads.append(downloadFile)
        elif "pivotal-hdb" in clusterDictionary["clusterType"]:
            if "Software" in fileInfo["name"]:
                for file in fileInfo["product_files"]:
                    if "RHEL" in file["name"]:
                        downloadFile["URL"] = file["_links"]["download"].get("href")
                        downloadFile["NAME"] = str(file["aws_object_key"]).split("/")[2]
                        downloads.append(downloadFile)
                        print str(file["aws_object_key"]).split("/")[2]
            elif "Language" in fileInfo["name"]:
                for file in fileInfo["product_files"]:
                    print file["name"]
                    downloadFile["URL"] = file["_links"]["download"].get("href")
                    downloadFile["NAME"] = str(file["aws_object_key"]).split("/")[2]
                    downloads.append(downloadFile)
            elif "MADlib" in fileInfo["name"]:
                for file in fileInfo["product_files"]:
                    print file["name"]
                    if os.environ.get("MADLIB_VERSION") in file["name"]:
                        downloadFile["URL"] = file["_links"]["download"].get("href")
                        downloadFile["NAME"] = str(file["aws_object_key"]).split("/")[2]
                        downloads.append(downloadFile)

    if "pivotal-gpdb" in clusterDictionary["clusterType"]:

        for node in clusterDictionary["clusterNodes"]:
            print node["nodeName"] + ": Downloading Required Software from PIVNET"
            connected = False
            attemptCount = 0
            while not connected:
                try:
                    attemptCount += 1

                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(WarningPolicy())
                    ssh.connect(node["externalIP"], 22, os.environ.get("SSH_USERNAME"), None, pkey=None, key_filename=os.environ.get("CONFIGS_PATH")+os.environ.get("SSH_KEY"),timeout=120)

                    for file in downloads:
                        print node["nodeName"]+": Downloading "+str(file['NAME'])
                        (stdin, stdout, stderr) = ssh.exec_command("wget --header=\"Authorization: Token "+os.environ.get("PIVNET_APIKEY") + "\" --post-data='' " + str(file['URL'])+" -O /tmp/"+str(file['NAME']))

                        stderr.readlines()
                        stdout.readlines()


                    connected = True
                except Exception as e:
                    print e
                    print node["nodeName"] + ": Attempting SSH Connection"
                    time.sleep(3)
                    if attemptCount > 1:
                        print "Failing Process"
                        exit()
                finally:
                    ssh.close()

    elif "pivotal-hdb" in clusterDictionary["clusterType"]:
        for node in clusterDictionary["clusterNodes"]:
            if "access" in node["role"]:
                print node["nodeName"] + ": Downloading Required Software from PIVNET to Management Server"
                connected = False
                attemptCount = 0
                while not connected:
                    try:
                        attemptCount += 1

                        ssh = paramiko.SSHClient()
                        ssh.set_missing_host_key_policy(WarningPolicy())
                        ssh.connect(node["externalIP"], 22, os.environ.get("SSH_USERNAME"), None, pkey=None,
                                    key_filename=os.environ.get("CONFIGS_PATH") + os.environ.get("SSH_KEY"), timeout=120)

                        for file in downloads:
                            print node["nodeName"] + ": Downloading " + str(file['NAME'])
                            (stdin, stdout, stderr) = ssh.exec_command(
                                "wget --header=\"Authorization: Token " + os.environ.get("PIVNET_APIKEY") + "\" --post-data='' " + str(
                                    file['URL']) + " -O /tmp/" + str(file['NAME']))

                            stderr.readlines()
                            stdout.readlines()

                        connected = True
                    except Exception as e:
                        print e
                        print node["nodeName"] + ": Attempting SSH Connection"
                        time.sleep(3)
                        if attemptCount > 1:
                            print "Failing Process"
                            exit()
                    finally:
                        ssh.close()

