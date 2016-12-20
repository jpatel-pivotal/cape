#!/usr/bin/env bash

###########################################################################################################################
##HDB on HDP  setup script
###########################################################################################################################

#Customize HDB install bits location
#export HDB_DOWNLOAD_LOC=https://www.dropbox.com/s/5rzhqxajbd5pq9k/hdb-2.0.0.0-22126.tar.gz
#export HDB_AMBARI_DOWNLOAD_LOC=https://www.dropbox.com/s/6ik8f3r472f7mzq/hdb-ambari-plugin-2.0.0-448.tar.gz
export PIV_NET_HDB=https://network.pivotal.io/api/v2/products/pivotal-hdb/releases/1695/product_files/4405/download
export PIV_NET_PLUGIN=https://network.pivotal.io/api/v2/products/pivotal-hdb/releases/1695/product_files/4404/download
export PIV_NET_MADLIB=https://network.pivotal.io/api/v2/products/pivotal-hdb/releases/1695/product_files/4327/download
export PIV_NET_EXTENSIONS=https://network.pivotal.io/api/v2/products/pivotal-hdb/releases/1695/product_files/4675/download

#Customize which services to deploy and other configs
export ambari_services="HDFS MAPREDUCE2 YARN ZOOKEEPER HIVE ZEPPELIN SPARK HAWQ PXF"
export ambari_password="admin"
export cluster_name=hdp
export host_count=1


################
# Script start #
################

#remove any files from previous install attempts
rm -rf /staging
rm -rf ~/ambari-bootstrap
rm -rf /usr/lib/hue
rm -f /etc/init.d/startup_script

#install bootstrap - for now use Dan's fork with _ fix
cd ~
#git clone https://github.com/seanorama/ambari-bootstrap.git
git clone https://github.com/dbbaskette/ambari-bootstrap.git


#install Ambari
echo "Installing Ambari..."
install_ambari_server=true ~/ambari-bootstrap/ambari-bootstrap.sh

#install zeppelin service defn
git clone https://github.com/hortonworks-gallery/ambari-zeppelin-service.git /var/lib/ambari-server/resources/stacks/HDP/2.4/services/ZEPPELIN
sed -i.bak '/dependencies for all/a \  "ZEPPELIN_MASTER-START": ["NAMENODE-START", "DATANODE-START"],' /var/lib/ambari-server/resources/stacks/HDP/2.4/role_command_order.json

#HAWQ setup
echo "Setting up HAWQ service defn..."
echo "GOT API KEY " $1
mkdir /staging
chmod a+rx /staging
cd /staging
wget -O "hdb.tar.gz" --post-data="" --header="Authorization: Token $1" $PIV_NET_HDB
wget -O "hdb-ambari-plugin.tar.gz" --post-data="" --header="Authorization: Token $1" $PIV_NET_PLUGIN
wget -O "madlib.tar.gz" --post-data="" --header="Authorization: Token $1" $PIV_NET_MADLIB

#wget ${HDB_AMBARI_DOWNLOAD_LOC}
#tar -xvzf /staging/hdb-2.0.0.0-*.tar.gz -C /staging/
#tar -xvzf /staging/hdb-ambari-plugin-2.0.0-*.tar.gz -C /staging/
tar -xvzf /staging/hdb.tar.gz -C /staging/
tar -xvzf /staging/hdb-ambari-plugin.tar.gz -C /staging/
tar -xvzf /staging/madlib.tar.gz -C /staging/

yum install -y httpd
service httpd start
chkconfig httpd on
cd /staging/hdb*
./setup_repo.sh
cd /staging/hdb-ambari-plugin*
./setup_repo.sh
yum install -y hdb-ambari-plugin


#restart Ambari
echo "Restarting Ambari..."
service ambari-server restart
service ambari-agent restart
sleep 5
curl -u admin:admin -H  X-Requested-By:ambari http://localhost:8080/api/v1/hosts

#make VM look like sandbox
echo "Make VM look like sandbox..."
cd ~
#wget https://github.com/abajwa-hw/security-workshops/raw/master/scripts/startup-HDB.zip
wget https://github.com/dbbaskette/hdb-sandbox/raw/master/startup-HDB.zip
unzip startup-HDB.zip -d /
ln -s /usr/lib/hue/tools/start_scripts/startup_script /etc/init.d/startup_script
rm -f startup-HDB.zip
echo "vmware" > /virtualization

#boot in text only and remove rhgb
#plymouth-set-default-theme text
sed -i "s/rhgb//g" /boot/grub/grub.conf

#add startup_script and splash page to startup
echo "setterm -blank 0" >> /etc/rc.local
echo "/etc/rc.d/init.d/startup_script start" >> /etc/rc.local
echo "python /usr/lib/hue/tools/start_scripts/splash.py" >> /etc/rc.local


#provide custom configs for HAWQ, and HDFS proxy users
echo "Creating custom configs..."
cat << EOF > ~/ambari-bootstrap/deploy/configuration-custom.json
{
  "configurations" : {
    "hdfs-site": {
        "dfs.allow.truncate": "true",
        "dfs.block.access.token.enable": "false",
        "dfs.block.local-path-access.user": "gpadmin",
        "dfs.client.read.shortcircuit": "true",
        "dfs.client.socket-timeout": "300000000",
        "dfs.client.use.legacy.blockreader.local": "false",
        "dfs.datanode.handler.count": "60",
        "dfs.datanode.socket.write.timeout": "7200000",
        "dfs.namenode.handler.count": "600",
        "dfs.support.append": "true",
        "dfs.replication": "1"

    },
    "hawq-site":{
        "hawq_master_address_port":"10432"
    },
    "hdfs-client":{
        "dfs.default.replica":"1"
    },
    "hawq-env":{
        "hawq_password":"gpadmin",
        "vm.overcommit_memory":"2"

    },
    "core-site": {
        "hadoop.proxyuser.root.groups": "*",
        "hadoop.proxyuser.root.hosts": "*",
        "ipc.client.connection.maxidletime": "3600000",
        "ipc.client.connect.timeout": "300000",
        "ipc.server.listen.queue.size": "3300"
    }
  }
}
EOF

echo "Starting cluster install..."

#generate BP using Ambari recommendation API and deploy HDP
cd ~/ambari-bootstrap/deploy/
./deploy-recommended-cluster.bash
sleep 5

#wait until cluster deployed
source ~/ambari-bootstrap/extras/ambari_functions.sh
ambari_configs
ambari_wait_request_complete 1


##post install steps
cd ~

echo "Update Zeppelin configs for HAWQ"
curl -sSL https://gist.githubusercontent.com/abajwa-hw/0fd9772c916fac3fc5912f462168799a/raw | sudo -E python

echo "Downloading demo HAWQ demo notebook and restarting Zeppelin"
notebook_id=2BQPFYB1X
sudo -u zeppelin mkdir /usr/hdp/current/zeppelin-server/lib/notebook/$notebook_id
sudo -u zeppelin wget https://gist.githubusercontent.com/abajwa-hw/2f72d084dd1d0c5889783ecf0cd967ab/raw -O /usr/hdp/current/zeppelin-server/lib/notebook/$notebook_id/note.json
curl -u admin:$ambari_password -i -H 'X-Requested-By: zeppelin' -X PUT -d '{"RequestInfo": {"context" :"Stop ZEPPELIN via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://localhost:8080/api/v1/clusters/$cluster_name/services/ZEPPELIN
sleep 30
curl -u admin:$ambari_password -i -H 'X-Requested-By: zeppelin' -X PUT -d '{"RequestInfo": {"context" :"Start ZEPPELIN via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://localhost:8080/api/v1/clusters/$cluster_name/services/ZEPPELIN

echo "import data into hive"
cd /tmp
wget https://raw.githubusercontent.com/abajwa-hw/security-workshops/master/data/sample_07.csv

hive -e "CREATE TABLE sample_07 (
code string ,
description string ,
total_emp int ,
salary int )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TextFile; "

hive -e "load data local inpath '/tmp/sample_07.csv' into table sample_07;"

echo "import retail sample data from pivotal github"
cd /tmp
git clone https://github.com/pivotalsoftware/pivotal-samples.git
cd /tmp/pivotal-samples/sample-data/
sudo -u hdfs ./load_data_to_HDFS.sh
sudo -u hdfs hdfs dfs -chmod -R 777 /retail_demo
hive -f /tmp/pivotal-samples/hive/create_hive_tables.sql

echo "Configure local connections to HAWQ and reload HAWQ configs.."

ip=$(/sbin/ip -o -4 addr list eth0 | awk '{print $4}' | cut -d/ -f1)

echo "# File is generated from ${SCRIPT}" > /data/hawq/master/pg_hba.conf
echo "local    all         gpadmin         ident" >> /data/hawq/master/pg_hba.conf
echo "host     all         gpadmin         127.0.0.1/28    trust" >> /data/hawq/master/pg_hba.conf
echo "host all all ${ip}/32 trust" >> /data/hawq/master/pg_hba.conf


# ADD PG templates to .bashrc
sudo -u gpadmin bash -c "echo 'export PGPORT=10432' >> /home/gpadmin/.bashrc"
sudo -u gpadmin bash -c "source /usr/local/hawq/greenplum_path.sh; hawq stop cluster -a --reload"

echo "Installing MADlib"
sudo -u gpadmin bash -c "export PGPORT=10432;source /usr/local/hawq/greenplum_path.sh; createdb gpadmin"
sudo -u gpadmin bash -c "source /usr/local/hawq/greenplum_path.sh; gppkg -i /staging/madlib*.gppkg"
sudo -u gpadmin bash -c "source /usr/local/hawq/greenplum_path.sh; /usr/local/hawq/madlib/bin/madpack install -s madlib -p hawq -c gpadmin@sandbox:10432"
sudo -u gpadmin bash -c "source /usr/local/hawq/greenplum_path.sh; /usr/local/hawq/madlib/bin/madpack install -s madlib -p hawq -c gpadmin@sandbox:10432"

#Setup /etc/issue
echo -e "To login to the shell, use:\n----------------------\n   username: root\n   password: hadoop\n\nGPADMIN Credentials:\n----------------------\n   username: gpadmin\n   password: gpadmin\n" >> /etc/issue

echo "getting ready to export VM"
rm -f /etc/udev/rules.d/*-persistent-net.rules
sed -i '/^HWADDR/d'  /etc/sysconfig/network-scripts/ifcfg-eth0
sed -i '/^UUID/d'  /etc/sysconfig/network-scripts/ifcfg-eth0


echo "reduce VM size"
wget http://dev2.hortonworks.com.s3.amazonaws.com/stuff/zero_machine.sh
chmod +x zero_machine.sh
rm -rf /staging/*
rm -rf ~/ambari-bootsrap
./zero_machine.sh
/bin/rm -f zero_machine.sh





echo "Install is complete. Access Ambari on port 8080, Zeppelin on port 9995"
echo "A demo HAWQ notebook is available at http://VM_ADDRESS:9995/#/notebook/2BQPFYB1X"
echo "To take an export of this VM, shutdown and stop the VM first then export the .ova file by running below from on your local laptop (replace HDB_sandbox with the name of your VM). This will export the .ova file in your Mac's Downloads dir"
echo "/Applications/VMware\ Fusion.app/Contents/Library/VMware\ OVF\ Tool/ovftool --acceptAllEulas ~/Documents/Virtual\ Machines.localized/HDB_sandbox.vmwarevm/HDB_sandbox.vmx ~/Downloads/HDB_sandbox.ova"

exit