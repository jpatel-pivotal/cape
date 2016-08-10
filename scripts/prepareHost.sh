#!/usr/bin/env bash

# This script runs as personal user with sudo privs.

#userSetup(){
#    echo "Setting password for GPADMIN on all Nodes"
#    sudo echo $1|sudo passwd --stdin gpadmin
#    sudo echo $2|sudo passwd --stdin root#
#}



setupDisk(){
echo "Setup Disk"
sudo yum -y install xfsprogs xfsdump
sudo fdisk /dev/sdb <<EOF
n
p
1
1

w
EOF
sudo mkdir /data
sudo sh -c "cat /tmp/fstab.cape >> /etc/fstab"
sudo mkfs.xfs -f /dev/sdb1 -L data
}

securitySetup(){
    echo "Disabling IP Tables"
    sudo /etc/init.d/iptables stop
    sudo /sbin/chkconfig iptables off
    echo "Disabling SELinux"
    sudo setenforce 0
    sudo sed -i "s/SELINUX=enforcing/SELINUX=disabled/" /etc/selinux/config
    sudo service sshd reload
}

networkSetup(){
    sudo sed -i 's|[#]*PasswordAuthentication no|PasswordAuthentication yes|g' /etc/ssh/sshd_config
    sudo sed -i 's|PermitRootLogin no|PermitRootLogin yes|g' /etc/ssh/sshd_config
        sudo sed -i 's|UsePAM no|UsePAM yes|g' /etc/ssh/sshd_config

    #sudo sh -c "echo 'Defaults \!requiretty' > /etc/sudoers.d/888-dont-requiretty"
    sudo sh -c "cat '/tmp/sysctl.conf.cape' >> /etc/sysctl.conf"
    sudo sh -c "cat '/tmp/limits.conf.cape' >> /etc/security/limits.conf"
}

installSoftware(){
    echo "Install Required Software"
    sudo yum -y install httpd java-1.8.0-openjdk java-1.8.0-openjdk-devel epel-release python-pip git python-argparse
    sudo pip install sh

}

serverSetup(){
    echo "Setup RC.D"
    sudo sh -c "echo 'if test -f /sys/kernel/mm/transparent_hugepage/enabled; then echo never > /sys/kernel/mm/redhat_transparent_hugepage/enabled; fi' >> /etc/rc.local"
    sudo sh -c "echo 'if test -f /sys/kernel/mm/transparent_hugepage/defrag; then echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag; fi' >> /etc/rc.local"


}







#installGPDBbins(){
#    wget https://storage.googleapis.com/pivedu-bins/$GPZIP
#    unzip $GPZIP
#    GPBIN="${GPZIP%.*}.bin"
#    sed -i 's/more <</cat <</g' ./$GPBIN
#    sed -i 's/agreed=/agreed=1/' ./$GPBIN
#    sed -i 's/pathVerification=/pathVerification=1/' ./$GPBIN
#    sed -i '/defaultInstallPath=/a installPath=${defaultInstallPath}' ./$GPBIN
#    sudo ./$GPBIN
#    sudo chown -R gpadmin: /usr/local/greenplum-db*
#
#}

#downloadExtensions(){
#    wget https://storage.googleapis.com/pivedu-bins/$MADLIB -O /tmp/$MADLIB
#    wget https://storage.googleapis.com/pivedu-bins/$PLR -O /tmp/$PLR
#
#    cd /tmp;tar xvfz /tmp/$MADLIB
#    sudo chown -R gpadmin: *.gppkg
#
#
#
#}



_main() {
    securitySetup
    networkSetup
    setupDisk
    installSoftware
    serverSetup



}


_main "$@"