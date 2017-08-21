#!/usr/bin/env bash

# This script runs as personal user with sudo privs.

#userSetup(){
#    echo "Setting password for GPADMIN on all Nodes"
#    sudo echo $1|sudo passwd --stdin gpadmin
#    sudo echo $2|sudo passwd --stdin root#
#}


check_args() {
  if [ -z "$1" ]; then
    echo "Failed! Did not get number of drives"
    exit 1
  fi

  if [ -z "$2" ]; then
    echo "Failed! Did not get a value for RAID0"
    exit 1
  fi
}

setupDisk(){
# Write fstab file
sudo sh -c 'cat /etc/fstab >> /etc/ORIG.fstab'
sudo sh -c 'cat /tmp/fstab.cape >> /etc/fstab'
sudo yum -y install xfsprogs xfsdump

if [ "$2" == "no" ]; then
  echo "Setup $1 Disk/s with RAID0: $2"



  for d in $(seq 1 $1)
  do
    sudo mkdir -p /data/disk$d
  done
  cnt=1
  echo $1
  for c in {b..z}
  do
    echo $c
    sudo fdisk /dev/sd$c <<EOF
n
p
1
1

w
EOF
#sudo sh -c 'echo "LABEL=data$cnt /data/data$cnt xfs rw,noatime,inode64,allocsize=16m 0 0" >> /etc/fstab'
    echo $cnt
    echo "MAKEFS"
    echo "/dev/sd$c -L data$cnt"
    sudo mkfs.xfs -f /dev/sd$c -L data$cnt
    sudo echo deadline > /sys/block/sd$c/queue/scheduler
    sudo /sbin/blockdev --setra 16384 /dev/sd$c
    ((++cnt > $1)) && break
  done
fi

if [ "$2" == "yes" ]; then
  echo "Setup $1 Disk/s with RAID0: $2"
  # Calculate how many volumes to create
  echo "Calculating how many volumes to use"
  if [[ "$1" -lt 8 ]]; then
    VOLUMES=1
  elif [[ "$1" -lt 16 ]]; then
    VOLUMES=2
  else
    VOLUMES=4
  fi
  if (( "${1}" % "${VOLUMES}" != 0 )); then
    echo "Drive count ("${1}") not divisible by number of volumes ("${VOLUMES}"), using VOLUMES=1"
    VOLUMES=1
  fi
  echo "VOLUMES=$VOLUMES"

  DRIVES=($(ls /dev/sd[b-z]))
  DRIVE_COUNT=${#DRIVES[@]}

  sudo umount /dev/md[0-9]* || true

  sudo umount ${DRIVES[*]} || true

  sudo mdadm --stop /dev/md[0-9]* || true

  sudo mdadm --zero-superblock ${DRIVES[*]}

  for VOLUME in $(seq $VOLUMES); do
    DPV=$(expr "$DRIVE_COUNT" "/" "$VOLUMES")
    DRIVE_SET=($(ls /dev/sd[b-z] | head -n $(expr "$DPV" "*" "$VOLUME") | tail -n "$DPV"))
    sudo mdadm --create /dev/md${VOLUME} --run --level 0 --chunk 256K --raid-devices=${#DRIVE_SET[@]} ${DRIVE_SET[*]} --force
    sudo mkfs.xfs -f /dev/md${VOLUME}
    sudo mkdir -p /data${VOLUME}
  done
  sudo sh -c 'mdadm --detail --scan > /etc/mdadm.conf'
  sudo mount -a

fi

# Configure 50GB swap file on boot disk for all nodes
sudo fallocate -l 50g /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
# Configure deadline scheduler to survive reboots
sudo grubby --update-kernel=ALL --args="elevator=deadline"
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
    sudo yum -y install httpd java-1.8.0-openjdk java-1.8.0-openjdk-devel epel-release git python-argparse gcc gcc-c++
    sudo yum -y install python27 python27-python-devel python27-python-pip python27-python-setuptools python27-python-tools python27-python-virtualenv
    sudo yum -y install python-pip python-devel lapack-devel
    sudo yum -y install sshpass git iperf3 dstat flex


    sudo pip install pip -U
    sudo pip install sh
    sudo pip install setuptools -U

}

serverSetup(){
    echo "Setup RC.D"
    sudo sh -c "echo 'if test -f /sys/kernel/mm/transparent_hugepage/enabled; then echo never > /sys/kernel/mm/redhat_transparent_hugepage/enabled; fi' >> /etc/rc.local"
    sudo sh -c "echo 'if test -f /sys/kernel/mm/transparent_hugepage/defrag; then echo never > /sys/kernel/mm/redhat_transparent_hugepage/defrag; fi' >> /etc/rc.local"


}

installGcsfuse(){
	sudo tee /etc/yum.repos.d/gcsfuse.repo > /dev/null <<EOF
[gcsfuse]
name=gcsfuse (packages.cloud.google.com)
baseurl=https://packages.cloud.google.com/yum/repos/gcsfuse-el7-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg
       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOF
	sudo yum -y update
	sudo yum -y install gcsfuse
}

_main() {
    echo "prepareHost.sh received args: $@"
    check_args $1 $2
    securitySetup
    networkSetup
    setupDisk $1 $2
    installSoftware
    installGcsfuse
    serverSetup
}


_main "$@"
