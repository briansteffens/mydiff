#!/bin/bash

apt-get update
apt-get install -y squid-deb-proxy-client

# python
apt-get install -y python3-pip
pip3 install pymysql

# set python path to base of repository
echo 'PYTHONPATH="/vagrant"' >> /home/vagrant/.bashrc
echo 'export PYTHONPATH' >> /home/vagrant/.bashrc

# mysql-server
echo mysql-server mysql-server/root_password password rootpass | sudo debconf-set-selections
echo mysql-server mysql-server/root_password_again password rootpass | sudo debconf-set-selections
apt-get install -y mysql-server
mysql --user=root --password=rootpass --execute="drop database if exists mydiff1; create database mydiff1;"
mysql --user=root --password=rootpass mydiff1 < "/vagrant/vagrant/mydiff1.sql"
mysql --user=root --password=rootpass --execute="drop database if exists mydiff2; create database mydiff2;"
mysql --user=root --password=rootpass mydiff2 < "/vagrant/vagrant/mydiff2.sql"
