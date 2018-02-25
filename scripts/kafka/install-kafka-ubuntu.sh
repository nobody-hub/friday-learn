#!/usr/bin/env bash

#######
# Java
#######
# Update Repository
sudo apt-get update -y
sudo apt-get upgrade -y

# Add a Personal Package Archives (PPA)
sudo add-apt-repository -y ppa:webupd8team/java

# Update the package database
sudo apt-get update -y

# Install the Oracle JDK
sudo apt-get install oracle-java8-set-default -y

# Print Java Version
sudo java -version

#######
# ZooKeeper
#######
# Install ZooKeeper to maintain configuration information, provide distributed synchronization, naming and provide group services.
sudo apt-get install zookeeperd -y

# Start server
/usr/share/zookeeper/bin/zkServer.sh start

# Check server status
/usr/share/zookeeper/bin/zkServer.sh status

# Check ZooKeeper
netstat -ant | grep :2181

# Check Zookeeper
telnet localhost 2181
srvr

# Check the configuration, location given by `dpkg -L zookeeper`
cat /etc/zookeeper/conf/zoo.cfg

#######
# Kafka
#######
# Download installation package
wget http://www-eu.apache.org/dist/kafka/1.0.0/kafka_2.11-1.0.0.tgz

# Create folder for installation
sudo mkdir /opt/Kafka

# Extract to download archive
sudo tar -xvf kafka_2.11-1.0.0.tgz -C /opt/Kafka/

# Start Kafka Server
#sudo  /opt/Kafka/kafka_2.11-1.0.0/bin/kafka-server-start.sh /opt/Kafka/kafka_2.10-0.10.0.1/config/server.properties