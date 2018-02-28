#!/usr/bin/env bash

#######
# Install
#
# https://docs.confluent.io/current/installation/installing_cp.html#id4
#######

# Install the Confluent public key
wget -qO - https://packages.confluent.io/deb/4.0/archive.key | sudo apt-key add -

# Add the repository to your `/etc/apt/sources.list`
sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/4.0 stable main"

# Install Confluent  Enterprise
#sudo apt-get update && sudo apt-get install confluent-platform-2.11 -y

# Install Confluent Open Source
sudo apt-get update && sudo apt-get install confluent-platform-oss-2.11 -y

#######
# Quick Start
#
# https://docs.confluent.io/current/quickstart.html#quickstart
#######

# Start ZooKeeper, Kafka and Schema Registry
#confluent start schema-registry



