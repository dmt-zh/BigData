#!/usr/bin/env bash

sudo apt-get update -y; \
sudo apt-get install -y -q \
    wget \
    vim \
    default-jre \
    python3-venv \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    kafkacat

sudo mkdir -p /etc/apt/keyrings;

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg;

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y;
sudo apt-get install docker-ce docker-ce-cli containerd.io -y -q;
sudo curl -L "https://github.com/docker/compose/releases/download/v2.6.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose;

sudo groupadd docker;
sudo usermod -aG docker $USER;

python3.12 -m venv env;
source env/bin/activate;
python3.12 -m pip install -r requirements.txt;
deactivate;
