#!/bin/bash

echo "restart httpd service so ganglia works"
sudo service httpd reload

echo "installing updates...."
sudo yum update -y

echo "installing docker..."
sudo amazon-linux-extras install docker

echo "restarting docker..."
sudo systemctl --now enable docker

echo "getting stable distro..."
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
   && curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.repo | sudo tee /etc/yum.repos.d/nvidia-docker.repo

echo "installing nvidia-docker dos.."
sudo yum install nvidia-docker2 -y

echo "cleaning cache..."
sudo yum clean expire-cache

echo "restarting docker!"
sudo systemctl restart docker

echo "nvidia docker tool set up complete"

echo "cluster set up complete"

