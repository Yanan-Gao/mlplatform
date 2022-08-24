#!/bin/bash



echo "installing updates.... \n"
sudo yum update -y

echo "installing docker... \n"
sudo amazon-linux-extras install docker

echo "restarting docker... \n"
sudo systemctl --now enable docker

echo "getting stable distro... \n"
distribution=$(. /etc/os-release;echo $ID$VERSION_ID) \
   && curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.repo | sudo tee /etc/yum.repos.d/nvidia-docker.repo

echo "installing nvidia-docker dos.. \n"
sudo yum install nvidia-docker2 -y

echo "cleaning cache... \n"
sudo yum clean expire-cache

echo "restarting docker! \n"
sudo systemctl restart docker

echo "nvidia docker tool set up complete \n starting s3 sync.. \n"

echo "set up complete"

