#!/bin/bash

# Prepare machine
# sudo yum -y update
# sudo yum -y install pkgconfig
# sudo yum -y install vulkan
# sudo yum -y upgrade kernel

BASE_URL=https://us.download.nvidia.com/tesla

# Check if nvidia-smi command exists
if ! command -v nvidia-smi &> /dev/null; then
    echo "nvidia-smi command not found. NVIDIA driver status cannot be checked."
else
    # Set default DRIVER_VERSION if not provided as an argument
    if [ -z "$1" ]; then
        DRIVER_VERSION=440.95.01  # Default version
    else
        DRIVER_VERSION="$1"
    fi

    # Command to get the current installed NVIDIA driver version (example command)
    current_version=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | awk '{print $1}')

    if [ -z "$current_version" ]; then
        echo "NVIDIA driver not installed or not found."
    else

        echo "Current NVIDIA driver version: $current_version"

        # Compare current version with DRIVER_VERSION
        if [ "$(printf '%s\n' "$DRIVER_VERSION" "$current_version" | sort -V | head -n1)" != "$DRIVER_VERSION" ]; then
            echo "Updating NVIDIA driver to version $DRIVER_VERSION"

            # Download the new driver
            wget "$BASE_URL/$DRIVER_VERSION/NVIDIA-Linux-x86_64-$DRIVER_VERSION.run"

            # Uninstall the existing driver
            sudo sh ./NVIDIA-Linux-x86_64-$DRIVER_VERSION.run --uninstall --silent

            # Install the new driver
            sudo sh ./NVIDIA-Linux-x86_64-$DRIVER_VERSION.run --install-libglvnd --silent #--no-drm --disable-nouveau --dkms --silent --install-libglvnd
        else
            echo "No need to update. Current driver version is up-to-date."
        fi
    fi
    
fi