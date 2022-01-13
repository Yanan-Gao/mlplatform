FROM nvidia/cuda:11.4.2-cudnn8-runtime-ubuntu20.04
# NV_CUDNN_VERSION 8.2.4.15

# Disable NVIDIA repos to prevent accidental upgrades.
RUN cd /etc/apt/sources.list.d && \
    mv cuda.list cuda.list.disabled

# See https://github.com/databricks/containers/blob/master/ubuntu/minimal/Dockerfile
RUN apt-get update && \
    apt-get install --yes --no-install-recommends \
      openjdk-8-jdk-headless \
      iproute2 \
      bash \
      sudo \
      coreutils \
      procps \
      wget && \
    /var/lib/dpkg/info/ca-certificates-java.postinst configure && \
    rm -rf /var/lib/apt/lists/*

## Conda
ENV PATH /databricks/conda/bin:$PATH

# Update to latest p3.8 miniconda
RUN wget -q https://repo.continuum.io/miniconda/Miniconda3-py38_4.10.3-Linux-x86_64.sh -O miniconda.sh && \
    bash miniconda.sh -b -p /databricks/conda && \
    rm miniconda.sh && \
    # Source conda.sh for all login and interactive shells.
    ln -s /databricks/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /etc/profile.d/conda.sh" >> ~/.bashrc && \
    # Set always_yes for non-interactive shells.
    conda config --system --set always_yes True && \
    conda clean --all

COPY tensorflow.yml /tmp/env.yml

RUN conda env create --file /tmp/env.yml && \
    rm -f /tmp/env.yml && \
    rm -rf $HOME/.cache/pip/*

# Set an environment variable used by Databricks to decide which conda environment to activate by default.
ENV DEFAULT_DATABRICKS_ROOT_CONDA_ENV=tensorflow