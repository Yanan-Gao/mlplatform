#Kongming

Package to create conversion models for Geronimo.


## Docker Build

```shell
docker build -f Dockerfile.prod -t kongming/training-gpu .
```

## Docker Run

```shell
docker run -it --rm --entrypoint=/bin/bash -t kongming/training-gpu:latest
```


## Build Package (?)

install `build`

```shell
pip install -U build
```

```shell
python -m build .
```