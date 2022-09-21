#Audience Extension

Package to create audience models for Geronimo.


## Docker Build

```shell
docker build -f Dockerfile.prod -t audience/training-unitary .
```

## Docker Run

```shell
docker run -it --rm --entrypoint=/bin/bash -t audience/training-unitary:latest
```


## Build Package (?)

install `build`

```shell
pip install -U build
```

```shell
python -m build .
```