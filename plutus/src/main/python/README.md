#Plutus

Package to create models that predict the market price (`minimum_bit_to_win`) for bidding opportunities.


## Docker Build

```shell
docker build -f Dockerfile.cpu -t plutus/training-cpu .
```

## Docker Run

```shell
docker run -it --rm --entrypoint=/bin/bash -t plutus/training-cpu:latest
```


## Build Package (?)

install `build`

```shell
pip install -U build
```

```shell
python -m build .
```