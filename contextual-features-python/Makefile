
login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 003576902480.dkr.ecr.us-east-1.amazonaws.com

build:
	docker build -t cdh-emr-test .

tag:
	docker tag cdh-emr-test:latest 003576902480.dkr.ecr.us-east-1.amazonaws.com/cdh-emr-test:latest

push_latest:
	docker push 003576902480.dkr.ecr.us-east-1.amazonaws.com/cdh-emr-test:latest

run_local:
	docker run --publish 5000:5000 cdh-emr-test:latest .

run_tests:
	docker run -it -v $(shell pwd)/tmp:/code/tmp --network host --entrypoint="/bin/bash" cdh-emr-test:latest -c pytest -rx

run_test_file:
	docker run -it -v $(shell pwd):/code --network host --entrypoint="/bin/bash" cdh-emr-test:latest -c "pytest -k '$(pyfile)'"

build_pex:
	pex . --python=python3 --inherit-path=fallback -f https://download.pytorch.org/whl/torch_stable.html --requirement=requirements.txt -o env.pex

build_wheel:
	rm -rf build dist contextual_features.egg-info
	python -m build

test: build run_tests
test_single: build run_test_file
build_push_ecr: login build tag push_latest
build_pex_docker:
	docker build -t cdh-emr-test .
	docker run -it -v $(shell pwd):/code --entrypoint="/bin/bash" cdh-emr-test -c "pex . --python=python3 --inherit-path=fallback -f https://download.pytorch.org/whl/torch_stable.html --requirement=requirements.txt -o env.pex"


.PHONY: login build tag push_latest run_local run_tests run_test_file test test_single