login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 003576902480.dkr.ecr.us-east-1.amazonaws.com

build:
	docker build -t cdh-spark .

tag:
	docker tag cdh-spark:latest 003576902480.dkr.ecr.us-east-1.amazonaws.com/cdh-spark:latest

push_latest:
	docker push 003576902480.dkr.ecr.us-east-1.amazonaws.com/cdh-spark:latest

run_tests:
	docker run -it -v ~/cache/coursier:/root/.cache/coursier --network host --entrypoint="/bin/bash" cdh-spark:latest -c "sbt test"

test: build run_tests
build_push_ecr: login build tag push_latest

.PHONY: login build tag push_latest run_tests test build_push_ecr