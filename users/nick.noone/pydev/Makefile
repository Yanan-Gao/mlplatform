
login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 003576902480.dkr.ecr.us-east-1.amazonaws.com

build:
	docker build -t nvn-emr-test .

tag:
	docker tag nvn-emr-test:latest 003576902480.dkr.ecr.us-east-1.amazonaws.com/nvn-emr-test:latest

push_latest:
	docker push 003576902480.dkr.ecr.us-east-1.amazonaws.com/nvn-emr-test:latest

build_push_ecr: login build tag push_latest

run_local:
	docker run --publish 5000:5000 nvn-emr-test:latest .


.PHONY: login build tag push_latest run_local