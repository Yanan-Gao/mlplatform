
login:
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 003576902480.dkr.ecr.us-east-1.amazonaws.com

build:
	docker build -t plutus-training:plutus-latest .

tag:
	docker tag plutus-training:latest 003576902480.dkr.ecr.us-east-1.amazonaws.com/plutus-training:latest

push_latest:
	docker push 003576902480.dkr.ecr.us-east-1.amazonaws.com/nvn-emr-test:plutus-latest

build_push_ecr: login build tag push_latest

run_local:
	docker run --publish 5000:5000 plutus-training:plutus-latest .


.PHONY: login build tag push_latest run_local