install:
	rm -rf Pipfile.lock
	pipenv install -d

deploy:
	cdk deploy --require-approval never