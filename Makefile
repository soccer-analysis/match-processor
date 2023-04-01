install:
	rm -rf Pipfile.lock
	pipenv install -d

deploy:
	cdk deploy --require-approval never

backfill_match_ids:
	pipenv run python -m src.backfill_match_ids

scrape_match:
	pipenv run python -m src.scrape_match