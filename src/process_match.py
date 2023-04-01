import gzip
from typing import Dict, Any, List

import boto3
import ujson

from src.data_lake import DataLake, DataLakeItem
from src.env import DATA_LAKE_BUCKET
from src.models import Match, Player


def get_match(raw: dict) -> Match:
	match_centre_data = raw['matchCentreData']
	home_team = match_centre_data['home']
	away_team = match_centre_data['away']
	return Match(
		id=raw['matchId'],
		league_id=raw['league_id'],
		season=raw['season'],
		date=match_centre_data['startDate'].split('T')[0],
		home_id=home_team['teamId'],
		home_score=home_team['scores']['fulltime'],
		away_id=away_team['teamId'],
		away_score=away_team['scores']['fulltime']
	)


def get_team_players(team: dict, match) -> List[Player]:
	return [
		Player(
			id=p['playerId'],
			name=p['name'],
			team_id=team['teamId'],
			shirt_number=p['shirtNo'],
			is_gk=p['position'] == 'GK',
			as_of_date=match.date
		) for p in team['players']
	]


def get_players(raw: dict, match: Match) -> List[Player]:
	match_centre_data = raw['matchCentreData']
	return get_team_players(match_centre_data['home'], match) + get_team_players(match_centre_data['away'], match)


def process(raw: dict) -> None:
	data_lake = DataLake()
	data_lake_items: List[DataLakeItem] = []
	match = get_match(raw)
	data_lake_items.append(match)
	data_lake_items.extend(get_players(raw, match))
	data_lake.persist(data_lake_items)


def unzip(bucket: str, key: str) -> dict:
	with gzip.GzipFile(fileobj=boto3.resource('s3').Object(bucket, key).get()['Body']) as gz:
		return ujson.loads(gz.read())


def lambda_handler(event: Dict = None, context: Any = None) -> None:
	for record in event['Records']:
		bucket = record['s3']['bucket']['name']
		key = record['s3']['object']['key']
		process(unzip(bucket, key))


if __name__ == '__main__':
	process(unzip(DATA_LAKE_BUCKET, 'raw/1643222.json.gzip'))
