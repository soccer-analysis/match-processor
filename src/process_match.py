import gzip
from typing import Dict, Any, List

import boto3
import ujson

from src.data_lake import DataLake, DataLakeItem
from src.env import DATA_LAKE_BUCKET
from src.models import Match, Player, Event, EventType, RosterPlayer


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
			is_gk=p['position'] == 'GK',
			as_of_date=match.date
		) for p in team['players']
	]


def get_players(raw: dict, match: Match) -> List[Player]:
	match_centre_data = raw['matchCentreData']
	return get_team_players(match_centre_data['home'], match) + get_team_players(match_centre_data['away'], match)


def get_events(raw: dict, match: Match) -> List[Event]:
	events: List[Event] = []
	for event in raw['matchCentreData']['events']:
		outcome_type_id = event.get('outcomeType', {}).get('value')
		events.append(Event(
			id=event['eventId'],
			timestamp=event['id'],
			league_id=match.league_id,
			season=match.season,
			match_id=match.id,
			player_id=event.get('playerId'),
			team_id=event['teamId'],
			half=event['period']['value'],
			half_minute=event['minute'],
			expanded_minute=event['expandedMinute'],
			second=event.get('second'),
			is_touch=event.get('isTouch', False),
			is_shot=event.get('isShot', False),
			x=float(event['x']),
			y=float(event['y']),
			type_id=event['type']['value'],
			outcome_type_id=event['outcomeType']['value'],
			end_x=event.get('endX'),
			end_y=event.get('endY'),
			blocked_x=event.get('blockedX'),
			blocked_y=event.get('blockedY'),
			goal_mouth_y=event.get('goalMouthY'),
			goal_mouth_z=event.get('goalMouthZ'),
			related_event_id=event.get('relatedEventId'),
			related_player_id=event.get('relatedPlayerId'),
			is_successful=None if outcome_type_id is None else outcome_type_id == 1
		))
	return events


def get_event_types(raw: dict) -> List[EventType]:
	event_type_map: Dict[int, str] = {}
	for event in raw['matchCentreData']['events']:
		event_type = event['type']
		event_type_map[event_type['value']] = event_type['displayName']
	return [
		EventType(id=_id, description=description)
		for _id, description in event_type_map.items()
	]


def get_team_roster(raw_team: dict, match: Match) -> List[RosterPlayer]:
	return [
		RosterPlayer(
			match_id=match.id,
			league_id=match.league_id,
			season=match.season,
			team_id=raw_team['teamId'],
			player_id=p['playerId'],
			started=p.get('isFirstEleven', False),
			replaced_by_player_id=p.get('subbedInPlayerId'),
			subbed_out_half=p.get('subbedOutPeriod', {}).get('value'),
			subbed_out_expanded_minute=p.get('subbedOutExpandedMinute'),
			replaced_player_id=p.get('subbedOutPlayerId'),
			subbed_in_half=p.get('subbedInPeriod', {}).get('value'),
			subbed_in_expanded_minute=p.get('subbedInExpandedMinute')
		)
		for p in raw_team['players']
	]


def get_team_rosters(raw: dict, match: Match) -> List[RosterPlayer]:
	match_centre_data = raw['matchCentreData']
	return get_team_roster(match_centre_data['home'], match) + get_team_roster(match_centre_data['away'], match)


def process(raw: dict) -> None:
	data_lake = DataLake()
	items: List[DataLakeItem] = []
	match = get_match(raw)
	items.append(match)
	items.extend(get_players(raw, match))
	items.extend(get_events(raw, match))
	items.extend(get_event_types(raw))
	items.extend(get_team_rosters(raw, match))
	data_lake.persist(items)


def unzip(bucket: str, key: str) -> dict:
	with gzip.GzipFile(fileobj=boto3.resource('s3').Object(bucket, key).get()['Body']) as gz:
		return ujson.loads(gz.read())


def lambda_handler(event: Dict = None, context: Any = None) -> None:
	for record in event['Records']:
		bucket = record['s3']['bucket']['name']
		key = record['s3']['object']['key']
		process(unzip(bucket, key))


if __name__ == '__main__':
	process(unzip(DATA_LAKE_BUCKET, 'raw/1643759.json.gzip'))
