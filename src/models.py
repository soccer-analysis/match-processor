from dataclasses import dataclass
from typing import List, Optional

from src.data_lake import DataLakeItem, DataLake


@dataclass
class Match(DataLakeItem):
	id: int
	league_id: int
	season: int
	date: str
	home_id: int
	home_score: int
	away_id: int
	away_score: int

	def get_key(self) -> str:
		return f'matches/league_id={self.league_id}/season={self.season}/id={self.id}.json'


@dataclass
class Player(DataLakeItem):
	id: int
	name: str
	team_id: int
	is_gk: bool
	as_of_date: str

	def get_key(self) -> str:
		return f'players/id={self.id}.json'

	def should_save(self, data_lake: DataLake) -> bool:
		existing_player = data_lake.get(self.get_key(), Player)
		if not existing_player:
			return True
		return self.as_of_date > existing_player.as_of_date


@dataclass
class RosterPlayer(DataLakeItem):
	match_id: int
	league_id: int
	season: int
	team_id: int
	player_id: int
	started: bool
	replaced_by_player_id: Optional[int]
	subbed_out_half: Optional[int]
	subbed_out_expanded_minute: Optional[int]
	replaced_player_id: Optional[int]
	subbed_in_half: Optional[int]
	subbed_in_expanded_minute: Optional[int]

	def get_key(self) -> str:
		return f'rosters/league_id={self.league_id}/season={self.season}/match_id={self.match_id}.jsonl'

	@staticmethod
	def _hidden_keys() -> List[str]:
		return ['league_id', 'season']


@dataclass
class Event(DataLakeItem):
	id: int
	timestamp: float
	league_id: int
	season: int
	match_id: int
	player_id: Optional[int]
	team_id: int
	half: int
	half_minute: int
	expanded_minute: int
	is_touch: bool
	is_shot: bool
	x: float
	y: float
	type_id: int
	outcome_type_id: int
	second: Optional[int]
	end_x: Optional[float]
	end_y: Optional[float]
	blocked_x: Optional[float]
	blocked_y: Optional[float]
	goal_mouth_y: Optional[float]
	goal_mouth_z: Optional[float]
	related_event_id: Optional[int]
	related_player_id: Optional[int]
	is_successful: Optional[bool]

	def get_key(self) -> str:
		return f'events/league_id={self.league_id}/season={self.season}/match_id={self.match_id}.jsonl'

	@staticmethod
	def _hidden_keys() -> List[str]:
		return ['league_id', 'season', 'match_id']


@dataclass
class EventType(DataLakeItem):
	id: int
	description: str

	def get_key(self) -> str:
		return f'event_types/id={self.id}.json'

	def should_save(self, data_lake: DataLake) -> bool:
		return not data_lake.exists(self.get_key())
