from dataclasses import dataclass

from src.data_lake import DataLakeItem


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
		return f'matches/league_id={self.league_id}/season={self.season}/{self.id}.json'


@dataclass
class Player(DataLakeItem):
	id: int
	name: str
	team_id: int
	shirt_number: int
	is_gk: bool
	as_of_date: str

	def get_key(self) -> str:
		return f'players/{self.id}.json'
