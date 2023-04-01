from concurrent.futures import ThreadPoolExecutor, as_completed
import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import List, Dict, Tuple, Any

import boto3
import ujson

from src.env import DATA_LAKE_BUCKET


@dataclass
class DataLakeItem(ABC):
	@abstractmethod
	def _get_key(self) -> str:
		pass

	def __str__(self) -> str:
		as_dict = dataclasses.asdict(self)
		hidden_keys = self._hidden_keys()

		def _reducer(agg: Dict[str, Any], curr: Tuple[str, Any]) -> Dict[str, Any]:
			key, value = curr
			return {
				**agg,
				**({key: value} if key in hidden_keys or value is None else {})
			}

		return ujson.dumps(reduce(_reducer, as_dict.items(), {}))

	@staticmethod
	def _hidden_keys() -> List[str]:
		return []


class DataLake:
	def __init__(self):
		self.__bucket = boto3.resource('s3').Bucket(DATA_LAKE_BUCKET)

	def persist(self, items: List[DataLakeItem]) -> None:
		key_items_map: Dict[str, List[DataLakeItem]] = {}
		for item in items:
			key = item._get_key()
			key_items_map[key] = key_items_map.get(key, []) + [item]
		with ThreadPoolExecutor() as x:
			for future in as_completed([x.submit(self.__save, t) for t in key_items_map.items()]):
				future.result()

	def __save(self, key_items: Tuple[str, List[DataLakeItem]]):
		key, items = key_items
		print(f'Saving {key}')
		self.__bucket.put_object(Key=key, Body='\n'.join([str(x) for x in items]))
