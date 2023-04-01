import concurrent.futures
import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Tuple

import boto3
import ujson

from src.env import DATA_LAKE_BUCKET


@dataclass
class DataLakeItem(ABC):
	@abstractmethod
	def get_key(self) -> str:
		pass

	def __str__(self) -> str:
		return ujson.dumps(dataclasses.asdict(self))


class DataLake:
	def __init__(self):
		self.__bucket = boto3.resource('s3').Bucket(DATA_LAKE_BUCKET)

	def persist(self, items: List[DataLakeItem]) -> None:
		key_items_map: Dict[str, List[DataLakeItem]] = {}
		for item in items:
			key = item.get_key()
			key_items_map[key] = key_items_map.get(key, []) + [item]

		with concurrent.futures.ThreadPoolExecutor() as executor:
			futures = [executor.submit(self.__save, (key, key_items)) for key, key_items in key_items_map.items()]
			for future in concurrent.futures.as_completed(futures):
				futures.append(future.result())

	def __save(self, key_items: Tuple[str, List[DataLakeItem]]):
		key, items = key_items
		print(f'Saving {key}')
		self.__bucket.put_object(Key=key, Body='\n'.join([str(x) for x in items]))
