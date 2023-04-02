import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import reduce
from typing import List, Dict, Tuple, Any, Callable, TypeVar, Optional

import boto3
import botocore
import ujson
from botocore.exceptions import ClientError

from src.env import DATA_LAKE_BUCKET


@dataclass
class DataLakeItem(ABC):
	@abstractmethod
	def get_key(self) -> str:
		pass

	def __str__(self) -> str:
		as_dict = dataclasses.asdict(self)
		hidden_keys = self._hidden_keys()

		def _reducer(agg: Dict[str, Any], curr: Tuple[str, Any]) -> Dict[str, Any]:
			key, value = curr
			return {
				**agg,
				**({} if key in hidden_keys or value is None else {key: value})
			}

		return ujson.dumps(reduce(_reducer, as_dict.items(), {}))

	@staticmethod
	def parse(_json: str) -> 'DataLakeItem':
		DataLakeItem(**{})

	def should_save(self, data_lake: 'DataLake') -> bool:
		return True

	@staticmethod
	def _hidden_keys() -> List[str]:
		return []


T = TypeVar('T', bound=DataLakeItem)


class DataLake:
	def __init__(self):
		self.__resource = boto3.resource('s3')
		self.__bucket = self.__resource.Bucket(DATA_LAKE_BUCKET)
		self.__client = boto3.client('s3')

	def exists(self, key: str) -> bool:
		try:
			self.__client.head_object(Bucket=DATA_LAKE_BUCKET, Key=key)
		except botocore.exceptions.ClientError as e:
			if e.response['Error']['Code'] == '404':
				return False
			raise e
		return True

	def get(self, key: str, klass: Callable[[Any], T]) -> Optional[T]:
		try:
			content = self.__resource.Object(DATA_LAKE_BUCKET, key).get()['Body'].read().decode('utf-8')
			return klass(**(ujson.loads(content)))
		except ClientError as e:
			if e.response['Error']['Code'] == 'NoSuchKey':
				return None
			raise e

	def persist(self, items: List[DataLakeItem]) -> None:
		key_items_map: Dict[str, List[DataLakeItem]] = {}
		for item in items:
			key = item.get_key()
			key_items_map[key] = key_items_map.get(key, []) + [item]
		for key, items in key_items_map.items():
			self.__save(key, items)

	def __save(self, key: str, items: List[DataLakeItem]) -> None:
		if any(not x.should_save(self) for x in items):
			print(f'Not saving {key}')
			return
		print(f'Saving {key}')
		self.__bucket.put_object(Key=key, Body='\n'.join([str(x) for x in items]))
