from typing import Optional

from dotenv import load_dotenv
from os import environ

load_dotenv()

BUCKET: str = environ.get('BUCKET')
MATCH_ID_QUEUE_URL: str = environ.get('MATCH_ID_QUEUE_URL')
