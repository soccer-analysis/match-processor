from dotenv import load_dotenv
from os import environ

load_dotenv()

DATA_LAKE_BUCKET: str = environ.get('DATA_LAKE_BUCKET')
