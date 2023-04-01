from typing import Dict, Any

def lambda_handler(event: Dict = None, context: Any = None) -> None:
	print(event)
