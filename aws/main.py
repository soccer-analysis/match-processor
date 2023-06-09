from aws_cdk import Stack, App
from aws_cdk.aws_s3 import Bucket, EventType, NotificationKeyFilter
from aws_cdk.aws_s3_notifications import LambdaDestination
from constructs import Construct
from shared_infrastructure import create_function, get_stack_output
from shared_infrastructure.function import Allow


class MatchProcessorStack(Stack):
	def __init__(self, scope: Construct):
		super().__init__(scope, 'soccer-analysis-match-processor')
		data_lake_bucket_name = get_stack_output('soccer-analysis-shared-infrastructure', 'data-lake-bucket')
		data_lake_bucket_arn = get_stack_output('soccer-analysis-shared-infrastructure', 'data-lake-bucket-arn')

		process_match = create_function(
			self,
			name='process-match',
			cmd='src.process_match.lambda_handler',
			env={
				'DATA_LAKE_BUCKET': data_lake_bucket_name
			},
			memory_size=256,
			reserved_concurrent_executions=100,
			allows=[
				Allow(
					actions=['s3:GetObject', 's3:ListBucket', 's3:PutObject'],
					resources=[data_lake_bucket_arn, f'{data_lake_bucket_arn}/*']
				)
			]
		)

		data_lake_bucket = Bucket.from_bucket_name(self, 'data-lake-bucket', data_lake_bucket_name)

		data_lake_bucket.add_event_notification(EventType.OBJECT_CREATED, LambdaDestination(process_match),
												NotificationKeyFilter(prefix='raw/', suffix='.json.gzip'))


if __name__ == '__main__':
	app = App()
	MatchProcessorStack(app)
	app.synth()
