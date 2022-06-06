import boto3
import subprocess
import sys
import time
import datetime

import awswrangler as wr

class DaySnapshot:

	def __init__(
		self,
		aws_region_name,
		s3_snapshot_bucket,
		s3_day_snapshot_bucket,
		snapshot_bucket_key,
		day_snapshot_bucket_key,
		database_name,
		table_name
	):		
		self.aws_region_name = aws_region_name
		self.s3_resource = boto3.resource('s3', region_name=self.aws_region_name)
		self.athena_client = boto3.client('athena', region_name=self.aws_region_name)

		self.s3_output_query = 's3://bi-dl-artifacts/query-results/'

		self.s3_snapshot_bucket = s3_snapshot_bucket
		self.s3_day_snapshot_bucket = s3_day_snapshot_bucket
		self.snapshot_bucket_key = snapshot_bucket_key
		self.day_snapshot_bucket_key = day_snapshot_bucket_key
		self.database_name = database_name
		self.table_name = table_name

		self.reference_date = datetime.date.today() - datetime.timedelta(1)

	def save_day_snapshot(self):
		try:
			objects_to_delete = self.s3_resource.meta.client.list_objects(Bucket=self.s3_day_snapshot_bucket, Prefix=self.day_snapshot_bucket_key)
			delete_keys = {'Objects': []}
			delete_keys['Objects'] = [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]
			self.s3_resource.meta.client.delete_objects(Bucket=self.s3_day_snapshot_bucket, Delete=delete_keys)
			print(f'Deleted data from s3://{self.s3_day_snapshot_bucket}/{self.day_snapshot_bucket_key}')
		except:
			print(f'File not found')

		last_snapshot_folder = f'{self.snapshot_bucket_key}/year={str(self.reference_date.year).zfill(4)}/month={str(self.reference_date.month).zfill(2)}/day={str(self.reference_date.day).zfill(2)}'
		folder = self.s3_resource.meta.client.list_objects_v2(
			Bucket=self.s3_snapshot_bucket,
			Prefix=last_snapshot_folder
		)
		parquet_files = folder['Contents']
		for file in parquet_files:
			copy_source = {
				'Bucket': self.s3_snapshot_bucket,
				'Key': file['Key']
			}
			target_key = f'{self.day_snapshot_bucket_key}/data.c000.snappy.parquet'
			self.s3_resource.meta.client.copy(copy_source, self.s3_day_snapshot_bucket, target_key)

		print(f'Copied all data from s3://{self.s3_snapshot_bucket}/{self.snapshot_bucket_key} to s3://{self.s3_day_snapshot_bucket}/{self.day_snapshot_bucket_key}')

		query_to_load_partition = f'MSCK REPAIR TABLE {self.database_name}.{self.table_name}'
		response = self.athena_client.start_query_execution(
			QueryString = (query_to_load_partition),
			QueryExecutionContext={
				'Database': self.database_name
			},
			ResultConfiguration={
				'OutputLocation': self.s3_output_query,
			}
		)
			
		query_status = None
		while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
			query_status = self.athena_client.get_query_execution(QueryExecutionId=response["QueryExecutionId"])['QueryExecution']['Status']['State']
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				raise Exception(f'Athena query with the string {query_to_load_partition} failed or was cancelled')
				failed_reason = self.athena_client.get_query_execution(response['QueryExecutionId'])['QueryExecution']['Status']['StateChangeReason']
				raise Exception(f'Failed reason: {failed_reason}')
			time.sleep(2)
		print(f'Load partition finished.')


if __name__ == '__main__':
	day_snapshot = DaySnapshot(
		aws_region_name=sys.argv[1],
		s3_snapshot_bucket=sys.argv[2],
		s3_day_snapshot_bucket=sys.argv[3],
		snapshot_bucket_key=sys.argv[4],
		day_snapshot_bucket_key=sys.argv[5],
		database_name=sys.argv[6],
		table_name=sys.argv[7]
	)
	day_snapshot.save_day_snapshot()
