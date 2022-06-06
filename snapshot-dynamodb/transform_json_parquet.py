import decimal
import sys
import json
import subprocess
import datetime
import boto3
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_extract, lower

class SubscriptionTransformData:

    def __init__(
        self,
        aws_region_name,
        json_path,
        s3_output_path,
        database_name,
        table_name
    ):
        self.json_path = json_path
        self.s3_output_path = s3_output_path
        self.reference_date = datetime.date.today() - datetime.timedelta(1)

        self.athena_client = boto3.client('athena', region_name=aws_region_name)
        self.s3_output_query = 's3://bi-dl-artifacts/query-results/'

        self.database_name = database_name
        self.table_name = table_name

    def extract_nested_data(self):
        source = f'{self.json_path}/*.json'
        targget = f'/{self.table_name}/json'
        subprocess.call(f'sudo -H -u hadoop bash -c "hdfs dfs -mkdir -p /{self.table_name}/json"', shell=True)
        subprocess.call(f'sudo -H -u hadoop bash -c "hadoop fs -put {source} {targget}"', shell=True)
        subprocess.call(f'sudo find {source} -type f -delete', shell=True)
        print(f'Dados importados para o HDFS com pasta de trabalho /json')

        spark = SparkSession.builder \
            .appName('Snapshot for dynamodb table') \
            .config('spark.sql.hive.convertMetastoreParquet', 'false') \
            .config('spark.sql.hive.caseSensitiveInferenceMode', 'INFER_AND_SAVE') \
            .config('spark.some.config.option', True) \
            .enableHiveSupport() \
            .getOrCreate()

        json_hdfs_path = f'/{self.table_name}/json/*.json'

        df_spark = spark.read.json(json_hdfs_path)

        df_data_columns = df_spark.withColumn("year", lit(str(self.reference_date.year).zfill(4))) \
            .withColumn("month", lit(str(self.reference_date.month).zfill(2))) \
            .withColumn("day", lit(str(self.reference_date.day).zfill(2)))

        print('Reparticionando em um arquivo')
        df_repartition = df_data_columns.repartition(1)

        df_repartition.write.mode('append').format('parquet').partitionBy("year", "month", "day").save(self.s3_output_path)
        print(f'Saved parquet files at {self.s3_output_path}')

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
    subscription_transform = SubscriptionTransformData(
        aws_region_name=sys.argv[1],
        json_path=sys.argv[2],
        s3_output_path=sys.argv[3],
        database_name=sys.argv[4],
        table_name=sys.argv[5]
    )
    subscription_transform.extract_nested_data()