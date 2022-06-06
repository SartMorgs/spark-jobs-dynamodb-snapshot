import boto3
import json
import subprocess
import os.path
import sys
import decimal
import threading

from botocore.exceptions import ClientError

class SnapshotDynamoDB:

    def __init__(
        self,
        aws_region_name,
        table_name,
        total_table_segments,
        json_path
    ):
        # default configs
        self.json_path = json_path

        # aws info 
        self.region_name = aws_region_name

        # snapshot table info
        self.table_name = table_name
        self.total_table_segments = total_table_segments

        # dynamo
        self.dynamodb = boto3.resource('dynamodb', region_name=self.region_name)
        self.dynamodb_table = self.dynamodb.Table(self.table_name)

    def get_json_data_from_dynamodb_table(self, segment, total_segments):
        call_for_scan_amount = 0
        items_returned_amount = 0

        response = self.dynamodb_table.scan(
            Segment=segment,
            TotalSegments=total_segments
        )

        with open(f'{self.json_path}/{self.table_name}_{segment}_{call_for_scan_amount}.json', 'w') as f:
            json.dump(response['Items'], f, cls=DecimalEncoder)

        items_returned_amount += len(response['Items'])

        while 'LastEvaluatedKey' in response:
            call_for_scan_amount += 1
            try:
                response = self.dynamodb_table.scan(
                    Segment=segment,
                    TotalSegments=total_segments,
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )

                with open(f'{self.json_path}/{self.table_name}_{segment}_{call_for_scan_amount}.json', 'w') as f:
                    json.dump(response['Items'], f, cls=DecimalEncoder)

                items_returned_amount += len(response['Items'])
            except ClientError as e:
                print(e.response['Error']['Message'])

        print(f'Segment: {segment} | Register amount: {items_returned_amount}')


    def dynamodb_parallel_table_scan(self):
        if not os.path.exists(self.json_path):
            subprocess.call(f'mkdir -p {self.json_path}', shell=True)

        thread_list = []
        total_threads = int(self.total_table_segments)

        for thread_index in range(total_threads):
            thread = threading.Thread(target=self.get_json_data_from_dynamodb_table, args=(thread_index, total_threads))
            thread_list.append(thread)

        for thread in thread_list:
            thread.start()

        for thread in thread_list:
            thread.join()

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

if __name__ == '__main__':
    table_snapshot = SnapshotDynamoDB(
        aws_region_name=sys.argv[1],
        table_name=sys.argv[2],
        total_table_segments=sys.argv[3],
        json_path=sys.argv[4]
    )
    table_snapshot.dynamodb_parallel_table_scan()