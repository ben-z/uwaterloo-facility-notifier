import boto3
from dataclasses import dataclass
import os
import dateutil.parser


@dataclass(frozen=True)
class EventConfig():
    """
    Data structure for holding the configuration for a single event type
    """
    facility_id: str
    facility_name: str
    lookahead_days: int
    event_name: str
    event_filter: callable


@dataclass(frozen=True)
class ReqParam():
    """
    Data structure for holding the parameters for API requests to the UWaterloo Warrior API
    """
    facility_id: str
    start: str
    end: str


def flatten(l):
    return [item for sublist in l for item in sublist]


def pretty_print_time_range(start: str, end: str):
    """
    Pretty print a time range in the format:
        Mon Jan 01 12:00PM - 01:00PM
    or
        Mon Jan 01 12:00PM - Tue Jan 02 01:00PM
    """

    start = dateutil.parser.parse(start)
    end = dateutil.parser.parse(end)

    start_date = start.date()
    end_date = end.date()

    if start_date == end_date:
        return f"{start.strftime('%a %b %d %I:%M%p')} - {end.strftime('%I:%M%p')}"
    else:
        return f"{start.strftime('%a %b %d %I:%M%p')} - {end.strftime('%a %b %d %I:%M%p')}"


def get_dynamodb_config():
    dynamodb_config = {}

    if os.environ.get('CUSTOM_AWS_ACCESS_KEY_ID'):
        dynamodb_config['aws_access_key_id'] = os.environ.get(
            'CUSTOM_AWS_ACCESS_KEY_ID')
    if os.environ.get('CUSTOM_AWS_SECRET_ACCESS_KEY'):
        dynamodb_config['aws_secret_access_key'] = os.environ.get(
            'CUSTOM_AWS_SECRET_ACCESS_KEY')

    dynamodb_config['region_name'] = 'us-east-1'

    return dynamodb_config


class DynamoDBTable:
    def __init__(self, table_name):
        self.dynamodb_config = get_dynamodb_config()

        self.dynamodb = boto3.resource('dynamodb', **self.dynamodb_config)
        self.table_name = table_name
        self.create_table()
        self.table = self.dynamodb.Table(self.table_name)

    def create_table(self):
        try:
            self.dynamodb.create_table(**{
                "TableName": self.table_name,
                "AttributeDefinitions": [
                    {
                        "AttributeName": "id",
                        "AttributeType": "S"
                    }
                ],
                "KeySchema": [
                    {
                        "AttributeName": "id",
                        "KeyType": "HASH"
                    }
                ],
                "BillingMode": 'PROVISIONED',
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1
                },
                "TableClass": "STANDARD",
            })

            self.dynamodb.meta.client.get_waiter('table_exists').wait(
                TableName=self.table_name)
        except self.dynamodb.meta.client.exceptions.ResourceInUseException:
            # Table already exists
            pass

    def get(self, id):
        response = self.table.get_item(Key={'id': id})
        if 'Item' in response:
            return response['Item']['value']
        else:
            return None

    def put(self, id, value):
        self.table.put_item(Item={'id': id, 'value': value})

    def delete(self, id):
        self.table.delete_item(Key={'id': id})
