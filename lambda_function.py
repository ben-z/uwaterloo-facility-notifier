import boto3
from datetime import datetime, timedelta
import dateutil.parser
import json
import pytz
import os
import requests

TZ = pytz.timezone('US/Eastern')
FACILITY_ID = "6f60aca9-7fba-4bf1-b6af-1f85e9376462" # CIF Arena
FACILITY_WEB_UI_URL_FORMATTER = "https://warrior.uwaterloo.ca/Facility/GetSchedule?facilityId={facilityId}"
CALENDAR_URL_FORMATTER = "https://warrior.uwaterloo.ca/Facility/GetScheduleCustomAppointments?selectedId={facilityId}&start={start}&end={end}"
LOOKAHEAD_DAYS = 7
LOOKAHEAD_TIME = timedelta(days=LOOKAHEAD_DAYS)
DISCORD_WEBHOOK_URLS = os.environ['DISCORD_WEBHOOK_URLS'].split(",")

DYNAMODB_TABLE_NAME = "uwaterloo-facility-notifier-db"

BOT_USERNAME = "sk8rgoose"
BOT_AVATAR_URL = "https://i.imgur.com/7OMGH86.png"

def filter_events(events):
    # Filter out events that are not for the CIF Arena
    return [e for e in events if "open rec" in e['title'].lower()]

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
        dynamodb_config['aws_access_key_id'] = os.environ.get('CUSTOM_AWS_ACCESS_KEY_ID')
    if os.environ.get('CUSTOM_AWS_SECRET_ACCESS_KEY'):
        dynamodb_config['aws_secret_access_key'] = os.environ.get('CUSTOM_AWS_SECRET_ACCESS_KEY')

    dynamodb_config['region_name'] = 'us-east-2'

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

def lambda_handler(event, context):
    # current time in the timezone TZ
    now = datetime.now(TZ)

    table = DynamoDBTable(DYNAMODB_TABLE_NAME)

    old_cal_entries = table.get('cal_entries') or []

    # beginning of day today
    start = now.strftime('%Y-%m-%dT00:00:00%z')
    # end of day in LOOKAHEAD_TIME days
    end = (now + LOOKAHEAD_TIME).strftime('%Y-%m-%dT23:59:59%z')

    # Make a request to the Warrior website to get the calendar data
    # Parse the data and return it to the client
    r = requests.get(CALENDAR_URL_FORMATTER.format(facilityId=FACILITY_ID,start=start,end=end))

    if r.status_code != 200:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error: could not get calendar data (status code {r.status_code})',
                'error': r.text,
            })
        }

    cal_entries = filter_events(r.json())

    deleted_upcoming_entries = [e for e in old_cal_entries if e not in cal_entries and dateutil.parser.parse(e['start']) > now.replace(tzinfo=None)]
    new_upcoming_entries = [e for e in cal_entries if e not in old_cal_entries and dateutil.parser.parse(e['start']) > now.replace(tzinfo=None)]

    changes = []

    # Send a message to Discord if there are any changes
    if deleted_upcoming_entries:
        changes.append(
            {
                "fields": [
                    {
                        "name": "Cancelled Open Rec Skate Sessions",
                        "value": "".join([f"{pretty_print_time_range(e['start'],e['end'])}\n" for e in deleted_upcoming_entries]),
                    },
                ],
                'color': '16711680', # red
                "timestamp": now.isoformat(),
            }
        )

    if new_upcoming_entries:
        changes.append(
            {
                'fields': [
                    {
                        "name": "New Open Rec Skate Sessions",
                        "value": "".join([f"{pretty_print_time_range(e['start'],e['end'])}\n" for e in new_upcoming_entries]),
                    }
                ],
                'color': '65280', # green
                "timestamp": now.isoformat(),
            }
        )

    errors = []
    if changes:
        for webhook_url in DISCORD_WEBHOOK_URLS:
            r = requests.post(webhook_url, json={
                "username": BOT_USERNAME,
                "avatar_url": BOT_AVATAR_URL,
                "embeds": [ {
                        "author": {
                            "name": f"{BOT_USERNAME} has an update!",
                            "icon_url": BOT_AVATAR_URL,
                        },
                    }
                ] + changes + [
                    {
                        "fields": [
                            {
                                "name": f"Open Rec Skate sessions at CIF in the next {LOOKAHEAD_DAYS} days",
                                "value": "".join(f"{pretty_print_time_range(e['start'],e['end'])}\n" for e in cal_entries),
                                "color": 1127128
                            },
                            {
                                "name": "",
                                "value": f"Check the [facility schedule]({FACILITY_WEB_UI_URL_FORMATTER.format(facilityId=FACILITY_ID)})",
                            },
                        ],
                        "timestamp": now.isoformat(),
                    },
                ],
            })
            if r.status_code != 204:
                errors.append({
                    'message': f'Error: could not send Discord message (status code {r.status_code}, webhook url {webhook_url})',
                    'error': r.text,
                })

    if old_cal_entries != cal_entries:
        table.put('cal_entries', cal_entries)

    if errors:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'errors': errors
            })
        }

    return {
        'statusCode': 200,
        'body': json.dumps({
            'num_old_cal_entries': len(old_cal_entries),
            'num_cal_entries': len(cal_entries),
            'old_cal_entries': old_cal_entries,
            'cal_entries': cal_entries,
        })
    }

if __name__ == "__main__":
    print(lambda_handler(None, None))