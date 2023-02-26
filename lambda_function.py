from datetime import datetime, timedelta
import dateutil.parser
import json
import pytz
import os
import requests
import concurrent.futures
from typing import List
from utils import DynamoDBTable, ReqParam, EventConfig, pretty_print_time_range, flatten

TZ = pytz.timezone('US/Eastern')
FACILITY_WEB_UI_URL_FORMATTER = "https://warrior.uwaterloo.ca/Facility/GetSchedule?facilityId={facilityId}"
CALENDAR_URL_FORMATTER = "https://warrior.uwaterloo.ca/Facility/GetScheduleCustomAppointments?selectedId={facilityId}&start={start}&end={end}"
DISCORD_WEBHOOK_URLS = os.environ['DISCORD_WEBHOOK_URLS'].split(",")

DYNAMODB_TABLE_NAME = "uwaterloo-facility-notifier-db"

BOT_USERNAME = "sk8rgoose"
BOT_AVATAR_URL = "https://i.imgur.com/7OMGH86.png"


event_configs: List[EventConfig] = [
    EventConfig(**{
        'facility_id': '6f60aca9-7fba-4bf1-b6af-1f85e9376462',
        'facility_name': 'CIF Arena',
        'lookahead_days': 7,
        'event_name': 'Open Rec Skate',
        'event_filter': lambda e: 'open rec' in e['title'].lower(),
    }),
    EventConfig(**{
        'facility_id': '6f60aca9-7fba-4bf1-b6af-1f85e9376462',
        'facility_name': 'CIF Arena',
        'lookahead_days': 7,
        'event_name': 'Figure Skating Club',
        'event_filter': lambda e: 'figure skating' in e['title'].lower() and "club" in e['title'].lower() and "hold" not in e['title'].lower(),
    }),
]


def get_calendar_data(rp: ReqParam):
    """
    Get calendar data for a facility from the UWaterloo Warrior API
    Arguments:
        facility_id: the ID of the facility to get calendar data for
        start: the start datetime to get calendar data for, in the format YYYY-MM-DDTHH:MM:SS%z
        end: the end datetime to get calendar data for, in the format YYYY-MM-DDTHH:MM:SS%z
    """

    r = requests.get(CALENDAR_URL_FORMATTER.format(
        facilityId=rp.facility_id, start=rp.start, end=rp.end))

    if r.status_code != 200:
        raise Exception(
            f"Error: could not get calendar data for facility {rp.facility_id} (status code {r.status_code})")

    return r.json()


def get_event_changes(event_config: EventConfig, stored_cal_entries: List, current_cal_entries: List, now: datetime):
    event_name = event_config.event_name
    facility_name = event_config.facility_name

    deleted_upcoming_entries = [e for e in stored_cal_entries if e not in current_cal_entries and dateutil.parser.parse(
        e['start']) > now.replace(tzinfo=None)]
    new_upcoming_entries = [e for e in current_cal_entries if e not in stored_cal_entries and dateutil.parser.parse(
        e['start']) > now.replace(tzinfo=None)]

    changes = []

    if deleted_upcoming_entries:
        changes.append(
            {
                "fields": [
                    {
                        "name": f"Cancelled {event_name} Sessions at {facility_name}",
                        "value": "".join([f"{pretty_print_time_range(e['start'],e['end'])}\n" for e in deleted_upcoming_entries]),
                    },
                ],
                'color': '16711680',  # red
                "timestamp": now.isoformat(),
            }
        )

    if new_upcoming_entries:
        changes.append(
            {
                'fields': [
                    {
                        "name": f"New {event_name} Sessions at {facility_name}",
                        "value": "".join([f"{pretty_print_time_range(e['start'],e['end'])}\n" for e in new_upcoming_entries]),
                    }
                ],
                'color': '65280',  # green
                "timestamp": now.isoformat(),
            }
        )

    return changes


def send_discord_message(changes_list: List, event_configs: List[EventConfig], cal_entries_list: List[List], now: datetime):
    cal_entries_embeds = [
        {
            "fields": [
                {
                    "name": f"{c.event_name} sessions at {c.facility_name} in the next {c.lookahead_days} days",
                    "value": "".join(f"{pretty_print_time_range(e['start'],e['end'])}\n" for e in cal_entries),
                    "color": 1127128
                },
                {
                    "name": "",
                    "value": f"Check the [facility schedule]({FACILITY_WEB_UI_URL_FORMATTER.format(facilityId=c.facility_id)})",
                },
            ],
            "timestamp": now.isoformat(),
        }
        for c, cal_entries in zip(event_configs, cal_entries_list)
    ]

    errors = []
    for webhook_url in DISCORD_WEBHOOK_URLS:
        r = requests.post(webhook_url, json={
            "username": BOT_USERNAME,
            "avatar_url": BOT_AVATAR_URL,
            "embeds": [
                {
                    "author": {
                        "name": f"{BOT_USERNAME} has an update!",
                        "icon_url": BOT_AVATAR_URL,
                    },
                },
                *flatten(changes_list),
                *cal_entries_embeds,
            ],
        })
        if r.status_code != 204:
            errors.append({
                'message': f'Error: could not send Discord message (status code {r.status_code}, webhook url {webhook_url})',
                'error': r.text,
            })

    return errors


def strftime_start_of_day(t):
    return t.strftime('%Y-%m-%dT00:00:00%z')


def strftime_end_of_day(t):
    return t.strftime('%Y-%m-%dT23:59:59%z')


def get_stored_calendar_entries(event_config: EventConfig, table: DynamoDBTable):
    record_name = make_record_name(event_config)
    return table.get(record_name) or []


def make_record_name(event_config: EventConfig):
    return f"cal_entries_{event_config.facility_id}_{event_config.event_name}"


def filter_calendar_entries(event_config: EventConfig, cal_entries: List):
    return [e for e in cal_entries if event_config.event_filter(e)]


def lambda_handler(event, context):
    # current time in the timezone TZ
    now = datetime.now(TZ)

    table = DynamoDBTable(DYNAMODB_TABLE_NAME)

    # construct the parameters for all of the API requests
    start = strftime_start_of_day(now)
    req_params = [
        ReqParam(config.facility_id, start, strftime_end_of_day(
            now + timedelta(days=config.lookahead_days)))
        for config in event_configs
    ]

    # Make all of the API requests in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # deduplicate the requests and submit them to the executor
        calendar_data_futures = {p: executor.submit(
            get_calendar_data, p) for p in set(req_params)}
        stored_cal_entries_list_futures = [executor.submit(
            get_stored_calendar_entries, config, table) for config in event_configs]
        try:
            calendar_data = {p: f.result()
                             for p, f in calendar_data_futures.items()}
            stored_cal_entries_list = [f.result()
                                       for f in stored_cal_entries_list_futures]
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error: could not get calendar data',
                    'error': str(e),
                })
            }

    current_cal_entries_list = [filter_calendar_entries(
        c, calendar_data[p]) for c, p in zip(event_configs, req_params)]
    changes_list = [
        get_event_changes(c, stored, current, now)
        for c, stored, current
        in zip(event_configs, stored_cal_entries_list, current_cal_entries_list)
    ]

    errors = []
    if any(changes_list):
        # send a message to discord
        errors.extend(send_discord_message(
            changes_list, event_configs, current_cal_entries_list, now))

    if errors:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'errors': errors
            })
        }

    # store the new calendar entries
    for c, stored, current in zip(event_configs, stored_cal_entries_list, current_cal_entries_list):
        if current != stored:
            table.put(make_record_name(c), current)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Success', 'message_sent': any(changes_list)})
    }


if __name__ == "__main__":
    print(lambda_handler(None, None))
