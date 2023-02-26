import concurrent.futures
import json
from datetime import datetime, timedelta
from typing import List

import dateutil.parser
import requests

from discord_utils import send_discord_message
from telegram_utils import refresh_telegram_subscribers, send_telegram_updates
from utils import (CALENDAR_URL_FORMATTER, TZ, ChangeType, DynamoDBTable,
                   EventChanges, EventConfig, ReqParam, TimeRange,
                   strftime_end_of_day, strftime_start_of_day)

DYNAMODB_TABLE_NAME = "uwaterloo-facility-notifier-db"

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


def get_event_changes(event_config: EventConfig, stored_cal_entries: List, current_cal_entries: List, now: datetime) -> EventChanges:
    deleted_upcoming_entries = [e for e in stored_cal_entries if e not in current_cal_entries and dateutil.parser.parse(
        e['start']) > now.replace(tzinfo=None)]
    new_upcoming_entries = [e for e in current_cal_entries if e not in stored_cal_entries and dateutil.parser.parse(
        e['start']) > now.replace(tzinfo=None)]

    changes = EventChanges(event_config=event_config, changes={
        ChangeType.CANCELLED: [TimeRange(e['start'], e['end']) for e in deleted_upcoming_entries],
        ChangeType.NEW: [TimeRange(e['start'], e['end']) for e in new_upcoming_entries],
    })

    return changes


def get_stored_calendar_entries(event_config: EventConfig, table: DynamoDBTable):
    record_name = make_record_name(event_config)
    return table.get(record_name) or []


def make_record_name(event_config: EventConfig):
    return f"cal_entries_{event_config.facility_id}_{event_config.event_name}"


def filter_calendar_entries(event_config: EventConfig, cal_entries: List):
    return [e for e in cal_entries if event_config.event_filter(e)]


def has_changes(changes_list: List[EventChanges]):
    return any([sum(len(v) for v in changes.changes.values()) > 0 for changes in changes_list])


def lambda_handler(event, context):
    # current time in the timezone TZ
    now = datetime.now(TZ)

    table = DynamoDBTable(DYNAMODB_TABLE_NAME)

    telegram_subscribers = refresh_telegram_subscribers(table)

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
    # a list of changes per event
    changes_list = [
        get_event_changes(c, stored, current, now)
        for c, stored, current
        in zip(event_configs, stored_cal_entries_list, current_cal_entries_list)
    ]

    errors = []
    if has_changes(changes_list):
        # send notifications
        errors.extend(send_discord_message(
            changes_list, event_configs, current_cal_entries_list, now))
        errors.extend(send_telegram_updates(telegram_subscribers,
                      changes_list, event_configs, current_cal_entries_list))

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
        'body': json.dumps({'message': 'Success', 'has_changes': has_changes(changes_list)})
    }


if __name__ == "__main__":
    print(lambda_handler(None, None))
