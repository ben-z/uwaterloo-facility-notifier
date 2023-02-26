import concurrent.futures
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Mapping, Set, Union

import dateutil.parser
import requests
from telegram.constants import ChatMemberStatus

from utils import (CALENDAR_URL_FORMATTER, FACILITY_WEB_UI_URL_FORMATTER,
                   ChangeType, DynamoDBTable, EventChanges, EventConfig,
                   ReqParam, TimeRange, flatten, pretty_print_time_range)

DISCORD_WEBHOOK_URLS = os.environ['DISCORD_WEBHOOK_URLS'].split(",")

BOT_USERNAME = "sk8rgoose"
BOT_AVATAR_URL = "https://i.imgur.com/7OMGH86.png"


def send_discord_message(changes_list: List[EventChanges], event_configs: List[EventConfig], cal_entries_list: List[List], now: datetime):
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
                *flatten(format_changes_for_discord(changes, now)
                         for changes in changes_list),
                *cal_entries_embeds,
            ],
        })
        if r.status_code != 204:
            errors.append({
                'message': f'Error: could not send Discord message (status code {r.status_code}, webhook url {webhook_url})',
                'error': r.text,
            })

    return errors


def format_changes_for_discord(changes: EventChanges, now: datetime):
    formatted_changes = []
    event_config = changes.event_config

    for change_type, time_ranges in changes.changes.items():
        if len(time_ranges) == 0:
            continue

        formatted_change = {
            "fields": [
                {
                    "name": f"{change_type.value} {event_config.event_name} Sessions at {event_config.facility_name}",
                    "value": "".join([f"{pretty_print_time_range(e.start, e.end)}\n" for e in time_ranges]),
                },
            ],
            # green if new, red if cancelled
            'color': '65280' if change_type == ChangeType.NEW else '16711680',
            "timestamp": now.isoformat(),
        }
        formatted_changes.append(formatted_change)
    return formatted_changes
