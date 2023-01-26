from datetime import datetime, timedelta
import dateutil.parser
import json
import pytz
import requests

TZ = pytz.timezone('US/Eastern')
FACILITY_ID = "6f60aca9-7fba-4bf1-b6af-1f85e9376462" # CIF Arena
FACILITY_WEB_UI_URL_FORMATTER = "https://warrior.uwaterloo.ca/Facility/GetSchedule?facilityId={facilityId}"
CALENDAR_URL_FORMATTER = "https://warrior.uwaterloo.ca/Facility/GetScheduleCustomAppointments?selectedId={facilityId}&start={start}&end={end}"
LOOKAHEAD_DAYS = 7
LOOKAHEAD_TIME = timedelta(days=LOOKAHEAD_DAYS)
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1068228337459265596/BgHIZTxLcW3qQ5gPUkawOOR1SusT_IoEAJHM0rJ0ZI2Qou8wrE7VBWG9ljFUC18a04Rz"

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

def lambda_handler(event, context):
    # current time in the timezone TZ
    now = datetime.now(TZ)

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

    # Send a message to Discord if there are any events
    message = f"**Open Rec Skate sessions at the CIF Arena in the next {LOOKAHEAD_DAYS} days:**\n"
    for e in cal_entries:
        message += f"{pretty_print_time_range(e['start'],e['end'])}\n"
    message += f"Check the calendar at {FACILITY_WEB_UI_URL_FORMATTER.format(facilityId=FACILITY_ID)}"
    requests.post(DISCORD_WEBHOOK_URL, json={"content": message})

    return {
        'statusCode': 200,
        'body': json.dumps(cal_entries)
    }

if __name__ == "__main__":
    print(lambda_handler(None, None))