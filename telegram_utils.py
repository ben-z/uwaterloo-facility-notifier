import asyncio
import os
from typing import List, Set

import telegram
from telegram.constants import ChatMemberStatus

from utils import (FACILITY_WEB_UI_URL_FORMATTER, SOURCE_CODE_URL, ChangeType,
                   DynamoDBTable, EventChanges, EventConfig, flatten,
                   pretty_print_time_range)

TELEGRAM_BOT_TOKEN = os.environ['TELEGRAM_BOT_TOKEN']

TELEGRAM_WELCOME_MESSAGE = "Hi there! I'm a bot that sends updates about UWaterloo facility schedules. To subscribe to updates, please send me `/subscribe`. To unsubscribe, please send me `/unsubscribe`. Send me `/help` to see this message again. I reply periodically instead of on-demand, so it might take a few minutes (depending on my deployment setting) for me to respond."


async def refresh_telegram_subscribers_async(table: DynamoDBTable):
    if not TELEGRAM_BOT_TOKEN:
        return []

    bot = telegram.Bot(TELEGRAM_BOT_TOKEN)
    last_update_id = int(table.get('telegram_last_update_id') or -1)
    update_subscribers = set(int(i) for i in (
        table.get('telegram_update_subscribers') or []))

    # Process updates
    async with bot:
        updates = await bot.get_updates(offset=last_update_id + 1)
        for update in updates:
            last_update_id = max(last_update_id, update.update_id)

            if not update.effective_chat:
                print(
                    f"Error: update {update} has no effective chat. Skipping.")
                continue
            effective_chat_id = update.effective_chat.id

            try:
                if update.effective_message:
                    # A message was sent to the bot
                    if not update.effective_message.text:
                        # Non-text messages (this is probably a service message)
                        continue

                    message_lower = update.effective_message.text.lower()
                    if '/unsubscribe' in message_lower:
                        update_subscribers.discard(effective_chat_id)
                        await bot.send_message(effective_chat_id, reply_to_message_id=update.effective_message.message_id, text="This chat has been unsubscribed from updates.")
                    elif '/subscribe' in message_lower:
                        update_subscribers.add(effective_chat_id)
                        await bot.send_message(effective_chat_id, reply_to_message_id=update.effective_message.message_id, text="This chat has been subscribed to updates!")
                    elif '/start' in message_lower or '/help' in message_lower:
                        await bot.send_message(effective_chat_id, reply_to_message_id=update.effective_message.message_id, text=TELEGRAM_WELCOME_MESSAGE)
                    else:
                        await bot.send_message(effective_chat_id, reply_to_message_id=update.effective_message.message_id, text="I don't understand this command. Please use `/help` to see a list of available commands.")
                elif update.my_chat_member:
                    # The bot's chat member status was updated in a chat
                    await bot.send_message(effective_chat_id, text=TELEGRAM_WELCOME_MESSAGE)
                else:
                    print(f"ERROR: Unknown update type: {update}")
            except (telegram.error.Forbidden, telegram.error.BadRequest) as e:
                print(
                    f"Bot cannot send messages to {effective_chat_id}! The bot was probably disabled or removed from the chat. Removing the chat from the subscribers list. Update: {update}. Error: {e}")
                update_subscribers.discard(effective_chat_id)

    table.put('telegram_last_update_id', last_update_id)
    table.put('telegram_update_subscribers', list(update_subscribers))

    return update_subscribers


def refresh_telegram_subscribers(*args, **kwargs):
    return asyncio.run(refresh_telegram_subscribers_async(*args, **kwargs))


def format_schedule_for_telegram(event_config: EventConfig, cal_entries: List):
    event_name = event_config.event_name
    facility_name = event_config.facility_name
    lookahead_days = event_config.lookahead_days

    message = f"*{event_name} sessions at {facility_name} in the next {lookahead_days} days*"
    for e in cal_entries:
        message += f"\n{pretty_print_time_range(e['start'],e['end'])}"

    message += f"\n[facility schedule]({FACILITY_WEB_UI_URL_FORMATTER.format(facilityId=event_config.facility_id)})"
    return message


def format_changes_for_telegram(changes: EventChanges):
    message = ""
    for change_type, time_ranges in changes.changes.items():
        if len(time_ranges) == 0:
            continue
        if message:
            message += "\n\n"

        message += f"*{change_type.value} {changes.event_config.event_name} sessions*"
        if change_type == ChangeType.NEW:
            emoji = u"✅"
        elif change_type == ChangeType.CANCELLED:
            emoji = u"❌"
        else:
            emoji = ""

        for time_range in time_ranges:
            message += f"\n{emoji} {pretty_print_time_range(time_range.start, time_range.end)}"

    return message


async def send_telegram_updates_async(
    subscribers: Set[int],
    changes_list: List[EventChanges],
    event_configs: List[EventConfig],
    cal_entries_list: List[List],
):
    if not TELEGRAM_BOT_TOKEN:
        return []

    bot = telegram.Bot(TELEGRAM_BOT_TOKEN)

    message = "\n\n".join(filter(None, flatten([
        [format_changes_for_telegram(ch) for ch in changes_list],
        [format_schedule_for_telegram(c, e) for c, e in zip(
            event_configs, cal_entries_list)],
        [f"------------\n[Bot source code]({SOURCE_CODE_URL})"],
    ])))

    async with bot:
        for chat_id in subscribers:
            await bot.send_message(chat_id, text=message, parse_mode=telegram.constants.ParseMode.MARKDOWN, disable_web_page_preview=True)

    return []


def send_telegram_updates(*args, **kwargs):
    return asyncio.run(send_telegram_updates_async(*args, **kwargs))
