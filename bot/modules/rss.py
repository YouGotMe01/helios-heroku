import re
import requests
import cloudscraper 
from bs4 import BeautifulSoup
from datetime import datetime
from feedparser import parse as feedparse
from time import sleep
from telegram.ext import CommandHandler, CallbackQueryHandler
from threading import Lock, Thread

from bot import dispatcher, job_queue, rss_dict, LOGGER, DB_URI, RSS_DELAY, RSS_CHAT_ID, RSS_COMMAND, AUTO_DELETE_MESSAGE_DURATION
from bot.helper.telegram_helper.message_utils import sendMessage, editMessage, sendMarkup, auto_delete_message, sendRss
from bot.helper.telegram_helper.filters import CustomFilters
from bot.helper.telegram_helper.bot_commands import BotCommands
from bot.helper.ext_utils.db_handler import DbManager
from bot.helper.telegram_helper import button_build

rss_dict_lock = Lock()

def rss_list(update, context):
    if len(rss_dict) > 0:
        list_feed = "<b>Your subscriptions: </b>\n\n"
        for title, url in list(rss_dict.items()):
            list_feed += f"<b>Title:</b> <code>{title}</code>\n<b>Feed Url: </b><code>{url[0]}</code>\n\n"
        sendMessage(list_feed, context.bot, update.message)
    else:
        sendMessage("No subscriptions.", context.bot, update.message)

def rss_get(update, context):
    try:
        title = context.args[0]
        count = int(context.args[1])
        feed_url = rss_dict.get(title)
        
        if feed_url is not None and count > 0:
            try:
                msg = sendMessage(f"Getting the last <b>{count}</b> item(s) from {title}", context.bot, update.message)
                rss_d = feedparse(feed_url[0])
                num_entries = min(count, len(rss_d.entries))  # Handle cases when count is larger than available items
                item_info = "Here are the last items:\n\n"
                
                for item_num in range(num_entries):
                    try:
                        link = rss_d.entries[item_num]['links'][1]['href']
                    except IndexError:
                        link = rss_d.entries[item_num]['link']
                    
                    item_info += f"{item_num + 1}. <b>Name: </b><code>{rss_d.entries[item_num]['title'].replace('>', '').replace('<', '')}</code>\n"
                    item_info += f"<b>Link: </b><code>{link}</code>\n\n"
                
                editMessage(item_info, msg)
            
            except IndexError as e:
                LOGGER.error(str(e))
                editMessage("Parse depth exceeded. Try again with a lower value.", msg)
            
            except Exception as e:
                LOGGER.error(str(e))
                editMessage(str(e), msg)
        
        else:
            sendMessage("Enter a valid title and a positive item count.", context.bot, update.message)
    
    except (IndexError, ValueError):
        sendMessage(f"Use this format to fetch:\n/{BotCommands.RssGetCommand} Title value", context.bot, update.message)
        
def rss_sub(update, context, new_title=None):
    rss_dict = context.bot_data.setdefault('rss', {})

    if new_title is not None:
        title = new_title.strip()
        # Rest of the function code...
    else:
        try:
            args = update.message.text.split(maxsplit=1)
            if len(args) < 2:
                raise IndexError
            title_url = args[1].strip().split('|', 1)
            title = title_url[0]
            feed_url = title_url[1] if len(title_url) > 1 else None
            exists = rss_dict.get(title)
            if exists is not None:
                # If the title exists, add all available feed URLs to the list.
                if feed_url is None:
                    for feed in exists:
                        exists_feed_url = feed['url']
                        for existing_feed in exists:
                            if existing_feed['url'] == exists_feed_url:
                                break
                        else:
                            exists.append({"url": exists_feed_url, "added": datetime.now()})
                else:
                    # If the user provided a feed URL, check if it's in the list of available feed URLs.
                    for feed in exists:
                        if feed["url"] == feed_url:
                            LOGGER.warning(f"Feed URL '{feed_url}' already subscribed to title '{title}'")
                            sendMessage(f"Feed URL '{feed_url}' already subscribed to title '{title}'", context.bot, update.message)
                            return
                    exists.append({"url": feed_url, "added": datetime.now()})
            else:
                # If the title doesn't exist, create a new entry in the dictionary with the feed URL.
                if feed_url is not None:
                    rss_dict[title] = [{"url": feed_url, "added": datetime.now()}]
                else:
                    LOGGER.error("No feed URL provided for the new title!")
                    return sendMessage("No feed URL provided for the new title! Please provide a feed URL.", context.bot, update.message)
            sendMessage(f"Title '{title}' subscribed!", context.bot, update.message)
            LOGGER.info(f"Rss Feed Title Added: {title}")
        except IndexError:
            msg = f"Use this format to add a feed URL\n/{BotCommands.RssSubCommand} Title|https://www.rss-url.com"
            sendMessage(msg, context.bot, update.message)

def rss_unsub(update, context):
    try:
        title = context.args[0]
        exists = rss_dict.get(title)
        if exists is None:
            msg = "Rss link not exists! Nothing removed!"
            LOGGER.error(msg)
            sendMessage(msg, context.bot, update.message)
        else:
            DbManager().rss_delete(title)
            with rss_dict_lock:
                del rss_dict[title]
            sendMessage(f"Rss link with Title: <code>{title}</code> has been removed!", context.bot, update.message)
            LOGGER.info(f"Rss link with Title: {title} has been removed!")
    except IndexError:
        sendMessage(f"Use this format to remove feed url:\n/{BotCommands.RssUnSubCommand} Title", context.bot, update.message)
    
def rss_settings(update, context):
    buttons = button_build.ButtonMaker()
    buttons.sbutton("Unsubscribe All", "rss unsuball")
    if rss_job.enabled:
        buttons.sbutton("Pause", "rss pause")
    else:
        buttons.sbutton("Start", "rss start")
    if AUTO_DELETE_MESSAGE_DURATION == -1:
        buttons.sbutton("Close", f"rss close")
    button = buttons.build_menu(1)
    setting = sendMarkup('Rss Settings', context.bot, update.message, button)
    Thread(target=auto_delete_message, args=(context.bot, update.message, setting)).start()

def rss_set_update(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    msg = query.message
    data = query.data
    data = data.split()
    if not CustomFilters._owner_query(user_id):
        query.answer(text="You don't have permission to use these buttons!", show_alert=True)
    elif data[1] == 'unsuball':
        query.answer()
        if len(rss_dict) > 0:
            DbManager().trunc_table('rss')
            with rss_dict_lock:
                rss_dict.clear()
            rss_job.enabled = False
            editMessage("All Rss Subscriptions have been removed.", msg)
            LOGGER.info("All Rss Subscriptions have been removed.")
        else:
            editMessage("No subscriptions to remove!", msg)
    elif data[1] == 'pause':
        query.answer()
        rss_job.enabled = False
        editMessage("Rss Paused", msg)
        LOGGER.info("Rss Paused")
    elif data[1] == 'start':
        query.answer()
        rss_job.enabled = True
        editMessage("Rss Started", msg)
        LOGGER.info("Rss Started")
    else:
        query.answer()
        try:
            query.message.delete()
            query.message.reply_to_message.delete()
        except:
            pass

def rss_monitor(context):
    with rss_dict_lock:
        if len(rss_dict) == 0:
            rss_job.enabled = False
            return
        rss_saver = rss_dict.copy()  # Create a copy of the rss_dict to avoid modifying the original while iterating.

    for name, data in rss_saver.items():
        try:
            rss_d = feedparse(data[0])
            last_link = rss_d.entries[0]['link']
            last_title = rss_d.entries[0]['title']

            if data[1] == last_link or data[2] == last_title:
                continue

            # Fetch the torrent content from the link.
            if 'links' in rss_d.entries[0]:
                url = rss_d.entries[0]['links'][0]['href']
            else:
                url = rss_d.entries[0]['link']
            response = requests.get(url)
            if response.ok:
                # Send the torrent content as a message to the Telegram chat with .torrent extension
                torrent_content = response.content
                torrent_filename = f"{rss_d.entries[0]['title']}.torrent"
                context.bot.send_document(chat_id=RSS_CHAT_ID, document=torrent_content, filename=torrent_filename, caption=f"{rss_d.entries[0]['title']}    @eswar2242")                                             
            # Update the last_link and last_title in the dictionary (or database if you want to persist it).
            rss_dict[name] = [data[0], str(last_link), str(last_title), data[3]]
            DbManager().rss_update(name, str(last_link), str(last_title))

            # You can send a confirmation message or log the new feed title here.
            LOGGER.info(f"New Feed Name: {name}")
            LOGGER.info(f"Last item: {last_link}")

        except Exception as e:
            LOGGER.error(f"{e} Feed Name: {name} - Feed Link: {data[0]}")
            continue

if DB_URI is not None and RSS_CHAT_ID is not None:
    rss_list_handler = CommandHandler(BotCommands.RssListCommand, rss_list, filters=CustomFilters.owner_filter | CustomFilters.sudo_user, run_async=True)
    rss_get_handler = CommandHandler(BotCommands.RssGetCommand, rss_get, filters=CustomFilters.owner_filter | CustomFilters.sudo_user, run_async=True)
    rss_sub_handler = CommandHandler(BotCommands.RssSubCommand, rss_sub, filters=CustomFilters.owner_filter | CustomFilters.sudo_user, run_async=True)
    rss_unsub_handler = CommandHandler(BotCommands.RssUnSubCommand, rss_unsub, filters=CustomFilters.owner_filter | CustomFilters.sudo_user, run_async=True)
    rss_settings_handler = CommandHandler(BotCommands.RssSettingsCommand, rss_settings, filters=CustomFilters.owner_filter | CustomFilters.sudo_user, run_async=True)
    rss_buttons_handler = CallbackQueryHandler(rss_set_update, pattern="rss", run_async=True)

    dispatcher.add_handler(rss_list_handler)
    dispatcher.add_handler(rss_get_handler)
    dispatcher.add_handler(rss_sub_handler)
    dispatcher.add_handler(rss_unsub_handler)
    dispatcher.add_handler(rss_settings_handler)
    dispatcher.add_handler(rss_buttons_handler)
    rss_job = job_queue.run_repeating(rss_monitor, interval=RSS_DELAY, first=20, name="RSS")
    rss_job.enabled = True
