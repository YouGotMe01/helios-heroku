import os
import functools
import logging
import threading
import psycopg2
import cloudscraper
import re
import feedparser
import time
import hashlib
from bs4 import BeautifulSoup
from time import sleep
from psycopg2 import DatabaseError
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
                rss_d = feedparser.parse(feed_url)
                item_info = ""
                for item_num in range(count):
                    try:
                        link = rss_d.entries[item_num]['links'][1]['href']
                    except IndexError:
                        link = rss_d.entries[item_num]['link']
                    item_info += f"<b>Name: </b><code>{rss_d.entries[item_num]['title'].replace('>', '').replace('<', '')}</code>\n"
                    item_info += f"<b>Link: </b><code>{link}</code>\n\n"
                editMessage(item_info, msg)
            except IndexError as e:
                LOGGER.error(str(e))
                editMessage("Parse depth exceeded. Try again with a lower value.", msg)
            except Exception as e:
                LOGGER.error(str(e))
                editMessage(str(e), msg)
        else:
            sendMessage("Enter a valid title/value.", context.bot, update.message)
    except (IndexError, ValueError):
        sendMessage(f"Use this format to fetch:\n/{BotCommands.RssGetCommand} Title value", context.bot, update.message)

def rss_sub(update, context):
    try:
        args = update.message.text.split(maxsplit=3)
        title = args[1].strip()
        feed_link = args[2].strip()
        feed_title = args[3].strip()  # Assuming the feed title is provided as an argument

        exists = rss_dict.get(title)
        if exists is not None:
            LOGGER.error("This title already subscribed! Choose another title!")
            return sendMessage("This title already subscribed! Choose another title!", context.bot, update.message)

        # Create a dictionary with 'url' and 'title' attributes
        feed_data = {'url': feed_link, 'title': feed_title}

        try:
            rss_d = feedparser.parse(feed_link)
            sub_msg = "<b>Subscribed!</b>"
            sub_msg += f"\n\n<b>Title: </b><code>{title}</code>\n<b>Feed Url: </b>{feed_link}"
            sub_msg += f"\n\n<b>latest record for </b>{rss_d.feed.title}:"
            sub_msg += f"\n\n<b>Name: </b><code>{rss_d.entries[0]['title'].replace('>', '').replace('<', '')}</code>"
            try:
                link = rss_d.entries[0]['links'][1]['href']
            except IndexError:
                link = rss_d.entries[0]['link']
            sub_msg += f"\n\n<b>Link: </b><code>{link}</code>"
            sub_msg += f"\n\n<b>Filters: </b><code>{filters}</code>"
            last_link = str(rss_d.entries[0]['link'])
            last_title = str(rss_d.entries[0]['title'])
            db_manager.rss_add(title, feed_link, last_link, last_title, filters)

            with rss_dict_lock:
                if len(rss_dict) == 0:
                    rss_job.enabled = True
                rss_dict[title] = [feed_data]

            sendMessage(sub_msg, context.bot, update.message)
            LOGGER.info(f"Rss Feed Added: {title} - {feed_link} - {filters}")
        except (IndexError, AttributeError) as e:
            LOGGER.error(str(e))
            msg = "The link doesn't seem to be an RSS feed or it's region-blocked!"
            sendMessage(msg, context.bot, update.message)
        except Exception as e:
            LOGGER.error(str(e))
            sendMessage(str(e), context.bot, update.message)
    except IndexError:
        # ...
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
            db_manager.rss_delete(title)
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
            db_manager.trunc_table('rss')
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
            
LOGGER = logging.getLogger(__name__)
db_url = os.environ.get('DATABASE_URL')

class DbManager:
    def __init__(self, db_url):
        self.db_url = db_url

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def rss_update(self, name, feed_url, entry_link, entry_title, last_title, new_title=None):
        with self.get_connection() as conn, conn.cursor() as cur:
            if new_title is None:
                cur.execute(
                    "UPDATE rss_data SET last_title = %s WHERE name = %s AND feed_url = %s AND last_title = %s",
                    (entry_title, name, feed_url, last_title))
                
            else:
                cur.execute(
                    "UPDATE rss_data SET last_title = %s WHERE name = %s AND feed_url = %s AND last_title = %s",
                    (new_title, name, feed_url, last_title))
                

                cur.execute(
                    "INSERT INTO rss_history(name, feed_url, entry_link, entry_title) VALUES (%s, %s, %s, %s)",
                    (name, feed_url, entry_link, entry_title))
                

    def update_feed_title(self, name, feed_title):
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE rss_data SET feed_title = %s WHERE name = %s", (feed_title, name))

    def get_last_processed_entry(self, name, feed_url):
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT entry_link FROM rss_history WHERE name = %s AND feed_url = %s ORDER BY id DESC LIMIT 1",
                (name, feed_url))
            
            row = cur.fetchone()
            return row[0] if row else None

    def rss(self, name, feed_url, feed_title=None):
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rss_data(name, feed_url, last_title, feed_title) VALUES (%s, %s, '', %s) ON CONFLICT DO NOTHING",
                (name, feed_url, feed_title))
            

    def rss_delete(self, name):
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM rss_data WHERE name = %s", (name,))
            cur.execute("DELETE FROM rss_history WHERE name = %s", (name,))


def rss_monitor(context, db_url): 
    db_manager = DbManager(db_url)

    with rss_dict_lock:
        rss_saver = rss_dict.copy()
        print(list(rss_saver.keys()))

    for name, data_list in rss_saver.items():
        try:
            for data in data_list:
                if not isinstance(data, dict):
                    LOGGER.warning(f"Invalid data structure for feed: {name}")
                    continue

                feed_url = data[0].get('url')  # Access 'url' from the first dictionary in data_list
                feed_title = data[0].get('title')  # Access 'title' from the first dictionary in data_list

                if feed_url is None:
                    LOGGER.warning(f"No feed URL available for feed: {name}")
                    continue

                with db_manager.get_connection() as conn, conn.cursor() as cur:
                    cur.execute("SELECT last_title, feed_url FROM rss_data WHERE name = %s", (name,))
                    row = cur.fetchone()
                    last_title = row[0] if row else None
                    db_feed_url = row[1] if row else None

                if db_feed_url == feed_url:
                    LOGGER.info(f"Feed URL already exists for feed: {name}")
                else:
                    db_manager.rss(name, feed_url, feed_title)

                rss_d = feedparser.parse(feed_url)
                if not rss_d.entries:
                    LOGGER.warning(f"No entries found for feed: {name} - Feed URL: {feed_url}")
                    continue

                magnets = set()
                cur_last_title = None
                for entry in rss_d.entries:
                    entry_link = entry.get('link')
                    entry_title = entry.get('title')
                    entry_id = entry.get('id')
                    last_processed_entry = db_manager.get_last_processed_entry(name, feed_url)
                    if last_processed_entry == entry_link:
                        LOGGER.info(f"Entry already processed for feed: {name}")
                        continue

                    identifier = hashlib.md5(f"{feed_url}-{entry_link}".encode()).hexdigest()

                    if RSS_COMMAND is not None:
                        magnet_url = entry_link
                        scraper = cloudscraper.create_scraper(allow_brotli=False)
                        html = scraper.get(magnet_url).text
                        soup = BeautifulSoup(html, 'html.parser')
                        for a_tag in soup.find_all('a', attrs={'href': re.compile(r"^magnet")}):
                            magnet_url = a_tag.get('href')
                            title = entry_title.replace('>', '').replace('<', '')
                            if (magnet_url, title) not in magnets:
                                magnets.add((magnet_url, title))
                        for magnet_url, title in magnets:
                            feed_msg = f"/{RSS_COMMAND} {magnet_url}"
                            sendRss(feed_msg, context.bot)
                    else:
                        feed_msg = f"<b>Name: </b><code>{entry_title.replace('>', '').replace('<', '')}</code>\n\n"
                        feed_msg += f"<b>Link: </b><code>{entry_link}</code>"
                        sendRss(feed_msg, context.bot)

                    if cur_last_title is None:
                        db_manager.rss_update(name, feed_url, entry_link, entry_title, last_title)
                        cur_last_title = entry_title
                    else:
                        db_manager.rss_update(name, feed_url, entry_link, entry_title, cur_last_title)
                        cur_last_title = entry_title
                        
        except Exception as e:
            LOGGER.error(f"Error monitoring feed: {name} - Error: {str(e)}")

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
    rss_job = job_queue.run_repeating(
        functools.partial(rss_monitor, db_url=db_url),
        interval=RSS_DELAY,
        first=20,
        name="RSS")


