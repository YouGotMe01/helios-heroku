import os
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
                rss_d = feedparser.parse(my_feed_url)
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
        # ...
        exists = rss_dict.get(title)
        if exists is not None:
            LOGGER.error("This title already subscribed! Choose another title!")
            return sendMessage("This title already subscribed! Choose another title!", context.bot, update.message)
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
                rss_dict[title] = [feed_link, last_link, last_title, f_lists]
            sendMessage(sub_msg, context.bot, update.message)
            LOGGER.info(f"Rss Feed Added: {title} - {feed_link} - {filters}")
        except (IndexError, AttributeError) as e:
            LOGGER.error(str(e))
            msg = "The link doesn't seem to be an RSS feed or it's region-blocked!"
            sendMessage(msg, context.bot, update.message)
        except Exception as e:
            LOGGER.error(str(e))
            sendMessage(str(e), context.bot, update.message)
        # ...
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
        self.processed_feed_urls = set()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def create_feed_url_column(self):
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'rss_data' AND column_name = 'feed_url'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE rss_data ADD COLUMN feed_url TEXT")
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'rss_data' AND column_name = 'last_updated'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE rss_data ADD COLUMN last_updated TIMESTAMP DEFAULT NOW()")

    def create_feed_title_column(self):
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'rss_data' AND column_name = 'feed_title'")
            if not cur.fetchone():
                cur.execute("ALTER TABLE rss_data ADD COLUMN feed_title TEXT")

    def rss_update(self, name, feed_url, last_link, last_title, cur_last_title=None, new_title=None):
        if feed_url is None or feed_url == '':
            LOGGER.warning(f"No feed URL available for feed: {name}")
            return

        self.create_feed_url_column()  # Add this line to create the column if necessary
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM rss_data WHERE feed_url = %s AND last_updated > NOW() - INTERVAL '1 HOUR'",
                (feed_url,))
            row = cur.fetchone()
            if row and row[0] != name:
                LOGGER.warning(f"Feed URL already exists for feed: {row[0]}")
                return

            if cur_last_title is None:
                cur.execute(
                    "INSERT INTO rss_data (name, feed_url, last_link, last_title) VALUES (%s, %s, %s, %s)",
                    (name, feed_url, last_link, last_title))
            else:
                if new_title is None:
                    cur.execute(
                        "UPDATE rss_data SET feed_url = %s, feed_title = %s, last_link = %s, last_title = %s, last_updated = NOW() WHERE name = %s AND last_title = %s",
                        (feed_url, "", last_link, last_title, name, cur_last_title))
                else:
                    cur.execute(
                        "UPDATE rss_data SET feed_url = %s, feed_title = %s, last_link = %s, last_title = %s, name = %s, last_updated = NOW() WHERE name = %s AND last_title = %s",
                        (feed_url, new_title, last_link, last_title, name, name, cur_last_title))

    def update_feed_title(self, name, feed_title):
        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute("UPDATE rss_data SET feed_title = %s WHERE name = %s", (feed_title, name))
            conn.commit()
            
db_manager = DbManager(db_url)

processed_urls = set()

processed_feed_urls = set()  # Set to store processed feed URLs
def rss_monitor(context):
    db_manager = DbManager(db_url)
    processed_feed_urls = set()  # Set to store processed feed URLs

    with rss_dict_lock:
        rss_saver = rss_dict.copy()
        print(list(rss_saver.keys()))

    for name, data in rss_saver.items():
        try:
            with db_manager.get_connection() as conn, conn.cursor() as cur:
                cur.execute("SELECT last_title, feed_url FROM rss_data WHERE name = %s", (name,))
                row = cur.fetchone()
                my_last_title = row[0] if row else None
                my_feed_url = row[1] if row else None

            if my_feed_url is None or my_feed_url == '':
                LOGGER.warning(f"No feed URL available for feed: {name}")
                continue

            # Reset the processed URLs set for this feed
            processed_urls = set()

            # Skip processing if the feed URL has already been processed
            if my_feed_url in processed_feed_urls:
                continue

            rss_d = feedparser.parse(my_feed_url)
            if not rss_d.entries:
                LOGGER.warning(f"No entries found for feed: {name} - Feed URL: {my_feed_url}")
                continue

            magnets = set()
            for entry in rss_d.entries:
                entry_link = entry.get('link')
                entry_title = entry.get('title')
                entry_id = entry.get('id')  # Unique identifier for the entry, if available

                # Generate a unique identifier for the entry
                identifier = hashlib.md5(f"{my_feed_url}-{entry_link}".encode()).hexdigest()

                # Skip processing if the entry identifier has already been processed
                if identifier in processed_urls:
                    continue

                # Mark the entry identifier as processed
                processed_urls.add(identifier)

                if RSS_COMMAND is not None:
                    # Replace 'url' with the appropriate variable or URL to scrape for magnet links
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

                # Update the last processed entry's title in the database
                db_manager.rss_update(name, my_feed_url, entry_link, entry_title, my_last_title, cur_last_title=entry_title)

            # Update the feed title in the database
            feed_title = rss_d.feed.get('title', '')
            if feed_title:
                db_manager.update_feed_title(name, feed_title)

            # Mark the feed URL as processed
            processed_feed_urls.add(my_feed_url)

            # Log the feed title and number of entries processed
            LOGGER.info(f"Processed {len(processed_urls)} entries for feed: {name}")
            LOGGER.info(f"Feed Name: {name}")
            LOGGER.info(f"Feed Title: {feed_title}")

        except Exception as e:
            LOGGER.error(f"Error occurred while processing feed: {name} - {str(e)}")
            
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
