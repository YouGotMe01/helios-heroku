import re
import cloudscraper 
from bs4 import BeautifulSoup
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
            sendMessage("Enter a vaild title/value.", context.bot, update.message)
    except (IndexError, ValueError):
        sendMessage(f"Use this format to fetch:\n/{BotCommands.RssGetCommand} Title value", context.bot, update.message)

def rss_sub(update, context):
    try:
        args = update.message.text.split(maxsplit=1)
        if len(args) < 2:
            raise IndexError
        arg_list = args[1].split('|')
        title = arg_list[0].strip()
        feed_link = arg_list[1].strip()
        filters = arg_list[2].strip().lower() if len(arg_list) > 2 else None

        exists = rss_dict.get(title)
        if exists is not None:
            LOGGER.error("This title already subscribed! Choose another title!")
            return sendMessage("This title already subscribed! Choose another title!", context.bot, update.message)

        try:
            rss_d = feedparse(feed_link)
            sub_msg = "<b>Subscribed!</b>"
            sub_msg += f"\n\n<b>Title: </b><code>{title}</code>\n<b>Feed Url: </b>{feed_link}"
            sub_msg += f"\n\n<b>latest record for </b>{rss_d.feed.title}:"
            sub_msg += f"\n\n<b>Name: </b><code>{rss_d.entries[0]['title'].replace('>', '').replace('<', '')}</code>"
            try:
                link = rss_d.entries[0]['links'][1]['href']
            except IndexError:
                link = rss_d.entries[0]['link']
            sub_msg += f"\n\n<b>Link: </b><code>{link}</code>"

            if filters:
                sub_msg += f"\n\n<b>Filters: </b><code>{filters}</code>"

            last_link = str(rss_d.entries[0]['link'])
            last_title = str(rss_d.entries[0]['title'])
            DbManager().rss_add(title, feed_link, last_link, last_title, filters)
            with rss_dict_lock:
                if len(rss_dict) == 0:
                    rss_job.enabled = True
                rss_dict[title] = [feed_link, last_link, last_title, filters]
            sendMessage(sub_msg, context.bot, update.message)
            LOGGER.info(f"Rss Feed Added: {title} - {feed_link} - {filters}")
        except (IndexError, AttributeError) as e:
            LOGGER.error(str(e))
            msg = "The link doesn't seem to be a valid RSS feed or it's region-blocked!"
            sendMessage(msg, context.bot, update.message)
        except Exception as e:
            LOGGER.error(str(e))
            sendMessage(str(e), context.bot, update.message)
    except IndexError:
        msg = f"Use this format to add feed URL:\n/{BotCommands.RssSubCommand} Title|https://www.rss-url.com"
        msg += " (optional)|filters_here\n\nYou can add filters to parse specific feed items based on their titles."
        msg += "\nFor example: 1080p|https://example.com/rss-feed|1080p or 720p|.web. or .webrip.|hevc or x264"
        msg += "\n\nFilters format:\n\n1. Use '|' to separate Title, URL, and Filters (if any).\n\n2. "
        msg += "Filters are case-insensitive and should be separated by 'or'. For example: 1080p or 720p"
        msg += "\n\n3. If you don't want to specify filters, just omit them from the command."
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
        rss_saver = rss_dict.copy()  # Make a copy of rss_dict to avoid modifying it during iteration
    for name, data in rss_saver.items():
        try:
            rss_d = feedparse(data[0])
            last_link = rss_d.entries[0]['link']
            last_title = rss_d.entries[0]['title']
            if data[1] == last_link or data[2] == last_title:
                continue
            feed_count = 0
            new_feed_found = False  # Flag to keep track of whether a new feed item has been found
            while True:
                try:
                    if data[1] == rss_d.entries[feed_count]['link'] or data[2] == rss_d.entries[feed_count]['title']:
                        break
                except IndexError:
                    LOGGER.warning(f"Reached Max index no. {feed_count} for this feed: {name}. \
                          Maybe you need to use less RSS_DELAY to not miss some torrents")
                    break
                parse = True
                for lst in data[3]:
                    if not any(x in str(rss_d.entries[feed_count]['title']).lower() for x in lst):
                        parse = False
                        feed_count += 1
                        break
                if not parse:
                    continue
                try:
                    url = rss_d.entries[feed_count]['links'][1]['href']
                except IndexError:
                    url = rss_d.entries[feed_count]['link']

                # Scrape the magnet link from the URL using cloudscraper and BeautifulSoup
                scraper = cloudscraper.create_scraper(allow_brotli=False)
                page_content = scraper.get(url).text
                soup = BeautifulSoup(page_content, 'html.parser')
                magnet_links = soup.find_all('a', href=re.compile(r'^magnet:'))
                if magnet_links:
                    magnet_link = magnet_links[0]['href']
                    # Send the magnet link and title of the new feed item using bot.send_message()
                    context.bot.send_message(chat_id=RSS_CHAT_ID, text=f"<b>Name: </b><code>{rss_d.entries[feed_count]['title'].replace('>', '').replace('<', '')}</code>\n\n"
                                                                     f"<b>Link: </b><code>{url}</code>\n\n"
                                                                     f"<b>Magnet Link: </b><code>{magnet_link}</code>",
                                             parse_mode='HTML')
                    new_feed_found = True
                else:
                    LOGGER.warning(f"No magnet link found for this feed: {name}, entry index: {feed_count}")

                feed_count += 1
                sleep(5)
            if new_feed_found:
                DbManger().rss_update(name, str(last_link), str(last_title))
                with rss_dict_lock:
                    rss_dict[name] = [data[0], str(last_link), str(last_title), data[3]]
                LOGGER.info(f"Feed Name: {name}")
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
