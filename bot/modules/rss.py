import re
import cloudscraper 
import hashlib
import py3createtorrent
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
        args = update.message.text.split(maxsplit=2)
        title = args[1].strip()
        count = int(args[2].strip())
        feed_url = rss_dict.get(title)
        if feed_url is not None and count > 0:
            try:
                msg = context.bot.send_message(update.message.chat_id, f"Getting the last {count} item(s) from {title}")
                rss_d = feedparse(feed_url[0])
                item_info = ""
                for item_num in range(count):
                    if item_num >= len(rss_d.entries):
                        break
                    try:
                        link = rss_d.entries[item_num]['links'][1]['href']
                    except IndexError:
                        link = rss_d.entries[item_num]['link']
                    item_info += f"<b>Name: </b><code>{rss_d.entries[item_num]['title'].replace('>', '').replace('<', '')}</code>\n"
                    item_info += f"<b>Link: </b><code>{link}</code>\n\n"
                context.bot.edit_message_text(chat_id=update.message.chat_id, message_id=msg.message_id, text=item_info, parse_mode="HTML")
            except IndexError as e:
                LOGGER.error(str(e))
                context.bot.edit_message_text(chat_id=update.message.chat_id, message_id=msg.message_id, text="Parse depth exceeded. Try again with a lower value.")
            except Exception as e:
                LOGGER.error(str(e))
                context.bot.edit_message_text(chat_id=update.message.chat_id, message_id=msg.message_id, text=str(e))
        else:
            context.bot.send_message(update.message.chat_id, "Enter a valid title and count.")
    except (IndexError, ValueError):
        context.bot.send_message(update.message.chat_id, "Enter a valid title and count.")

def rss_sub(update, context):
    try:
        args = update.message.text.split(maxsplit=3)
        title = args[1].strip()
        feed_link = args[2].strip()

        exists = rss_dict.get(title)
        if exists is not None:
            LOGGER.error("This title is already subscribed! Choose another title.")
            return sendMessage("This title is already subscribed! Choose another title.", context.bot, update.message)
        
        try:
            rss_d = feedparse(feed_link)
            sub_msg = "<b>Subscribed!</b>"
            sub_msg += f"\n\n<b>Title: </b><code>{title}</code>\n<b>Feed URL: </b>{feed_link}"
            sub_msg += f"\n\n<b>Latest record for </b>{rss_d.feed.title}:"
            sub_msg += f"\n\n<b>Name: </b><code>{rss_d.entries[0]['title'].replace('>', '').replace('<', '')}</code>"
            
            try:
                link = rss_d.entries[0]['links'][1]['href']
            except IndexError:
                link = rss_d.entries[0]['link']
            
            sub_msg += f"\n\n<b>Link: </b><code>{link}</code>"
            
            last_link = str(rss_d.entries[0]['link'])
            last_title = str(rss_d.entries[0]['title'])
            
            DbManager().rss_add(title, feed_link, last_link, last_title, None)
            
            with rss_dict_lock:
                if len(rss_dict) == 0:
                    rss_job.enabled = True
                rss_dict[title] = [feed_link, last_link, last_title, None]
            
            sendMessage(sub_msg, context.bot, update.message)
            LOGGER.info(f"RSS Feed Added: {title} - {feed_link}")
        except (IndexError, AttributeError) as e:
            LOGGER.error(str(e))
            msg = "The link doesn't seem to be an RSS feed or it's region-blocked!"
            sendMessage(msg, context.bot, update.message)
        except Exception as e:
            LOGGER.error(str(e))
            sendMessage(str(e), context.bot, update.message)
    except IndexError:
        msg = f"Use this format to add a feed URL:\n/{BotCommands.RssSubCommand} Title https://www.rss-url.com"
        msg += "\n\nExample: /rss_sub MyFeed https://www.example.com/rss-feed.xml"
        sendMessage(msg, context.bot, update.message)
        
def rss_unsub(update, context):
    try:
        title = context.args[0]
        exists = rss_dict.get(title)
        if exists is None:
            msg = f"Rss link with Title: <code>{title}</code> does not exist! Nothing removed!"
            LOGGER.error(msg)
            sendMessage(msg, context.bot, update.message)
        else:
            DbManager().rss_delete(title)
            with rss_dict_lock:
                del rss_dict[title]
            msg = f"Rss link with Title: <code>{title}</code> has been removed!"
            sendMessage(msg, context.bot, update.message)
            LOGGER.info(msg)
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
            if data[1] == rss_d.entries[0]['link']:
                last_title = rss_d.entries[0]['title']
                if data[2] == last_title:
                    continue
            else:
                last_title = rss_d.entries[0]['title']
            feed_count = 0
            while True:
                try:
                    if data[1] == rss_d.entries[feed_count]['link'] and data[2] == rss_d.entries[feed_count]['title']:
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
                    new_link = rss_d.entries[feed_count]['links'][1]['href']
                    new_title = rss_d.entries[feed_count]['title']
                    if RSS_COMMAND is not None:
                        scraper = cloudscraper.create_scraper(allow_brotli=False)
                        page_content = scraper.get(new_link).text
                        soup = BeautifulSoup(page_content, 'html.parser')
                        magnet_links = soup.find_all('a', href=re.compile(r'^magnet:'))
                        if magnet_links:
                            magnet_link = magnet_links[0]['href']
                            feed_msg = f"{RSS_COMMAND} {magnet_link}"
                            sendRss(feed_msg, context.bot)
                except IndexError:
                    LOGGER.warning(f"No link found for this feed: {name}, entry index: {feed_count}")
                feed_count += 1
                sleep(5)
            DbManager().rss_update(name, data[1], str(last_title))
            with rss_dict_lock:
                rss_dict[name] = [data[0], data[1], str(last_title), data[3]]
            LOGGER.info(f"Feed Name: {name}")
            LOGGER.info(f"Last item: {data[1]}")
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

    
