import asyncio
import time

from main import *
from handlers import *


loop = asyncio.get_event_loop()

async def find_new_links():

    print(f"Going to find new links for the db...")
    await Parser().parse_links()


async def check_db_for_old_items():
    print(f"Going to check links for old items...")

    await item_checker()


if __name__ == '__main__':

    schedule.every(30).minutes.do(check_db_for_old_items)
    schedule.every().day.at("4:20").do(find_new_links)

    while True:
        loop.run_until_complete(schedule.run_pending())
        time.sleep(0.1)

