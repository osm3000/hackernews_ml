from dotenv import load_dotenv
x = load_dotenv(".env")
from hn_scrape import db_wrapper, scrapper
import os
import time
import concurrent.futures
import logging

logger = logging.getLogger(__name__)
# Set the logger format
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def scrape_items():
    max_item = scrapper.get_maxitem()
    logger.info(f"Max item: {max_item}")

    time_0 = time.time()

    # In chunks of 1000
    chunk_size = 10_000
    max_workers = 100

    # Get the latest (highest) element id from the database
    latest_id = int(db_wrapper.get_latest_id())
    if latest_id > chunk_size:
        latest_id -= chunk_size  # Since to make sure I didn't miss anything, since I am using 1000 as the chunk size

    id_of_all_items = range(latest_id, max_item)

    logger.info(f"Latest id: {latest_id}")
    logger.info(f"Number of items to get: {len(id_of_all_items)}")
    logger.info(f'Size of all hackernews items: {max_item}')
    for i in range(0, len(id_of_all_items), chunk_size):
        chunk = id_of_all_items[i:i + chunk_size]
        logger.info(f"Getting items {i} to {i + chunk_size} out of {len(id_of_all_items)}")
        items = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            thread_results = executor.map(scrapper.get_single_item, chunk)
            items = list(thread_results)
        logger.info(f"Adding {len(items)} items to the database.")

        # Insert the items into the database in async
        # with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        #     executor.map(db_wrapper.insert_item, items)
        if i == 0:
            db_wrapper.insert_many_items(items, check_existence=True)
        else:
            db_wrapper.insert_many_items(items, check_existence=False)

    time_1 = time.time()
    logger.info(f"Time taken: {time_1 - time_0}")


def scrape_users_data():
    # Get all the username who shared stories on hackernews from the database
    all_usernames = db_wrapper.get_all_usernames_who_shared_stories()
    logger.info(f"Number of usernames: {len(all_usernames)}")

    time_0 = time.time()
    chunk_size = 10_000
    max_workers = 100
    cnt = 0
    for i in range(0, len(all_usernames), chunk_size):
        chunk = all_usernames[i:i + chunk_size]
        logger.info(f"Getting users {i} to {i + chunk_size} out of {len(all_usernames)}")
        users = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            thread_results = executor.map(scrapper.get_user_data, chunk)
            users = list(thread_results)
        logger.info(f"Adding {len(users)} users to the database.")

        # cnt += 1
        # if cnt == 5:
        #     break
        # Insert the users into the database in async
        db_wrapper.insert_many_users(users, check_existence=False)
        

if __name__ == "__main__":
    # scrape_items()
    scrape_users_data()
