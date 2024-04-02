import os
from contextlib import contextmanager
import pymongo
from . import datamodels

DB_NAME = os.getenv("DB_NAME")

@contextmanager
def db_connection_context(
    db_name: str = DB_NAME, collection_name: str = "hackernews_items"
):
    db_client = pymongo.MongoClient(
        os.getenv(
            "MONGO_DB_URL",
            None,
        ),
    )

    collection = db_client[db_name][collection_name]

    try:
        yield collection
    finally:
        db_client.close()

def get_all_usernames_who_shared_stories():
    with db_connection_context(collection_name="hackernews_items") as collection:
        query_result = collection.aggregate(
            [
                {
                    "$match": {
                        "type": "story",
                        "by": {"$exists": True, "$ne": None},
                        "time": {"$ne": None},
                        "deleted": False,
                    }
                },
                {"$project": {"by": 1}},
                {"$group": {"_id": "$by", "freq": {"$count": {}}}},
                {"$addFields": {"author_name": "$_id"}},
                {"$unset": "_id"},
            ]
        )
        return [item["author_name"] for item in query_result]

def insert_many_users(users: list[datamodels.HackerNewsUser], check_existence: bool = True):
    with db_connection_context(collection_name="hackernews_users") as collection:
        # search the database for those user ids
        if not check_existence:
            collection.insert_many([user.model_dump() for user in users])
            return
        
        user_ids = [user.id for user in users]
        existing_users = collection.find({"id": {"$in": user_ids}})

        # Insert only the users that don't exist
        existing_user_ids = [user["id"] for user in existing_users]
        users_to_insert = [user for user in users if user.id not in existing_user_ids]

        if users_to_insert:
            collection.insert_many([user.model_dump() for user in users_to_insert])

def insert_item(item: datamodels.HackerNewsItem):
    with db_connection_context(collection_name="hackernews_items") as collection:
        # Check if the item exists in the database
        if collection.find_one({"id": item.id}):
            return

        item_id = collection.insert_one(item.model_dump())
        return item_id

def insert_many_items(items: list[datamodels.HackerNewsItem], check_existence: bool = True):
    with db_connection_context(collection_name="hackernews_items") as collection:
        # search the database for those item ids
        if not check_existence:
            collection.insert_many([item.model_dump() for item in items])
            return
        
        item_ids = [item.id for item in items]
        existing_items = collection.find({"id": {"$in": item_ids}})

        # Insert only the items that don't exist
        existing_item_ids = [item["id"] for item in existing_items]
        items_to_insert = [item for item in items if item.id not in existing_item_ids]

        if items_to_insert:
            collection.insert_many([item.model_dump() for item in items_to_insert])


def get_all_ids():
    """
    Return the "id" field of all items in the database.
    """
    with db_connection_context(collection_name="hackernews_items") as collection:
        query_result = collection.find({}, {"id": 1, "_id": 0})
        print(f"Query result: {query_result}")
        return [item["id"] for item in query_result]

def get_latest_id():
    """
    Get the highest id from the database.
    """
    with db_connection_context(collection_name="hackernews_items") as collection:
        query_result = collection.find_one(sort=[("id", -1)])
        return query_result["id"] if query_result else 1 # 1 because the first item has id 1
