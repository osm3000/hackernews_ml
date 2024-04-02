import requests
import json
import tenacity
from . import datamodels


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
)
def get_single_item(item_id: int):
    response = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json")
    if not response.ok:
        return None
    
    item = response.json()
    return datamodels.HackerNewsItem(**item)


def get_maxitem():
    response = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json")
    if not response.ok:
        return None

    return response.json()


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
)
def get_user_data(username: str):
    response = requests.get(f"https://hacker-news.firebaseio.com/v0/user/{username}.json")
    if not response.ok:
        return None
    
    data = response.json()
    # Delete the "submitted" field, since it is too large
    try:
        del data["submitted"]
    except:
        pass
    
    if data is None:
        return datamodels.HackerNewsUser(id=username)
    
    return datamodels.HackerNewsUser(**data)
