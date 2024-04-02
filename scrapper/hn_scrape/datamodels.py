from pydantic import BaseModel, Field, PrivateAttr, model_validator
import bson
from typing import List, Optional, Literal
import datetime

class HackerNewsItem(BaseModel):
    """
    This is the schema for a HackerNews item.
    """
    _id: bson.ObjectId = PrivateAttr()

    id: int = Field(description="ID from HackerNews")
    deleted: bool = Field(description="Is the item deleted?", default=False)
    type: Literal["job", "story", "comment", "poll", "pollopt"] = Field(description="Type of the item")
    by: Optional[str] = Field(description="Author of the item", default=None)
    time: Optional[int] = Field(description="Creation time of the item", default=None)
    text: Optional[str] = Field(description="Text content of the item", default=None)
    dead: bool = Field(description="Is the item dead?", default=False)
    parent: Optional[int] = Field(description="Parent item", default=None)
    poll: Optional[int] = Field(description="Poll item", default=None)
    kids: Optional[List[int]] = Field(description="List of child items", default=None)
    url: Optional[str] = Field(description="URL of the item", default=None)
    score: Optional[int] = Field(description="Score of the item", default=None)
    title: Optional[str] = Field(description="Title of the item", default=None)
    parts: Optional[List[int]] = Field(description="Parts of the item", default=None)
    descendants: Optional[int] = Field(description="Number of descendants of the item", default=None)
    iso_time: Optional[datetime.datetime] = Field(description="ISO time of the item", default=None)

    @model_validator(mode="after")
    def id_is_set_if_not_deleted(cls, values):
        if not values.deleted and not values.id:
            raise ValueError("ID must be set if item is not deleted")
        return values

    class Config:
        allow_arbitrary_types = True

    def __init__(self, **data):
        super().__init__(**data)
        if "_id" not in data:
            self._id = bson.ObjectId()
        else:
            self._id = data["_id"]

        if "time" in data:
            self.iso_time = datetime.datetime.utcfromtimestamp(data["time"])

class HackerNewsUser(BaseModel):
    """
    This is the schema for a HackerNews user.
    """
    _id: bson.ObjectId = PrivateAttr()

    id: str = Field(description="ID from HackerNews")
    created: Optional[int] = Field(description="Creation time of the user", default=None)
    karma: Optional[int] = Field(description="Karma of the user", default=None)
    about: Optional[str] = Field(description="About of the user", default=None)
    delay: Optional[int] = Field(description="Delay of the user", default=None) # I don't know what this is
    # submitted: Optional[List[int]] = Field(description="List of items submitted by the user", default=None)
    iso_time: Optional[datetime.datetime] = Field(description="ISO time of the user", default=None)


    @model_validator(mode="after")
    def id_is_set(cls, values):
        if not values.id:
            raise ValueError("ID must be set")
        return values

    class Config:
        allow_arbitrary_types = True

    def __init__(self, **data):
        super().__init__(**data)
        if "_id" not in data:
            self._id = bson.ObjectId()
        else:
            self._id = data["_id"]

        if "created" in data:
            self.iso_time = datetime.datetime.utcfromtimestamp(data["created"])