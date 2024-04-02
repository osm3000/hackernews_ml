import openai
import dotenv
import os
import pandas as pd
import tqdm
import tenacity
import asyncio

dotenv.load_dotenv()

CLIENT = openai.AsyncClient()
SYSTEM_PROMPT = """
You are an expert on HackerNews data. You have been asked to analyze the data and provide insights on the most popular topics on HackerNews. You have been given a dataset with the top 1000 posts on HackerNews. You have been asked to analyze the data and provide insights on the most popular topics on HackerNews. You have been given a dataset with the top 1000 posts on HackerNews. You have been asked to analyze the data and provide insights on the most popular topics on HackerNews. You have been given a dataset with the top 1000 posts on HackerNews. You have been asked to analyze the data and provide insights on the most popular topics on HackerNews. You have been given a dataset with the top 1000 posts on HackerNews. You have been asked to analyze the data and provide insights on the most popular topics on HackerNews. You have been given a dataset with the top 1000 posts on HackerNews. You have been asked to analyze the data and provide insights on the most popular topics on HackerNews.
You will be given list of story titles. Your job is to extract the topics and trends that best describe those stories.

The following topics are of particular interest:
- ai and machine learning
- cybersecurity
- web programming
- cryptocurrency
- blockchain
- fintech
- rust
- c++
- python
- javascript
- health technology
- military technology
- virtual reality
- database
- internet of things
- quantum computing
- transportation technology
- space technology
- nanotechnology
- biotechnology
- robotics
- 3d printing
- drones
- autonomous vehicles
- wearables
- medtech
- agritech
- edtech
- legal tech
- energy technology
- creative technology
- cloud computing
- networks
- business
- lifestyle
- entertainment
- self-development
- hardware technology
- politics
- death news
- science
- travel
- finance
- mental health
- philosophy
- design
- social media
- sports
- education
- environment
- privacy

Choose the topic that is more relevant to the title. If none of the topics are suitable, then introduce a new topic, but few as possible.

Your response will be in the following format:
<Story Title>: <The most dominant topic>
"""

@tenacity.retry(wait=tenacity.wait_random_exponential(multiplier=1, max=60), stop=tenacity.stop_after_attempt(5))
async def create_completion(messages):
    global CLIENT
    print("Creating completion")
    response = await CLIENT.chat.completions.create(
            model="gpt-4-0125-preview",
            messages=messages,
            max_tokens=2048,
        )
    return response.choices[0].message.content

async def simple_call():
    global SYSTEM_PROMPT
    # Load the dataset
    df = pd.read_csv("data/hackernews.hackernews_items.csv")
    df["topic"] = None 

    # Get the story titles
    story_titles = df["title"].tolist()

    chunk_size = 100
    for i in tqdm.tqdm(range(0, len(story_titles), chunk_size)):
        chunk = story_titles[i : i + chunk_size]
        user_prompt = """
        Story titles:
        {}
        """.format(
            "\n".join(chunk)
        )
        # Get the topics for each story title
        messages=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {"role": "user", "content": user_prompt},
        ]

        clean_response = await create_completion(messages)

        # Add the topic to the dataframe
        for line_index, line in enumerate(clean_response.split("\n")):
            df.loc[i + line_index, "topic"] = line.split(":")[-1].strip()

        # print(df.loc[i : i + chunk_size - 1, ["title", "topic"]])

        cnt += 1

    # Print the value counts of the topics
    print(df["topic"].value_counts())

    # Save the dataframe
    df.to_csv("data/hackernews_topic.csv", index=False)


async def multiple_async_calls():
    global SYSTEM_PROMPT
    # Load the dataset
    df = pd.read_csv("data/hackernews.hackernews_items.csv")
    df["topic"] = None

    # Get the story titles
    story_titles = df["title"].tolist()#[0:1000]
    chunk_size = 100

    all_messages = []
    for i in range(0, len(story_titles), chunk_size):
        chunk = story_titles[i : i + chunk_size]
        user_prompt = """
        Story titles:
        {}
        """.format(
            "\n".join(chunk)
        )
        all_messages.append(
            [
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT,
                },
                {"role": "user", "content": user_prompt},
            ]
        )

    responses = await asyncio.gather(
        *[create_completion(messages) for messages in all_messages]
    )

    # Add the topic to the dataframe
    for response, i in zip(responses, range(0, len(story_titles), chunk_size)):
        for line_index, line in enumerate(response.split("\n")):
            df.loc[i + line_index, "topic"] = line.split(":")[-1].strip()

    # Post processing
    df = df[~df['topic'].isnull()]
    df["topic"] = df["topic"].apply(lambda x: x.lower())
    df["topic"] = df["topic"].str.replace("**", "")
    df["topic"] = df["topic"].str.replace(">", "")
    df["topic"] = df["topic"].str.replace("<", "")
    df['topic'] = df['topic'].str.replace(r"\s\(.+\)", "", regex=True)
    df['topic'] = df['topic'].str.replace("programming languages", "programming", case=False)

    # Print the value counts of the topics
    print(df["topic"].value_counts())

    # Save the dataframe
    df.to_csv("data/hackernews_topic.csv", index=False)


if __name__ == "__main__":
    # asyncio.run(simple_call())
    asyncio.run(multiple_async_calls())
