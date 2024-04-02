"""
Trying nomic:
1. Get the embeddings for the dataset using OpenAI's API
2. Use the embeddings to create a Nomic atlas for the dataset
"""
import pandas as pd
from nomic import atlas
import numpy as np
from nomic import AtlasDataset

LOAD_FRESH = False
if LOAD_FRESH:
    data_atlas = AtlasDataset(identifier="lumbering-boltzmann")

    data = data_atlas.maps[0].data.df
    topics = data_atlas.maps[0].topics.df

    # Concatenate the data and topics - side by side
    full_data = pd.concat([data, topics], axis=1)
    print(full_data.head())

    # Save the full data
    full_data.to_csv("data/nomic_full_data.csv", index=False)
else:
    full_data = pd.read_csv("data/nomic_full_data.csv")

############################################
# Processing the data
############################################
# We will use topic_depth_2 as the main topic, when there is "Miscellaneous" or null in topic_depth_2, we will use topic_depth_1
full_data.loc[full_data["topic_depth_2"].isna(), "topic_depth_2"] = full_data.loc[
    full_data["topic_depth_2"].isna(), "topic_depth_1"
]
full_data.loc[
    full_data["topic_depth_2"].str.contains("Miscellaneous"), "topic_depth_2"
] = full_data.loc[
    full_data["topic_depth_2"].str.contains("Miscellaneous"), "topic_depth_1"
]

print(full_data)
# print(full_data["topic_depth_2"].value_counts())

# Any topic_depth_2 that contains the following pattern (digit), remove that pattern
full_data["topic_depth_2"] = full_data["topic_depth_2"].str.replace(r"\ \(\d+\)", "", regex=True)

# Print the value counts of the topics
for item_name, item in zip(full_data["topic_depth_2"].value_counts().index, full_data["topic_depth_2"].value_counts()):
    print(f"{item_name}: {item}")

print(full_data["topic_depth_2"].value_counts())