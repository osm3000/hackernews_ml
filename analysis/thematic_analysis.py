import plotly.express as px
import pandas as pd
import datetime

data = pd.read_csv("data/hackernews_topic.csv")
print(data.shape)

data["topic"] = data["topic"].str.replace(
    "programming languages", "software development", case=False
)
data["topic"] = data["topic"].str.replace(
    "programming", "software development", case=False
)
data["topic"] = data["topic"].str.replace(
    "web programming", "software development", case=False
)
data["topic"] = data["topic"].str.replace(
    "web software development", "software development", case=False
)
# data = data[data["topic"] != "business"]
# Get the top 200 topics only, and filter out the rest
# top_topics = data["topic"].value_counts().head(200).index.tolist()
top_topics = data["topic"].value_counts().head(10).index.tolist()
data = data[data["topic"].isin(top_topics)]
print(data.shape)

data["time_iso"] = pd.to_datetime(data["time"], unit="s")
# data["time_iso"] = data["time_iso"].dt.tz_localize("UTC").dt.tz_convert("US/Pacific")

data['year'] = data['time_iso'].dt.year

# Exclude the year 2024
data = data[data["year"] != 2024]
data = data[data["year"] != 2006]


# Create a pivot table to count the number of stories per year and topic
# relevant_table = data.pivot_table(index="year", columns="topic", aggfunc="count", normalize='index')
relevant_table = pd.crosstab(data["year"], data["topic"], normalize='index')
relevant_table = relevant_table.fillna(0)
relevant_table *= 100
relevant_table = relevant_table.round(2)

# Convert the table to a long format
relevant_table = relevant_table.reset_index()
relevant_table = pd.melt(relevant_table, id_vars=["year"], var_name="topic", value_name="percentage")

# Plot the data
# fig = px.bar(
#     relevant_table,
#     y="topic",
#     x="percentage",
#     animation_frame="year",
#     title="Number of stories per year by topic",
#     range_x=[0, 20],
#     orientation="h",
# )

# fig.show()

# Plot a heatmap: Topics on the y-axis, years on the x-axis, and the percentage of stories as the color
# fig = px.imshow(
#     relevant_table.pivot(index="topic", columns="year", values="percentage"),
#     title="Percentage of stories per year by topic",
#     labels=dict(x="Year", y="Topic", color="Percentage"),
# )

# fig.show()

# print(relevant_table.pivot(index="topic", columns="year", values="percentage"))

# Make a line plot for the top 20 topics.
# The x-axis is the year, and the y-axis is the percentage of stories
font_settings = dict(
    family="Courier New, monospace",
    size=20,
)

fig = px.line(
    relevant_table,
    x="year",
    y="percentage",
    color="topic",
    title="Top-10 topics trends on Hacker News",
    log_y=True,
)
# Set the x-axis to linear
fig.update_xaxes(tickmode="linear")

# Set the x-axis and y-axis titles
fig.update_xaxes(title_text="Year")
fig.update_yaxes(title_text="Percentage per year, log scale")

# Increase the line width
fig.update_traces(line=dict(width=3))

# Set the font settings
fig.update_layout(
    font=font_settings,
)

# Set the figure size
fig.update_layout(
    autosize=False,
    width=1900,
    height=1000,
)

fig.show()

# Save the figure
fig.write_image("images/hackernews_topic_percentage.png")