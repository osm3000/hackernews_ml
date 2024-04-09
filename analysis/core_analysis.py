import pymongo
import os
from contextlib import contextmanager
import dotenv
import pandas as pd
import plotly.express as px
import numpy as np
import time

dotenv.load_dotenv()

font_settings = dict(
    family="Courier New, monospace",
    size=10,
)

@contextmanager
def get_mongo_client():
    client = pymongo.MongoClient(os.getenv("MONGO_DB_URL"))
    try:
        yield client["hackernews"]["hackernews_items"]
    finally:
        client.close()

class AnyQueries:

    def __init__(self, fresh=False, query_name=""):
        self.query_folder = "./queries"
        self.data_folder = "./data"
        self.df = pd.DataFrame()
        self.fresh = fresh
        self.data_file = query_name + ".csv"
        self.query_file = query_name + ".txt"

        if fresh:
            self.query = self.load_query()
            # Execute the query
            with get_mongo_client() as client:
                self.result = client.aggregate(self.query)
                self.df = pd.DataFrame(list(self.result))

            # Save the result to a file
            self.save_data()

        else:
            self.df = self.load_data()

    def load_query(self):
        with open(os.path.join(self.query_folder, self.query_file), "r") as f:
            return eval(f.read())

    def load_data(self):
        with open(os.path.join(self.data_folder, self.data_file), "r") as f:
            return pd.read_csv(f)

    def save_data(self):
        with open(os.path.join(self.data_folder, self.data_file), "w") as f:
            self.df.to_csv(f)

    def get_df(self):
        return self.df


def analyze_contributions_vs_score():
    contributions = AnyQueries(fresh=False, query_name="contributions_vs_score")
    df = contributions.get_df()
    print(df.shape)

    ##################################################################
    # How many users show up (first contribution) per year?
    ##################################################################
    # Plot the histogram of the year of the first contribution of the top users
    view_of_interest = (
        df[df['first_appearance'] < 2023] # Remove this year and last year, in order no to mix the findings.
        .groupby("first_appearance")
        .agg({"author_name": "count"})
        .reset_index()
    )
    view_of_interest = view_of_interest.sort_values("first_appearance")

    fig = px.bar(
        view_of_interest,
        x="first_appearance",
        y="author_name",
        title="Number of New users (who contributed stories) showing up per Year",
    )
    fig.update_xaxes(title_text="Year of First Appearance")
    fig.update_yaxes(title_text="Number of Users")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # fig.show()

    # Count the number of new user showing up per year, and the number of users last showing up per year

    view_of_interest_last = (
        df[df["last_appearance"] < 2024] # Remove this year only, as it is not finished yet
        .groupby("last_appearance")
        .agg({"author_name": "count"})
        .reset_index()
    )
    view_of_interest_last = view_of_interest_last.sort_values("last_appearance")

    view_of_interest_first = (
        df[
            (df["first_appearance"] < 2024) #& (~df["last_appearance"].isin([2023, 2024]))
            # ~(
            #     (df["first_appearance"] == 2023) & (df["last_appearance"] == 2023)
            # )  # .any( axis=1)
            # & (df["last_appearance"] < 2024)
        ]  # Remove this year and last year, in order no to mix the findings.
        .groupby("first_appearance")
        .agg({"author_name": "count"})
        .reset_index()
    )
    view_of_interest_first = view_of_interest_first.sort_values("first_appearance")

    # Merge the two data-frames
    view_of_interest = view_of_interest_first.merge(
        view_of_interest_last,
        how="outer",
        # how="inner",
        left_on="first_appearance",
        right_on="last_appearance",
        suffixes=("_first", "_last"),
    )
    view_of_interest = view_of_interest.rename(
        columns={"author_name_first": "new_users", "author_name_last": "last_users"}
    )
    print(view_of_interest)
    view_of_interest = view_of_interest.drop("last_appearance", axis=1)
    view_of_interest = view_of_interest.rename(columns={"first_appearance": "year"})
    view_of_interest = view_of_interest.sort_values("year")
    # view_of_interest = view_of_interest[view_of_interest["year"] < 2024]
    # print(view_of_interest)

    fig = px.line(
        view_of_interest,
        x="year",
        y=["new_users", "last_users"],
        title="Number of New and Last users showing up per Year",
    )
    fig.update_xaxes(title_text="Year of Appearance")
    fig.update_yaxes(title_text="Number of Users")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # Show all the xticks
    fig.update_xaxes(tickmode="linear")

    # Rename the y-axis legends
    newnames = {
        "new_users": "First-seen users",
        "last_users": "Last-seen users<br>(not active for<br>>= year)",
    }
    fig.for_each_trace(
        lambda t: t.update(
            name=newnames[t.name],
            legendgroup=newnames[t.name],
            hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name]),
        )
    )
    # Name the legend, and change the figure size
    fig.update_layout(
        legend=dict(
            title="Type of Users",
            # orientation="h",
            # yanchor="middle",
            # y=-0.4,
            # xanchor="center",
            # x=1,
        ),
        width=800,
        height=600,

    )

    fig.write_image("new_and_last_users_per_year.png")
    # exit()
    ##################################################################
    # How long do users stay active?
    ##################################################################
    # Plot the histogram of the year of the first contribution of the top users
    df["active_years"] = df["last_appearance"] - df["first_appearance"]
    print(df.shape)
    print(
        df[
            ~((df["first_appearance"] == 2023) & (df["last_appearance"] == 2023))#.any( axis=1)
            & (df["last_appearance"] < 2024)
        ].shape
    )
    view_of_interest = (
        df[
            ~(
                (df["first_appearance"] == 2023) & (df["last_appearance"] == 2023)
            )  # .any( axis=1)
            & (df["last_appearance"] < 2024)
        ]
        # df
        .groupby("active_years")
        .agg({"author_name": "count"})
        .reset_index()
    )
    view_of_interest = view_of_interest.sort_values("active_years")
    view_of_interest.rename(columns={"author_name": "nb_of_users"}, inplace=True)
    expected_user_lifetime = (view_of_interest["active_years"] ) * view_of_interest["nb_of_users"] / view_of_interest["nb_of_users"].sum()
    expected_user_lifetime = expected_user_lifetime.sum().round(2)

    fig = px.bar(
        view_of_interest,
        x="active_years",
        y="nb_of_users",
        title="Number of Users per Number of Active Years",
    )
    # Adding a line at the median
    fig.add_shape(
        type="line",
        x0=expected_user_lifetime,
        y0=0,
        x1=expected_user_lifetime,
        y1=400_000,
        line=dict(color="red", width=2, dash="dash"),
        label=dict(
            text=f"Expected user lifetime: {expected_user_lifetime} years",
            textposition="end",
            font=dict(size=12, color="red"),
            yanchor="top",
            xanchor="left",
            textangle=0,
        ),
    )
    fig.update_xaxes(title_text="Number of Active Years")
    fig.update_yaxes(title_text="Number of Users")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # fig.show()
    fig.write_image("active_years_histogram.png")
    # exit()

    # Plot the author_total_score vs nb_of_stories
    fig = px.scatter(
        df,
        y="author_total_score",
        x="nb_of_stories",
        log_y=True,
        title="(Score) User Cumulative Upvote<br>VS<br>(Consistency) Number of Stories they have contributed",
        trendline="ols",
        trendline_color_override="red",
    )
    # Rename the x-axis and y-axis
    fig.update_xaxes(title_text="(Consistency) Number of Stories shared per User")
    fig.update_yaxes(title_text="(Score) User Cumulative Upvote, log scale")
    # Center the title
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    fig.show()
    # Save the figure
    fig.write_image("nb_of_stories_vs_author_total_score.png")

    # # Plot the histogram of the author_total_score with non-zero values
    fig = px.histogram(
        df[df["author_total_score"] > 0],
        x="author_total_score",
        title="Histogram of User Cumulative Upvote",
        nbins=100,
        log_y=True
    )
    fig.update_xaxes(title_text="User's Cumulative Upvote")
    fig.update_yaxes(title_text="Frequency of Authors (log scale)")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # fig.show()
    # Save the figure
    fig.write_image("author_cum_score_histogram.png")

    # # Plot the histogram of the nb_of_stories
    fig = px.histogram(
        df,
        x="nb_of_stories",
        title="Histogram of Number of Stories shared per User",
        nbins=100,
        log_y=True,
    )
    fig.update_xaxes(title_text="Number of Stories shared per User")
    fig.update_yaxes(title_text="Frequency of Authors (log scale)")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)

    # fig.show()
    # Save the figure
    fig.write_image("nb_of_stories_histogram.png")

    # Plot the role of luck: the median upvote of the user vs the cumulative upvote of the user
    df["identical"] = df["author_total_score"] == df["author_median_score"]
    df = df[df["author_total_score"] > 0]
    df["log_nb_of_stories"] = np.log(df["nb_of_stories"])

    # Plot the role of luck: the median upvote of the user vs the cumulative upvote of the user
    fig = px.scatter(
        df,
        x="author_total_score",
        y="author_median_score",
        color="identical",
        # color="nb_of_stories",
        # color="log_nb_of_stories",
        log_x=True,
        # log_y=True,
        title="User Cumulative Upvote vs User Median Upvote",
        hover_name="author_name",
    )
    fig.update_xaxes(title_text="User Cumulative Upvote (log scale)")
    fig.update_yaxes(title_text="User Median Upvote")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # fig.show()
    fig.write_image("author_cum_score_vs_median_score_logscale_identical.png")

    # Remove the identical points, and make the same plot again
    # Plot the role of luck: the median upvote of the user vs the cumulative upvote of the user
    df['lucky'] = (df['author_total_score'] >= df['author_median_score']) & (df['author_total_score'] <= df['author_median_score'] * 10) & (df['author_total_score'] > 500)
    fig = px.scatter(
        df[df["identical"] == False],
        x="author_total_score",
        y="author_median_score",
        # color="lucky",
        color="log_nb_of_stories",
        log_x=True,
        # log_y=True,
        title="(Score) User Cumulative Upvote vs (Impact) User Median Upvote",
        hover_name="author_name",
    )
    fig.update_xaxes(title_text="(Score) User Cumulative Upvote, log scale")
    fig.update_yaxes(title_text="(Impact) User Median Upvote")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    fig.update_layout(
        coloraxis_colorbar=dict(
            title="(Consistency)<br>Nb of stories<br>shared per<br>user, log scale",
        )
    )
    fig.add_shape(
        type="line",
        # showlegend=True,
        x0=1000,
        y0=0,
        x1=1000,
        y1=1000,
        line=dict(color="green", width=2, dash="dash"),
        label=dict(
            text="End-of-luck wall",
            textposition="end",
            font=dict(size=15, color="red"),
            yanchor="top",
            textangle = 0
        ),
    )
    # fig.show()
    fig.write_image("author_cum_score_vs_median_score_logscale_lucky.png")


def analyze_type_per_time():
    type_per_time = AnyQueries(fresh=False, query_name="freq_of_type_per_time")
    df = type_per_time.get_df()
    print(df.shape)
    # df = df[(df['year'] < 2024) & (df['year'] > 2006)]

    ##################################################################
    # Bar plot of the frequency of each type of item per year
    ##################################################################
    view_of_interest = (
        df[(df["year"] < 2024) & (df["year"] > 2006)]
        .groupby(["year", "type"])
        .sum()
        .reset_index()
    )

    # Plot a figure for each type of item
    for type_group in [["story"], ["comment"], ["poll", "pollopt", "job"]]:
        fig = px.bar(
            view_of_interest[view_of_interest["type"].isin(type_group)],
            x="year",
            y="freq",
            color="type" if len(type_group) > 1 else None,
            barmode="group",
            title=f"Frequency of {', '.join(type_group)} items shared per year",
        )
        fig.update_xaxes(title_text="Year")
        fig.update_yaxes(title_text="Frequency")
        fig.update_layout(title_x=0.5)
        fig.update_xaxes(tickmode="linear")
        fig.update_layout(
            font=font_settings
        )
        # fig.show()
        # Save the figure
        fig.write_image(f"freq_of_{type_group[0]}_per_year.png")

    ##################################################################
    # Basic: plot the percentage number of each item shared, in total
    ##################################################################
    view_of_interest = (
        df.groupby("type")
        .agg({"freq": "sum"})
        .reset_index()
    )
    view_of_interest["percentage"] = view_of_interest["freq"] / view_of_interest["freq"].sum() * 100
    view_of_interest = view_of_interest.sort_values("percentage", ascending=False)
    view_of_interest['percentage'] = view_of_interest['percentage'].round(2)
    # view_of_interest = view_of_interest[view_of_interest['type'] in ['story', 'comment']]
    print(view_of_interest)

    fig = px.pie(
        view_of_interest,
        values="percentage",
        names="type",
        title="Percentage of each type of item shared",
        labels={"type": "Type of Item", "percentage": "Percentage of Item Shared"},
    )
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(title_x=0.5)
    fig.update_layout(
        font=font_settings
    )
    # fig.show()
    # Save the figure
    fig.write_image("percentage_of_type_of_item_shared.png")

def analyze_commentators_behavior():
    df = AnyQueries(fresh=False, query_name="commentators_behavior").get_df()
    print(df.shape)

    ##################################################################

    df["active_years"] = df["last_appearance"] - df["first_appearance"]
    print(df.shape)
    print(
        df[
            ~(
                (df["first_appearance"] == 2023) & (df["last_appearance"] == 2023)
            )  # .any( axis=1)
            & (df["last_appearance"] < 2024)
        ].shape
    )
    view_of_interest = (
        df[
            ~(
                (df["first_appearance"] == 2023) & (df["last_appearance"] == 2023)
            )  # .any( axis=1)
            & (df["last_appearance"] < 2024)
        ]
        # df
        .groupby("active_years")
        .agg({"author_name": "count"})
        .reset_index()
    )
    view_of_interest = view_of_interest.sort_values("active_years")
    view_of_interest.rename(columns={"author_name": "nb_of_users"}, inplace=True)
    expected_user_lifetime = (
        (view_of_interest["active_years"])
        * view_of_interest["nb_of_users"]
        / view_of_interest["nb_of_users"].sum()
    )
    expected_user_lifetime = expected_user_lifetime.sum().round(2)

    fig = px.bar(
        view_of_interest,
        x="active_years",
        y="nb_of_users",
        title="Number of Users (commentators) per Number of Active Years",
    )
    # Adding a line at the median
    fig.add_shape(
        type="line",
        x0=expected_user_lifetime,
        y0=0,
        x1=expected_user_lifetime,
        y1=400_000,
        line=dict(color="red", width=2, dash="dash"),
        label=dict(
            text=f"Expected user lifetime: {expected_user_lifetime} years",
            textposition="end",
            font=dict(size=12, color="red"),
            yanchor="top",
            xanchor="left",
            textangle=0,
        ),
    )
    fig.update_xaxes(title_text="Number of Active Years")
    fig.update_yaxes(title_text="Number of Users")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # fig.show()
    fig.write_image("commentators_active_years_histogram.png")

    ##################################################################
    view_of_interest_last = (
        df[
            df["last_appearance"] < 2024
        ]  # Remove this year only, as it is not finished yet
        .groupby("last_appearance")
        .agg({"author_name": "count"})
        .reset_index()
    )
    view_of_interest_last = view_of_interest_last.sort_values("last_appearance")

    view_of_interest_first = (
        df[
            (
                df["first_appearance"] < 2024
            )  # & (~df["last_appearance"].isin([2023, 2024]))
        ]  # Remove this year and last year, in order no to mix the findings.
        .groupby("first_appearance")
        .agg({"author_name": "count"})
        .reset_index()
    )
    view_of_interest_first = view_of_interest_first.sort_values("first_appearance")

    # Merge the two dataframes
    view_of_interest = view_of_interest_first.merge(
        view_of_interest_last,
        how="outer",
        left_on="first_appearance",
        right_on="last_appearance",
        suffixes=("_first", "_last"),
    )
    view_of_interest = view_of_interest.rename(
        columns={"author_name_first": "new_users", "author_name_last": "last_users"}
    )
    print(view_of_interest)
    view_of_interest = view_of_interest.drop("last_appearance", axis=1)
    view_of_interest = view_of_interest.rename(columns={"first_appearance": "year"})
    view_of_interest = view_of_interest.sort_values("year")
    # view_of_interest = view_of_interest[view_of_interest["year"] < 2024]
    # print(view_of_interest)

    fig = px.line(
        view_of_interest,
        x="year",
        y=["new_users", "last_users"],
        title="Number of New and Last users (commentators) showing up per Year",
    )
    fig.update_xaxes(title_text="Year of Appearance")
    fig.update_yaxes(title_text="Number of Users")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # Show all the xticks
    fig.update_xaxes(tickmode="linear")
    # Rename the y-axis legends
    newnames = {
        "new_users": "First-seen users",
        "last_users": "Last-seen users<br>(not active for<br>>= year)",
    }
    fig.for_each_trace(
        lambda t: t.update(
            name=newnames[t.name],
            legendgroup=newnames[t.name],
            hovertemplate=t.hovertemplate.replace(t.name, newnames[t.name]),
        )
    )
    # Name the legend, and change the figure size
    fig.update_layout(
        legend=dict(
            title="Type of Users",
            # orientation="h",
            # yanchor="middle",
            # y=-0.4,
            # xanchor="center",
            # x=1,
        ),
        width=800,
        height=600,
    )
    # fig.show()
    fig.write_image("commentators_new_and_last_users_per_year.png")

def analyze_contributions_vs_score_v2():
    time_start = time.time()
    df = AnyQueries(
        fresh=False,
        query_name="contributions_vs_karma_vs_score",
        # fresh=False, query_name="comments_per_year"
    ).get_df()
    print(f"Time to get the data: {time.time() - time_start:.2f} seconds")
    print(df.shape)

    # Plot the role of luck: the median upvote of the user vs the cumulative upvote of the user
    df["identical"] = df["author_total_score"] == df["author_median_score"]
    df = df[df["author_total_score"] > 0]
    df = df[df["karma"] > 10]
    df["log_nb_of_stories"] = np.log(df["nb_of_stories"])

    ##################################################################
    # Number of stories shared per user vs the user's karma
    ##################################################################
    # Plot the author_total_score vs nb_of_stories
    fig = px.scatter(
        df,
        y="karma",
        x="nb_of_stories",
        log_y=True,
        title="User karma vs Number of Stories they have contributed",
        trendline="ols",
        trendline_color_override="red",
    )
    # Rename the x-axis and y-axis
    fig.update_xaxes(title_text="Number of Stories shared per User")
    fig.update_yaxes(title_text="User's Karma (log scale)")
    # Center the title
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    fig.show()
    # Save the figure
    fig.write_image("author_karma_vs_nb_of_stories.png")

    # Plot the role of luck: the median upvote of the user vs the cumulative upvote of the user
    fig = px.scatter(
        df,
        x="karma",
        y="author_median_score",
        log_x=True,
        # log_y=True,
        title="User's karma vs User's median upvote (luck)",
        hover_name="author_name",
    )
    fig.update_xaxes(title_text="User's karma (log scale)")
    fig.update_yaxes(title_text="User median upvote - Luck")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    # fig.show()
    fig.write_image("author_karma_vs_median_score_logscale_identical.png")

    # Remove the identical points, and make the same plot again
    # Plot the role of luck: the median upvote of the user vs the cumulative upvote of the user
    fig = px.scatter(
        # df[(df["karma"] > 0) & (df["author_median_score"] > 0)],
        df,
        x="karma",
        y="author_median_score",
        # color="lucky",
        color="log_nb_of_stories",
        log_x=True,
        # log_y=True,
        title="User's karma vs User Median Upvote",
        hover_name="author_name",
        # trendline="lowess",
        # trendline_options=dict(log_x=True, log_y=True),
    )
    fig.update_xaxes(title_text="User's karma (log scale)")
    fig.update_yaxes(title_text="User median upvote - Luck")
    fig.update_layout(title_x=0.5)
    fig.update_layout(font=font_settings)
    fig.update_layout(
        coloraxis_colorbar=dict(
            title="Nb of stories<br>shared per<br>user(log scale)",
        )
    )
    # fig.add_shape(
    #     type="line",
    #     # showlegend=True,
    #     x0=1000,
    #     y0=0,
    #     x1=1000,
    #     y1=df["author_median_score"].max(),
    #     line=dict(color="green", width=2, dash="dash"),
    #     label=dict(
    #         text="End-of-luck wall",
    #         textposition="end",
    #         font=dict(size=15, color="red"),
    #         yanchor="top",
    #         textangle=0,
    #     ),
    # )
    # fig.show()
    fig.write_image("author_karma_vs_median_score_logscale_lucky.png")

    # fig = px.scatter(
    #     df,
    #     x="karma",
    #     y="author_mean_score",
    #     # color="lucky",
    #     color="log_nb_of_stories",
    #     log_x=True,
    #     # log_y=True,
    #     title="User Cumulative Upvote vs User Mean Upvote (excluding identical points)",
    #     hover_name="author_name",
    # )
    # fig.update_xaxes(title_text="User's Karma (log scale)")
    # fig.update_yaxes(title_text="User Mean Upvote")
    # fig.update_layout(title_x=0.5)
    # fig.update_layout(font=font_settings)
    # fig.update_layout(
    #     coloraxis_colorbar=dict(
    #         title="Nb of stories<br>shared per<br>user(log scale)",
    #     )
    # )
    # fig.add_shape(
    #     type="line",
    #     # showlegend=True,
    #     x0=1000,
    #     y0=0,
    #     x1=1000,
    #     y1=df["author_mean_score"].max(),
    #     line=dict(color="green", width=2, dash="dash"),
    #     label=dict(
    #         text="End-of-luck wall",
    #         textposition="end",
    #         font=dict(size=15, color="red"),
    #         yanchor="top",
    #         textangle=0,
    #     ),
    # )
    # fig.show()
    # fig.write_image("author_karma_vs_mean_score_logscale_lucky.png")

def main():
    analyze_contributions_vs_score()
    analyze_type_per_time()
    analyze_commentators_behavior()
    analyze_contributions_vs_score_v2()

if __name__ == "__main__":
    main()
