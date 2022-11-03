import json
import pandas as pd


def convert_to_df(file):
    """
    Takes a CSV file and returns a Pandas dataframe
    """
    return pd.read_csv(file)


def flatten_raw_df(df):
    """
    Takes the pandas dataframe of keystroke match data and flattens the 'match_element' column which is JSON
    """
    # Convert match_element column to json but replace the single quotes with double quotes so that json.loads works
    json_column = df["match_element"].str.replace("'", '"').apply(json.loads)
    # Replace original column with json column
    df["match_element"] = json_column
    # Flatten the json column
    flattened_json_column = pd.json_normalize(df["match_element"])
    # Join back to original df
    flattened_df = df[["match_id", "message_id"]].join(flattened_json_column)
    return flattened_df


def clean(df):
    """
    Takes a flattened dataframe of keystroke match data and returns a dataframe of serve data
    """
    # Grab relevant columns from df - match_id and message_id are PKs(?)
    events_df = df[["match_id", "message_id", "eventElementType", "details.scoredBy"]]
    # Get the relevant events from eventElementType
    cleaned = events_df[
        events_df["eventElementType"].isin(
            ["PhysioCalled", "PointLet", "PointFault", "PointScored"]
        )
    ]
    # Get server information (with PKs)
    servers = df[["match_id", "message_id", "server.team"]]
    # Perform hot encoding on event type and score columns
    hot_encoding = pd.get_dummies(
        cleaned[["match_id", "message_id", "eventElementType", "details.scoredBy"]]
    )
    # Join servers and hot encoded dfs
    serves_df = servers.merge(
        hot_encoding,
        how="inner",
        left_on=["match_id", "message_id"],
        right_on=["match_id", "message_id"],
    ).drop(columns=["eventElementType_PointScored"])
    # Add serve_id column but partition by the match_id
    serves_df["serve_id"] = (
        serves_df.sort_values(["match_id", "message_id"], ascending=[True, True])
        .groupby(["match_id"])
        .cumcount()
        + 1
    )
    # Rearrange the serve_id so it's after the physio column, but before the outcome of the serve columns (as per the PDF)
    serve_id_col = serves_df.pop("serve_id")
    serves_df.insert(4, "serve_id", serve_id_col)
    return serves_df


def enrich(df):
    """
    Takes a dataframe of serve data and adds an additional serve_number column to state whether or not the serve is a
    first or second serve
    """
    enriched_df = df.sort_values(by=["match_id", "message_id"], ascending=[True, True])
    # Add serve_number column with default value 1 (first serve)
    enriched_df.insert(5, "serve_number", 1)
    # Iterating over rows.. better to use iterrows()? Read somewhere that you shouldn't iterate over rows in Pandas at all!
    for row_num in range(1, len(enriched_df)):
        # If the previous serve was a fault, the next serve is a second.
        if enriched_df.loc[row_num - 1, "eventElementType_PointFault"] == 1:
            enriched_df.loc[row_num, "serve_number"] = 2
        # If the previous serve was let, the next serve is the same serve number.
        if enriched_df.loc[row_num - 1, "eventElementType_PointLet"] == 1:
            enriched_df.loc[row_num, "serve_number"] = enriched_df.loc[
                row_num - 1, "serve_number"
            ]
    return enriched_df


def find_scores(sets_score):
    """
    Takes a list of set scores and returns a dictionary of Team A and B's scores.

    !! Ignores tie break scores !!
    """
    scores = {"A": 0, "B": 0}
    for set in sets_score:
        score_A = set["gamesA"]
        score_B = set["gamesB"]
        if score_A > score_B:
            scores["A"] += 1
        else:
            scores["B"] += 1
    return scores


def transform(df):
    """
    Takes a flattened dataframe of keystroke match data and repairs the set score columns for Team A and B
    """
    # Filter out PointScored event types
    pointscored_df = df[df["eventElementType"] == "PointScored"]
    # Grab relevant columns from df - match_id and message_id are PKs(?)
    scores_df = pointscored_df[
        [
            "match_id",
            "message_id",
            "score.previousSetsScore",
            "score.overallSetScore.setsA",
            "score.overallSetScore.setsB",
        ]
    ]
    # Iterate over rows
    for i, row in scores_df.iterrows():
        prev_sets = row["score.previousSetsScore"]
        if len(prev_sets) > 0:
            # Note: ignores tie break data!!
            scores = find_scores(prev_sets)
            scores_df.loc[i, "score.overallSetScore.setsA"] = scores["A"]
            scores_df.loc[i, "score.overallSetScore.setsB"] = scores["B"]

    return scores_df


if __name__ == "__main__":
    raw_data = convert_to_df(f"../data/keystrokes-for-tech-test.csv")
    flattened_data = flatten_raw_df(raw_data)
    cleaned_data = clean(flattened_data)
    enriched_data = enrich(cleaned_data)
    transformed_data = transform(flattened_data)
    print(enriched_data)
    print(transformed_data)
