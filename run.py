import json
from pathlib import Path
import requests
import pandas as pd
import time
import sqlite3

from utils import request_reddit_oauth, df_from_reddit_response


# Load subreddit data
subreddits = Path("subreddits.txt").read_text().splitlines()

# Load secrets
secrets = json.loads(Path("secrets.json").read_text())

# Initialize params
target_num = 1000
iter_times = target_num // 100
sleep_time = 60

# Initialize database
# https://www.learncodewithmike.com/2021/05/pandas-and-sqlite.html
con = sqlite3.connect("mydatabase.db")

# Loop through subreddits
for subreddit in subreddits:
    # Initialize dataframe
    data = pd.DataFrame()
    params = {"limit": 100}
    headers = request_reddit_oauth(secrets["CLIENT_ID"], secrets["SECRET_TOKEN"], secrets["USERNAME"], secrets["PASSWORD"])

    # Loop through iter_times
    for i in range(iter_times):
        try:
            # make request
            res = requests.get(f"https://oauth.reddit.com/r/{subreddit}/new", headers=headers, params=params)
            # get dataframe from response
            new_df = df_from_reddit_response(res)
            # take the final row (oldest entry)
            row = new_df.iloc[len(new_df)-1]
            # create fullname
            fullname = row["kind"] + "_" + row["id"]
            # add/update fullname in params
            params["after"] = fullname
            
            # append new_df to data
            data = pd.concat([data, new_df], ignore_index=True)
            print(f"Added {len(new_df)} rows from {subreddit}.")
        except Exception as e:
            print(f"Failed to get data from {subreddit} after {i} times: {e}")

        # sleep
        print(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)

    # Update data into database
    try:
        res = con.execute('SELECT DISTINCT(id) FROM reddit;')
        keys = [k[0] for k in res.fetchall()]
        data = data[~data.id.isin(keys)]
    except:
        pass # table doesn't exist yet

    if len(data) > 0:
        print(f"Saving {len(data)} rows to database...")
        data.to_sql("reddit", con, if_exists="append", index=False)