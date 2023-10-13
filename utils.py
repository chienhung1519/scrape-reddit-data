import requests
import pandas as pd
from datetime import datetime

def request_reddit_oauth(client_id, secret_token, username, password):
    """
    Add headers=headers to every request. 
    The OAuth token will expire after ~2 hours, and a new one will need to be requested.
    """
    # note that CLIENT_ID refers to "personal use script" and SECRET_TOKEN to "token"
    auth = requests.auth.HTTPBasicAuth(client_id, secret_token)

    # here we pass our login method (password), username, and password
    data = {"grant_type": "password", "username": username, "password": password}

    # setup our header info, which gives reddit a brief description of our app
    headers = {"User-Agent": "MyBot/0.0.1"}

    # send our request for an OAuth token
    res = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth, data=data, headers=headers
    )

    # convert response to JSON and pull access_token value
    TOKEN = res.json()["access_token"]

    # add authorization to our headers dictionary
    headers = {**headers, **{"Authorization": f"bearer {TOKEN}"}}

    return headers


def df_from_reddit_response(res):
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()

    # loop through each post pulled from res and append to df
    for post in res.json()["data"]["children"]:
        df = pd.concat([df, pd.DataFrame.from_records([{
            "subreddit": post["data"]["subreddit"],
            "title": post["data"]["title"],
            "selftext": post["data"]["selftext"],
            "author_fullname": post["data"]["author_fullname"] if "author_fullname" in post["data"] else None,
            "upvote_ratio": post["data"]["upvote_ratio"],
            "ups": post["data"]["ups"],
            "downs": post["data"]["downs"],
            "score": post["data"]["score"],
            "link_flair_css_class": post["data"]["link_flair_css_class"],
            "created_utc": datetime.fromtimestamp(post["data"]["created_utc"]).strftime("%Y-%m-%d-%H:%M:%S"),
            "id": post["data"]["id"],
            "kind": post["kind"]
        }])], ignore_index=True)

    return df


def df_from_praw_response(res):
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()

    # loop through each post pulled from res and append to df
    for post in res:
        df = pd.concat([df, pd.DataFrame.from_records([{
            "subreddit": post.subreddit,
            "title": post.title,
            "selftext": post.selftext,
            "author": post.author,
            "upvote_ratio": post.upvote_ratio,
            "score": post.score,
            "link_flair_text": post.link_flair_text,
            "created_utc": datetime.fromtimestamp(post.created_utc).strftime("%Y-%m-%d-%H:%M:%S"),
            "id": post.id,
            "name": post.name
        }])], ignore_index=True)

    return df