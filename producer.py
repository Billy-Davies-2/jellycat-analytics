import os
import asyncio
import time
import praw
import tweepy
from facebook import GraphAPI
from nats.aio.client import Client as NATS

# API setup from environment
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent=os.getenv('REDDIT_USER_AGENT', 'jellycat_sentiment_app')
)
twitter_auth = tweepy.OAuth1UserHandler(
    os.getenv('TWITTER_API_KEY'),
    os.getenv('TWITTER_API_SECRET'),
    os.getenv('TWITTER_ACCESS_TOKEN'),
    os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
)
twitter = tweepy.API(twitter_auth)
fb = GraphAPI(access_token=os.getenv('FB_ACCESS_TOKEN'))

async def poll_and_produce():
    nats_url = os.getenv('NATS_URL', 'nats://localhost:4222')
    nc = await NATS.connect(nats_url)

    for submission in reddit.subreddit('Jellycatplush+plushies').new(limit=10):
        await nc.publish("jellycat_reviews", submission.selftext.encode())
    
    for tweet in tweepy.Cursor(twitter.search_tweets, q='jellycat -filter:retweets', lang='en', tweet_mode='extended').items(10):
        await nc.publish("jellycat_reviews", tweet.text.encode())

    posts = fb.get_connections(id='jellycatlondon', connection_name='posts', limit=10)
    for post in posts['data']:
        await nc.publish("jellycat_reviews", post.get('message', '').encode())

    await nc.drain()

def main():
    loop = asyncio.get_event_loop()
    while True:
        loop.run_until_complete(poll_and_produce())
        time.sleep(300)  # Poll every 5 minutes

if __name__ == "__main__":
    main()