import asyncio
import time
import praw
import tweepy
from facebook_graph_api import GraphAPI
from nats.aio.client import Client as NATS

# api setup 
reddit = praw.Reddit(client_id='REDDIT_ID',
                     client_secret='REDDIT_SECRET',
                     user_agent='jellycat_sentiment_app')
twitter_auth = tweepy.OAuth1UserHandler('TWITTER_API_KEY',
                                        'TWITTER_API_SECRET',
                                        'TWITTER_ACCESS_TOKEN',
                                        'TWITTER_ACCESS_TOKEN_SECRET')
twitter = tweepy.API(twitter_auth)
fb = GraphAPI(access_token='FB_ACCESS_TOKEN')

async def poll_and_produce():
    nc = await NATS.connect("nats://localhost:4222")

    for submission in reddit.subreddit('Jellycatplush+plushies').new(limit=10):
        await nc.publish("jellycat_reviews", submission.id.encode(), payload=submission.selftext.encode())
    
    for tweet in tweepy.Cursor(twitter.search_tweets, q='jellycat -filter:retweets', lang='en', tweet_mode='extended').items(10):
        await nc.publish("jellycat_reviews", str(tweet.id).encode(), payload=tweet.text.encode())

    posts = fb.get_connections(id='jellycatlondon', connection_name='posts', limit=10)
    for post in posts['data']:
        await nc.publish("jellycat_reviews", post['id'].encode(), payload=post.get('message', '').encode())

    await nc.drain()

loop = asyncio.get_event_loop()
while True:
    loop.run_until_complete(poll_and_produce())
    time.sleep(300)  # Poll every 5 minutes