import datetime
import pymongo
#import pandas as pd
import argparse
import sys
#import re
#from pandas.io.json import json_normalize
import os
import json

mongo_details = {'address': None, 'auth': True, 'project_name': 'ConspiracyTheoriesUA', 'col_name': 'preNov2020',
                 'user': input('Enter the mongo username: '), 'password': input('Enter the mongo password: ')}

date_criteria = {'start_date': None, # Date should be entered in YYYY-MM-DD format. Ex: 2019-07-01 for July 1 2019
                 'end_date': None} # If you don't want a specific start or end date for data, set start_date or end_date to None

ap = argparse.ArgumentParser()
ap.add_argument("-o", "--output-file", type=str, required=False, help="The path (including .xlsx) for the output file. If not specified, the default is the timestamp for when the data was pulled.")
args = vars(ap.parse_args())

if args['output_file']:
    output_file_name = 'data/' +  args['output_file']
elif not args['output_file']:
    output_file_name = 'data/preNov2020_tweet_type_count' + str(datetime.datetime.now().replace(microsecond=0)).replace(' ', 'T') + '.xlsx'

os.makedirs(os.path.dirname(output_file_name), exist_ok=True)

def build_mongo_connection():
    print(str(datetime.datetime.now().replace(microsecond=0)) + " Building mongo connection")
    mongoClient = pymongo.MongoClient(mongo_details['address'])
    mongoClient.admin.authenticate(mongo_details['user'], mongo_details['password'])
    databases = mongoClient.database_names()
    project_dbs = [f for f in databases if mongo_details['project_name'] in f]
    if len(project_dbs) == 0:
        print('No databases found for the specified project. Is the project name in data_pull_config.py correct?')
        sys.exit()
    elif len(project_dbs) > 2:
        print('The specified project name returns too many results. Is the project name in data_pull_config.py correct?')
        sys.exit()
    project_config_db = [f for f in project_dbs if 'Config' in f][0]
    project_config_db = mongoClient[project_config_db]['config']
    project_data_db = [f for f in project_dbs if '_' in f][0]
    project_data_db = mongoClient[project_data_db][mongo_details['col_name']]
    return project_config_db, project_data_db

project_config_db, project_data_db = build_mongo_connection()

all_tweets_cursor = project_data_db.find()

tweet_type_counts = {'original': 0, 'retweet': 0, 'quote_tweet': 0}
tweet_counter = 0
print(str(datetime.datetime.now().replace(microsecond=0)) + " Processing tweets")
for tweet in all_tweets_cursor:
    tweet_counter += 1
    tweet_type = None
    retweet_id = None
    quoted_tweet_id = None
    retweeted_quote_id = 0
    quoted_retweet_id = 0
    tweet_id = tweet['id']
    retweet_info = tweet.get('retweeted_status')
    if retweet_info:
        # Inside of this loop means there is a retweet involved
        retweet_id = retweet_info['id']
        retweeted_quote = retweet_info.get('quoted_status')
        if retweeted_quote:
            retweeted_quote_id = retweeted_quote['id']
    quoted_tweet_info = tweet.get('quoted_status')
    if quoted_tweet_info:
        # Inside of this loop means there is a quote tweet involved
        quoted_tweet_id = quoted_tweet_info['id']
        quoted_retweet = quoted_tweet_info.get('retweeted_status')
        if quoted_retweet:
            quoted_retweet_id = quoted_retweet['id']
    if retweet_id and not quoted_tweet_id:
        tweet_type = 'retweet'
    elif quoted_tweet_id and not retweet_id:
        tweet_type = 'quote_tweet'
    elif retweet_id and quoted_tweet_id:
        if retweeted_quote_id == quoted_tweet_id:
            tweet_type = 'retweet'
        if quoted_retweet_id == retweet_id:
            tweet_type = 'quote_tweet'
    if not tweet_type:
        tweet_type = 'original'
    tweet_type_counts[tweet_type] += 1
    if tweet_counter % 100000 == 0:
        print(str(datetime.datetime.now().replace(microsecond=0)) + " Processed {} tweets so far".format(tweet_counter))

print(tweet_type_counts)

with open(output_file_name.replace('.xlsx', '.txt'), 'w') as outfile:
    outfile.write(str(datetime.datetime.now().replace(microsecond=0)))
    outfile.write(json.dumps(tweet_type_counts))
