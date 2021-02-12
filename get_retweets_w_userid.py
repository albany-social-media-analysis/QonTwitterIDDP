import datetime
import pymongo
import pandas as pd
import argparse
import sys
from pandas.io.json import json_normalize
import os
import matplotlib.pyplot as plt

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
    output_file_name = 'data/preNov2020_retweets_w_userid/' + str(datetime.datetime.now().replace(microsecond=0)).replace(' ', 'T') + '.xlsx'

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

aggregation_pipeline = [{'$match': {'retweeted_status': {'$exists': True}}}, {'$group': {'_id': '$retweeted_status.id_str', 'count': {'$sum': 1}, 'rt_account': {'$first': '$retweeted_status.user.id'}, 'rt_info': {'$push': {'account': '$user.id', 'rt_time': '$created_ts'}}}}]
rt_count = list(project_data_db.aggregate(aggregation_pipeline, allowDiskUse=True))

rt_count = json_normalize(rt_count)
rt_count['_id'] = "ID_" + rt_count['_id']

simple_rt_file = output_file_name.replace(os.path.basename(output_file_name), 'simple_rt_count_info.csv')
simple_rt_count = rt_count[['_id', 'count', 'rt_account']]
simple_rt_count.to_csv(simple_rt_file, index=False)

output_folder = 'metadata_pulls/data/preNov2020_retweets_w_id/counts/'
os.makedirs(output_folder, exist_ok=True)
for idx in rt_count.index:
    rt_row = rt_count.loc[idx]
    rt_info = rt_row['rt_info']
    rt_info = json_normalize(rt_info)
    try:
        rt_info_file = output_folder + str(rt_row['_id']) + '.csv'
        rt_info.to_csv(rt_info_file, index=False)
    except OSError as e:
        # This makes a new folder when the number of files in a folder exceeds the limit
        print(e)
        output_folder = output_folder.replace('counts/', 'counts2/')
        os.makedirs(output_folder, exist_ok=True)
        rt_info_file = output_folder + str(rt_row['_id']) + '.csv'
        rt_info.to_csv(rt_info_file, index=False)

##############################
#
# The next section makes viz
#
##############################

rt_count = simple_rt_count['count'].value_counts()
rt_count.sort_index(inplace=True)

account_rt_count = simple_rt_count.groupby(by='rt_account').agg({'count': 'sum'})
account_rt_count.sort_values(by='count', ascending=False, inplace=True)

count_account_rt_count = account_rt_count['count'].value_counts()
count_account_rt_count.sort_index(inplace=True)

"""
First up, a scatter plot of the number of retweets per tweet
"""

fig = plt.figure(figsize=(6,4))
plot1 = fig.add_subplot(111)
plot1.set_yscale('log')
scatter = plt.scatter(x=rt_count.index, y = rt_count, s=.5)
plot1.set_xlim(0,30000)
plot1.set_xlabel('Number of retweets')
plot1.set_ylabel('Number of tweets retweeted this many times')

#plt.show()
plt.savefig('retweets_per_tweet.pdf')

"""
Next, a scatter plot of the number of retweets of accounts
"""

fig = plt.figure()
plot1 = fig.add_subplot(111)
scatter = plt.scatter(x=count_account_rt_count.index, y = count_account_rt_count, s=.5)
plot1.set_xlabel('Number of retweets')
plot1.set_ylabel('Number of accounts retweeted this many times')

#plt.show()
plt.savefig('retweets_of_accounts.pdf')

##############################
#
# To figure out how many times an account retweets others, we need to transform the network.
# This is probably excessive, but it's the approach we used.
#
##############################

del simple_rt_count, rt_count

print("{} Reading {}.".format(datetime.datetime.now(), simple_rt_file))
rt_summary_data = pd.read_csv(simple_rt_file)
rt_summary_data.set_index('_id', inplace=True)
rt_summary_data.drop(columns='count', inplace=True)

folders = ['counts', 'counts2']

print("{} Building file list.".format(datetime.datetime.now()))
all_files = []
for folder in folders:
    folder_files = os.listdir(folder)
    folder_files = [folder + '/' + f for f in folder_files]
    folder_files = [f for f in folder_files if 'ID_' in f]
    all_files.extend(folder_files)

retweet_matrix = {}

print("{} Building retweet matrix.".format(datetime.datetime.now()))
file_counter = 0
number_of_file = len(all_files)
for file in all_files:
    file_counter += 1
    file_contents = pd.read_csv(file)
    if file_counter % 10000 == 0:
        print("{} Processing {} of {} retweet files.".format(datetime.datetime.now(), file_counter, number_of_file))
    retweeted_account = str(rt_summary_data.loc[os.path.basename(file).replace('.csv','')]['rt_account'])
    if not retweet_matrix.get(retweeted_account):
        retweet_matrix[retweeted_account] = {}
    accounts_that_retweeted = file_contents['account']
    retweeted_account_retweeters = retweet_matrix[retweeted_account]
    accounts_that_retweeted = [str(account) for account in accounts_that_retweeted]
    for account in accounts_that_retweeted:
        if account in retweeted_account_retweeters:
            retweeted_account_retweeters[account] += 1
        else:
            retweeted_account_retweeters[account] = 1
    retweet_matrix[retweeted_account] = retweeted_account_retweeters

retweet_network_info_list = []

to_account_counter = 0
num_to_accounts = len(retweet_matrix)
for to_account in retweet_matrix:
    to_account_counter += 1
    from_accounts = retweet_matrix[to_account]
    for from_account in from_accounts:
        from_account_number = from_accounts[from_account]
        df_row = [{'RT': from_account, 'Org': to_account}]
        df_rows = [info for info in df_row for idx in range(from_account_number)]
        retweet_network_info_list.extend(df_rows)
    if to_account_counter % 10000 == 0:
        print("{} {} of {} to-accounts processed".format(datetime.datetime.now(), to_account_counter, num_to_accounts))

retweet_network_df = pd.DataFrame(retweet_network_info_list)

self_retweets = retweet_network_df[retweet_network_df['Org'] == retweet_network_df['RT']]
self_retweets.describe()

count_times_account_was_retweeted = retweet_network_df['Org'].value_counts()
count_times_account_retweeted_another = retweet_network_df['RT'].value_counts()

count_sent_retweets = count_times_account_retweeted_another.value_counts()
count_sent_retweets.sort_index(inplace=True)

count_sent_retweets[count_sent_retweets.index > 1000].sum()

fig = plt.figure(figsize=(6,4))
plot1 = fig.add_subplot(111)
plot1.set_yscale('log')
scatter = plt.scatter(x=count_sent_retweets.index, y = count_sent_retweets, s=.5)
plot1.set_xlim(0,40000)
plot1.set_xlabel('Number of retweets')
plot1.set_ylabel('Number of accounts sent this many retweets')

#plt.show()
plt.savefig('retweets_by_accounts.pdf')
