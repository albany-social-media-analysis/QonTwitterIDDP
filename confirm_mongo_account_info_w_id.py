"""
This uses the file created with the following mongoexport command:
 mongoexport -u [REDACTED] -d ConspiracyTheoriesUA_5d7a4aaf1da0444b0f499b06 -c preNov2020 -f created_at,user.screen_name,user.created_at,user.verified,user.description,user.lang,user.location,user.id --type csv --authenticationMechanism MONGODB-CR --authenticationDatabase admin
"""
import pandas as pd
import argparse
import os
import datetime
import matplotlib.pyplot as plt

ap = argparse.ArgumentParser()
ap.add_argument("-o", "--output-file", type=str, required=False, help="The path (including .xlsx) for the output file. If not specified, the default is the timestamp for when the data was pulled.")
args = vars(ap.parse_args())

if args['output_file']:
    output_file_name = 'data/' +  args['output_file']
elif not args['output_file']:
    output_file_name = 'data/user_info_' + str(datetime.datetime.now().replace(microsecond=0)).replace(' ', 'T') + '.csv'

os.makedirs(os.path.dirname(output_file_name), exist_ok=True)

user_info_df = pd.read_csv('preNov2020_mongo_export_user_info_w_id.csv')
#total_tweets = user_info_df.shape[0]

user_tweet_counts = user_info_df['user.id'].value_counts()

id_screenname_pairs = user_info_df.drop_duplicates(subset=['user.screen_name', 'user.id'])
id_screenname_pairs = id_screenname_pairs[['user.screen_name', 'user.id']]

user_tweet_counts.to_csv('user_id_tweet_count.csv')

id_screenname_pairs.to_csv('user_id_screenname_pairs.csv')

user_tweet_counts.describe()

id_screenname_pairs.describe()

##############################
#
# The next section makes viz
#
##############################

tweet_count_count = user_tweet_counts.value_counts()
tweet_count_count = tweet_count_count.sort_index(ascending=True)

fig = plt.figure(figsize=(6,4))
plot1 = fig.add_subplot(111)
plot1.set_yscale('log')
scatter = plt.scatter(x=tweet_count_count.index, y = tweet_count_count, s=.5)
plot1.set_xlim(0,60000) #
plot1.set_xlabel('Number of tweets')
plot1.set_ylabel('Number of accounts sending X number of tweets')

#plt.show()
plt.savefig('tweets_per_account.pdf')
