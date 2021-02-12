"""
This uses the file created with the following mongoexport command:
 mongoexport -u [REDACTED] -d ConspiracyTheoriesUA_5d7a4aaf1da0444b0f499b06 -c preNov2020 -f created_at,user.screen_name,user.created_at,user.verified,user.description,user.lang,user.location,user.id --type csv --authenticationMechanism MONGODB-CR --authenticationDatabase admin
"""

import pandas as pd
import os
import matplotlib.pyplot as plt
import datetime

output_file_name = '/network/rit/lab/jacksonlab/QonTwitter/metadata_pulls/data/preNov2020_user_info/user_id_create_date.csv'

os.makedirs(os.path.dirname(output_file_name), exist_ok=True)

user_info_df = pd.read_csv('preNov2020_mongo_export_user_info_w_id.csv')

user_create_date = user_info_df[['user.id', 'user.created_at']]
user_create_date.drop_duplicates(subset='user.id', inplace=True)
user_create_date['user.created_at'] = pd.to_datetime(user_create_date['user.created_at'])

buggy_date = user_create_date[user_create_date['user.created_date'] < datetime.date(2006,1,1)].copy()
user_create_date = user_create_date[user_create_date['user.created_date'] > datetime.date(2006,1,1)]

account_create_date_count = user_create_date['user.created_date'].value_counts()
account_create_date_count.sort_index(inplace=True)

account_create_date_count.describe()

##############################
#
# The next section makes viz
#
##############################

fig = plt.figure(figsize=(12,4))
plot1 = fig.add_subplot(111)

scatter = plt.scatter(x=account_create_date_count.index, y=account_create_date_count, s=.5)

#plt.show()
plt.savefig('account_create_date.pdf')
