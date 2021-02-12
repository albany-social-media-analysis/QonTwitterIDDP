import datetime
import pymongo
import logging
import os
import pandas as pd
import sys
import matplotlib.pyplot as plt

output_file_name = 'data/preNov2020_daily_count.csv'
os.makedirs(os.path.dirname(output_file_name), exist_ok=True)

log_file_name = os.path.basename(output_file_name.replace('.xlsx', '.log'))
log_level = logging.DEBUG # Option are logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG. The log file will only contain messages that are at this level or "higher." Debug will create the most detailed log file
os.makedirs(os.path.dirname(log_file_name), exist_ok=True)
logging.basicConfig(filename=log_file_name,filemode='w+',level=log_level, format="[%(asctime)s] %(levelname)s:%(name)s:%(message)s")

mongo_details = {'address': None, # If address is None, use local mongo
                 'auth': True, # If auth is True, mongo has authentication turned on and the code needs a mongo user and pass
                 'project_name': 'ConspiracyTheoriesUA', # The name of the project for pulling data. This code will automatically use this to generate the project config DB and project data DB names.
                 'col_name': 'preNov2020', # The name of the collection containing the data to be pulled
                 'user': input('Enter the mongo username: '), 'password': input('Enter the mongo password: ') } # The user will be prompted for the mongo username and password


def build_mongo_connection():
    logging.debug("Building mongo connection")
    mongoClient = pymongo.MongoClient(mongo_details['address'])
    mongoClient.admin.authenticate(mongo_details['user'], mongo_details['password'])
    databases = mongoClient.database_names()
    project_dbs = [f for f in databases if mongo_details['project_name'] in f]
    if len(project_dbs) == 0:
        logging.critical('No databases found for the specified project. Is the project name in data_pull_config.py correct?')
        sys.exit()
    elif len(project_dbs) > 2:
        logging.critical('The specified project name returns too many results. Is the project name in data_pull_config.py correct?')
        sys.exit()
    project_config_db = [f for f in project_dbs if 'Config' in f][0]
    project_config_db = mongoClient[project_config_db]['config']
    project_data_db = [f for f in project_dbs if '_' in f][0]
    project_data_db = mongoClient[project_data_db][mongo_details['col_name']]
    return project_config_db, project_data_db

project_config_db, project_data_db = build_mongo_connection()

all_tweets = pd.DataFrame(list(project_data_db.find({},{'id_str':1, 'created_ts':1, '_id':0})))

all_tweets.sort_values(by='created_ts', ascending=True, inplace=True)

daily_counts = all_tweets['created_ts'].dt.date.value_counts()
daily_counts.sort_index(inplace=True, ascending=True)
daily_counts.describe()

daily_counts.to_csv(output_file_name)

##############################
#
# The next section makes viz
#
##############################

rows_to_add = {'2018-12-07': 0, '2018-12-08': 0, '2018-12-09': 0}
rows_to_add_index = rows_to_add.keys()
rows_to_add_df = pd.Series(data=0, index=rows_to_add.keys())
daily_counts = daily_counts.append(rows_to_add_df)
daily_counts.sort_index(inplace=True)

fig = plt.figure(figsize=(12,4))
plot1 = fig.add_subplot(111)

line_chart = daily_counts.plot.line(label='_nolegend_') # This label value keeps it out of the legend
line_chart.set_xlabel(None)
line_chart.ticklabel_format(style='plain', axis='y')
fig.autofmt_xdate()
Covid_line = line_chart.axvline(x=613, label='National COVID emergency declared', color='r', linestyle='dashed') # March 13 2020
Floyd_line = line_chart.axvline(x=686, label='George Floyd\'s death', color='g', linestyle='dashed') # May 25 2020
action_line = line_chart.axvline(x=744, label='Twitter removes 7k QAnon accounts', color='tab:orange', linestyle='dashed') # July 23 2020
legend = plot1.legend(frameon=False)
plt.title('Number of tweets in dataset each day')
for spine in plot1.spines:
    plot1.spines[spine].set_visible(False)
plot1.tick_params(right=False, labelright=True, bottom=False, left=False)


line_chart.annotate('Most tweets in one day: 662,012 on July 22 2020\n165k day before removal, 186k day after', xy=(744, 662012), xytext=(300, 550000),
             arrowprops=dict(arrowstyle='->'))
line_chart.annotate('Last spike: 525,339 tweets on Oct 16 2020', xy=(830, 525339), xytext=(300, 290000),arrowprops=dict(arrowstyle='->', ls='--'))

#plt.show()
plt.savefig('preNov2020_over_time.pdf')


fig = plt.figure(figsize=(8,4))
plot1 = fig.add_subplot(111)

hist_plot = daily_counts.plot.hist(bins=100)
#plt.show()
plt.savefig('preNov2020_daily_count_hist.pdf')