"""
This code uses a Mongo collection created from tweet objects using the following aggregation pipeline:
 [{'$group': {'_id': '$lang','language_occurrence': {'$sum': 1}}},
 {'$project':{'date': <date object>,'language': '$lang','language_occurrence': '$language_occurrence'}}]
"""

import datetime
import pymongo
import logging
import os
from pandas.io.json import json_normalize
import sys
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import cm


output_file_name = 'data/preNov2020_language_breakdown.csv'
os.makedirs(os.path.dirname(output_file_name), exist_ok=True)

log_file_name = output_file_name.replace('.csv', '.log')
log_level = logging.DEBUG # Option are logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG. The log file will only contain messages that are at this level or "higher." Debug will create the most detailed log file
os.makedirs(os.path.dirname(log_file_name), exist_ok=True)
logging.basicConfig(filename=log_file_name,filemode='w+',level=log_level, format="[%(asctime)s] %(levelname)s:%(name)s:%(message)s")

mongo_details = {'address': None, # If address is None, use local mongo
                 'auth': True, # If auth is True, mongo has authentication turned on and the code needs a mongo user and pass
                 'project_name': 'ConspiracyTheoriesUA', # The name of the project for pulling data. This code will automatically use this to generate the project config DB and project data DB names.
                 'col_name': 'daily_lang_counts', # The name of the collection containing the data to be pulled
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

daily_lang_counts = list(project_data_db.find({"_id": {"$lt": datetime.datetime(2020,11,1)}}))

daily_lang_counts = json_normalize(daily_lang_counts)
daily_lang_counts.set_index('_id', inplace=True)
daily_lang_counts.index = daily_lang_counts.index.date

lang_sums = daily_lang_counts.sum()
lang_sums.to_csv(output_file_name)

language_total = lang_sums
del lang_sums

##############################
#
# The next section makes viz
#
##############################

lang_map = {'en': 'English', 'und': 'Undefined', 'es': 'Spanish', 'ja': 'Japanese', 'pl': 'Polish',
            'de': 'German', 'pt': 'Portuguese', 'tl': 'Tagalog', 'fr': 'French', 'ca': 'Catalan'}

language_total.sort_values(ascending=False,inplace=True)
language_total.index = language_total.index.str.replace('language_occurrences.','')
language_total.index = language_total.index.to_series().map(lang_map)

main_lang = language_total.iloc[:2]
secondary_lang = language_total.iloc[2:]

main_lang.loc['Inset'] = secondary_lang.sum()
rotate_angle = 180 * main_lang.loc['Inset']/main_lang.sum()


# overall plot setup
fig = plt.figure(figsize=(15,5))
plot1 = fig.add_subplot(121)
plot2 = fig.add_subplot(122)
fig.subplots_adjust(wspace=0)
plot_colors = cm.get_cmap(name='Paired').colors


# main pie chart
main_pie = plot1.pie(main_lang,autopct='%.2f%%', startangle=rotate_angle, labels=main_lang.index, colors=[plot_colors[1], plot_colors[0], plot_colors[2]])

# bar chart expansion
total_secondary = secondary_lang.sum()
ratio_thresh = .02
raw_thresh = total_secondary * ratio_thresh
secondary_truncated = secondary_lang[secondary_lang > raw_thresh]
secondary_truncated['Other'] = secondary_lang[secondary_lang <= raw_thresh].sum()
secondary_trunc_ratio = secondary_truncated / secondary_truncated.sum()
secondary_trunc_ratio = pd.DataFrame(secondary_trunc_ratio)
secondary_trunc_ratio = secondary_trunc_ratio[::-1]
inset_bar = secondary_trunc_ratio.T.plot.bar(stacked=True,ax=plot2, color=plot_colors[3:])#, table=True)
plot2.axis('off')
handles, labels = plot2.get_legend_handles_labels()
plot2.legend(loc=5,labels=labels[::-1], handles=handles[::-1])

# show all viz
#plt.show()
plt.savefig('preNov2020_language_breakdown.pdf', format='pdf')

overall_percents = language_total
overall_percents['other'] = overall_percents.loc[overall_percents.index.isnull()].sum()
overall_percents = overall_percents.loc[overall_percents.index.notna()]
overall_percents = (overall_percents / overall_percents.sum()) * 100
