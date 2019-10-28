#!/usr/bin/python
# Python Task for McMakler
# Author: Nahid Akhtar
# Email: nahid.saarland@gmail.com
########################################################################################################################
#  Libraries
########################################################################################################################
from datetime import datetime
from datetime import timedelta
import pandas as pd
import requests
import json
import sqlite3
import matplotlib.pyplot as plt
########################################################################################################################
'''The function download the exchange data using API
As the free API can bring data for a single date that'why API is called 180 times for 180 days data. 
'''


def download_exchange_data(base, symbols, base_url, personal_access_key, time_duration):

    # Setting Payload for calling API
    payload = {'access_key': personal_access_key, 'base': base, 'symbols': symbols}
    end_date = datetime.now()   # Current date
    start_date = end_date - timedelta(days=time_duration) # Current date - 180
    column_names = ['Date', 'Currency', 'Value']

    # Creating Dataframe
    exchange_data = pd.DataFrame(columns=column_names)

    # Download the data by calling the API and appending to dataframe
    try:
        k = 0
        for i in range(time_duration):
            each_date = start_date + timedelta(days=i)
            each_date = datetime.strptime(str(each_date.date()), '%Y-%m-%d') # Changing Data format
            new_base_url = base_url + str(each_date.date())
            r = requests.post(new_base_url, params=payload)
            result = json.loads(r.text)
            new_date = datetime.strptime(result['date'], '%Y-%m-%d').strftime('%d-%m-%Y')

            # Inserting data into dataframe separately by increasing index
            exchange_data.loc[k] = [ new_date, 'USD', round(result['rates']['USD'], 2)]
            exchange_data.loc[k+1] = [new_date, 'JPY', round(result['rates']['JPY'], 2)]
            exchange_data.loc[k+2] = [new_date, 'GBP', round(result['rates']['GBP'], 2)]
            k = k + 3
            print( i+1, ': Record fetched using API')
    except:
        print('Error: The data cannot be fetched by using the API.')
    return exchange_data
########################################################################################################################
''' The function download the exchange data using API
As the free API can bring data for a single date that'why API is called 180 times for 180 days data. 
'''


########################################################################################################################
# The function plots a graph for Exchange Rates for USD, JPY and GBP for last 180 Days Based on EUR

def single_graph(data):
    fig = plt.figure(figsize=(15, 9)) # setting figure size
    ax = fig.subplots()
    fig.canvas.set_window_title('Single Graph')
    for key, grp in data.groupby('Currency'):
        grp.plot(x='Date', y='Value', ax=ax, label=key)
    plt.legend(loc='best', labels=data['Currency'].unique())
    plt.xticks(rotation=45)
    ax.title.set_text('Exchange Values for USD, JPY and GBP for last 180 Days Based on EUR')
    plt.ylabel('Value')

########################################################################################################################
# Call for Main Function


def separate_graphs(data):
    fig2 = plt.figure(figsize=(15, 9))

    # Subplots ax1,ax2 and ax3
    ax1 = fig2.add_subplot(311)
    ax2 = fig2.add_subplot(312)
    ax3 = fig2.add_subplot(313)

    # Subplots ylabel
    ax1.set_ylabel('Value')
    ax2.set_ylabel('Value')
    ax3.set_ylabel('Value')

    # Figure title and windows title
    fig2.suptitle("Exchange Values for USD+,GBP and JPY for last 180 Days Based on EUR", fontsize=16)
    fig2.canvas.set_window_title('Separate Graphs')

    # Data for Subplots ax1,ax2 and ax3
    df_GBP = data.drop(data[(data['Currency'] == 'USD') | (data['Currency'] == 'JPY')].index)
    df_USD = data.drop(data[(data['Currency'] == 'GBP') | (data['Currency'] == 'JPY')].index)
    df_JPY = data.drop(data[(data['Currency'] == 'GBP') | (data['Currency'] == 'USD')].index)

    # Plotting Subplots ax1,ax2 and ax3
    for key, grp in df_USD.groupby('Currency'):
        grp.plot(x='Date', y='Value', ax=ax1, label=key, color="green")
    for key, grp in df_GBP.groupby('Currency'):
        grp.plot(x='Date', y='Value', ax=ax2, label=key, color="blue")
    for key, grp in df_JPY.groupby('Currency'):
        grp.plot(x='Date', y='Value', ax=ax3, label=key, color="red")
########################################################################################################################
# Load pickle and create database locally


def create_database(pickle_file):
    data = pd.read_pickle(pickle_file)
    try:
        db = sqlite3.connect(':memory:')
        data.to_sql("Rates_History", db,
                    if_exists="replace")  # replace is used only when to replace the existing data from the db
        db.commit()
        print("Data is inserted into Database")
    except:
        print("Data is Couldn't be loaded into Database")
    cursor = db.cursor()
    return cursor, db
########################################################################################################################
# Run the SQL query over the database


def run_query(cursor, db):
    cursor.execute('''SELECT  Currency , MAX(Value) AS max_rate
            From Rates_History
            GROUP BY 1;''')
    rows = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    column_labels = ''
    for i in column_names:
        column_labels = column_labels + i + '    '
    print('----------------------------')
    print(column_labels)
    print('----------------------------')
    for row in rows:
        print('{0}   {1}'.format(row[0], row[1]))

    print('----------------------------')
    db.close()
########################################################################################################################
# Call for Main Function


def main():
    # Inputs
    base = "EUR"
    symbols = "USD,GBP,JPY"
    base_url = "http://data.fixer.io/api/"
    personal_access_key = '3052f716ee6527c45bf6c50999dddfbf'
    time_duration = 180
    ####################################################################################################################
    # Question 1 & 2: Download data for last 180 days using the API call
    print('Kindly wait for few minutes as is downloading data records for last 180 days using API.')
    data = download_exchange_data(base, symbols, base_url, personal_access_key, time_duration)
    #print(data)
    ####################################################################################################################
    # Question 4:  Pickle the Dataframe

    pickle_file = './data.pkl'
    data.to_pickle(pickle_file)
    ####################################################################################################################
    # Question 5:  Create the database by unpickle the file

    cursor, db = create_database(pickle_file)
    ####################################################################################################################
    ''' Question 6: Run SQL over this DB and print the result to stdout:
    # select maximum rate/value for every symbol/currency (USD, GBP, JPY) '''

    run_query(cursor, db)
    ####################################################################################################################
    '''  Question 3: Draw graphs from data (As values of GBP and USD are very close and have overlap so I created two 
      graphs i.e. one with all three values together and the other with separate graphs'''

    separate_graphs(data)
    single_graph(data)
    plt.show()
########################################################################################################################
# Call for Main Function


if __name__ == '__main__':
    main()