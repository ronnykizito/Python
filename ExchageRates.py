# -*- coding: utf-8 -*-
"""
Created on Wed Mar 20 17:55:25 2019

@author: F2531197
"""

import pandas as pd
from forex_python.converter import get_rate
from datetime import datetime
import time
import dateutil.relativedelta
import sqlite3


'''========================================================================='''

chunksize=1000*1000
HistoryTable='Currency'


Main_Path=r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\DontRemove\\'

'''setup a database for SQLITE3. The execution plan for SQLIT3 is much faster then other database'''



sqlite_db = sqlite3.connect(Main_Path+'\\Database\\'+str(HistoryTable)+'.db')
c = sqlite_db.cursor()

'''============get the exchagerates for a period============================'''

Today=time.strftime("%Y-%m-%d")

read_sql_query='''

SELECT
      substr(max(a.Date),1,10) AS maxdate
FROM
    %s a

'''%(HistoryTable)



df =pd.read_sql_query(read_sql_query, sqlite_db)
df['maxdate'] = df['maxdate'].values.astype("datetime64[s]")
df['period_start'] = (df.maxdate - pd.DateOffset(days=3)).dt.strftime('%Y-%m-%d')

period_start="[]".join(df.period_start)

main_currency='SEK'


int_float=['Year','Month','Day']



df = pd.DataFrame(index = pd.date_range(period_start, Today, freq='D'))
df['Date']=df.index
df["Year"] = df.Date.dt.strftime('%Y')
df["Month"] = df.Date.dt.strftime('%m')
df["Day"] = df.Date.dt.strftime('%d')
df[int_float] = df[int_float].apply(pd.to_numeric, errors='coerce')

year=list(df.Year)
month=list(df.Month)
day=list(df.Day)



for y,m,d in zip(year,month,day):
    TheDay = datetime(y, m, d)  # the 18th of October, 2001
    df=pd.DataFrame({'Date':[TheDay],str(main_currency):[1],
                     'GBP':[get_rate(str(main_currency), "GBP", TheDay)]})
    df['USD']=get_rate(str(main_currency), "USD", TheDay)
    df['EUR']=get_rate(str(main_currency), "EUR", TheDay)
    df["DateKey"] = df.Date.dt.strftime('%Y-%m-01')
    df.to_sql(name='v1',con=sqlite_db,index=False,if_exists='append') #replace append


pivot_columns=['Date','DateKey']
for df in pd.read_sql_query('select * from v1', sqlite_db,chunksize=chunksize):
    df = pd.melt(df, id_vars=pivot_columns, var_name="Currency", value_name="Value")
    df.to_sql(name='v2',con=sqlite_db,index=False,if_exists='append') #replace append

read_sql_query='''
select * from currency
union
select * from v2
'''
pd.io.sql.execute('DROP table IF EXISTS v3', sqlite_db)
for df in pd.read_sql_query(read_sql_query, sqlite_db,chunksize=chunksize):
    df.to_sql(name='v3',con=sqlite_db,index=False,if_exists='append')
    
df = pd.read_sql_query('select * from v3', sqlite_db)
df.drop_duplicates(['Date','DateKey','Currency','Value'],keep= 'first', inplace=True)
df.to_sql(name=str(HistoryTable),con=sqlite_db,index=False,if_exists='replace')
    
DeleteTales=['v1','v2','v3']
for DeleteTale in DeleteTales:
    pd.io.sql.execute('DROP table IF EXISTS '+DeleteTale, sqlite_db)
