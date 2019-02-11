# -*- coding: utf-8 -*-
"""
Created on Fri Dec 21 09:50:53 2018

@author: F2531197
"""

import pandas as pd
import dateutil.relativedelta 
import time
from datetime import datetime
import sqlite3


Today=time.strftime("%Y-%m-%d")



#..............................................................................

#create a database for testing

SQLITE3 = sqlite3.connect(r'C:\RONSEN\PYTHON\Mytest.db')      

SQLITE3_cursor = SQLITE3.cursor()

#create a database in memory for production
#from sqlalchemy import create_engine
#SQLITE3 = create_engine('sqlite:///:memory:', echo=False)

#..............................................................................


#define a period for the loop
#period_start='2018-01-01'
period_start = (datetime.today() - dateutil.relativedelta.relativedelta(months=2)).strftime("%Y-%m-01")
#period_start = (datetime.today() - dateutil.relativedelta.relativedelta(months=36)).strftime("%Y-%m-01")
period_end = (datetime.today() - dateutil.relativedelta.relativedelta(months=35)).strftime("%Y-%m-01")
last_day_month=(datetime.today() + 
   dateutil.relativedelta.relativedelta(months=1)+
   dateutil.relativedelta.relativedelta(day=31)).strftime("%Y-%m-%d")

#period_end='2018-10-01'
#daily D, Monthy MS, Yearly YS
def create_date_table2(start, end):
    df = pd.DataFrame({"Date": pd.date_range(start, end,freq='MS')})
    df['Month']= df.Date.dt.strftime('%Y%m').astype(int)
    return df
period_range=create_date_table2(start=period_start, end=period_end)

#..............................................................................

#chunksize for handle bigdata and therefore handle the memory issue
chunksize=1000*50
separator =','
encoding="ISO-8859-1" #encoding="ISO-8859-1" #'utf-8-sig'

#
#difine the types for the variables
dates_col=['starttime','stoptime']


dtypes= {"tripduration":str,"start station id":str,"start station name":str,
         "start station latitude":str,"start station longitude":str,
         "end station id":str,"end station name":str,
         "end station latitude":str,"end station longitude":str,"bikeid":str,
         "usertype":str,"birth year":str,"gender":str}


#create  a database table for indata 

#use real(for floats and int) Date and text or real(for int)

table='CitibikesNYC'
create_table='''
CREATE TABLE  f%s 
(
Tripduration real,	Starttime date,	Stoptime date,	StartStationId real,
StartStationName text,	StartStationLatitude real,	StartStationLongitude real,	
EndStationId real,	EndStationName text,	EndStationLatitude real,
EndStationLongitude real,	Bikeid real,	Usertype text,	BirthYear text,
Gender int,	Gender_text text, File_Period real

)
'''%(table)

#first drop and then create an empty table in sqlite3
pd.io.sql.execute('DROP TABLE IF EXISTS '+ 'f'+table, SQLITE3)
pd.io.sql.execute(create_table, SQLITE3) #create the new empty table

months=list(period_range.Month)
months=['201712','201710']


import dask.dataframe as dd 


url='https://s3.amazonaws.com/tripdata/201712-citibike-tripdata.csv.zip'
df1 = dd.read_csv(url, encoding='latin1',engine='c')

file=r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\TestKund\Indata\311_Service_Requests_from_2010_to_Present.csv'

df1 = pd.read_csv(file,sep=separator,encoding=encoding,dtype='str',nrows=1000)



df1.to_sql(name='testttt',con=SQLITE3,index=False,if_exists='append')



int_float=['Debet','Kredit','Konto']
def coerce_df_columns_to_numeric(df, int_float):
    df[int_float] = df[int_float].apply(pd.to_numeric, errors='coerce')
    df.update(df[int_float].fillna(0))


df = dd.read_csv(file,sep=separator,encoding=encoding,dtype='str')
coerce_df_columns_to_numeric(df, int_float)


df.info()
ff=df.head(10)

sql = "SELECT * FROM testttt"
for df in pd.read_sql_query(sql , con=SQLITE3, chunksize=5):
    df['one']=1
    df['DateKey1'] = df.groupby(['one'])['one'].apply(lambda x: x.cumsum())
    df.to_sql(name='testttt2',con=SQLITE3,index=False,if_exists='append')

def Q(sql):
    res=pd.read_sql_query(sql , con=SQLITE3, chunksize=5)
    return next(res)




df.info()

df1.compute(10).to_sql(name='ff',con=SQLITE3,index=False,if_exists='append')

res=pd.read_sql_query(sql , con=SQLITE3, chunksize=5)

df1.info()

df1 = dd.read_csv(file,sep=separator,encoding=encoding,dtype='str')
df1['one']=1
df1['DateKey'] = df1.groupby(['one'])['one'].apply(lambda x: x.cumsum())
df3=df1.head(10)

start_time = time.time()
chuck_sum=0
for month in months:
    url='https://s3.amazonaws.com/tripdata/'+str(month)+'-citibike-tripdata.csv.zip'
    for df in  pd.read_csv(url,chunksize=chunksize,sep=separator,
               encoding=encoding,low_memory=False,iterator=True,dtype='str'):
        df.columns = df.columns.str.title()
        df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns}) # Remove spaces from columns
        df['Gender_text']='unknown'
        df.loc[(df.Gender ==1) , 'Gender_text'] = 'male'
        df.loc[(df.Gender ==2) , 'Gender_text'] = 'female'
        chuck_sum += chunksize
        df['File_Period'] = month
#        df['DurationSeconds']=(df.Stoptime-df.Starttime).astype('timedelta64[s]')
#        df['DurationMinutes']=(df.Stoptime-df.Starttime).astype('timedelta64[m]')
        df.to_sql(name=str('f'+table),con=SQLITE3,index=False,if_exists='append') #replace append
        print('Data to Database ',time.strftime("%H:%M:%S"),time.strftime("%H:%M:%S", 
              time.gmtime(time.time() - start_time)),df.shape,chuck_sum,month)



#period = (datetime.today() - dateutil.relativedelta.relativedelta(months=2)).strftime("%Y%m")

#delete duplicates


create_view='''
create view v%s as

SELECT
    distinct * from f%s 

'''%(table,table)

pd.io.sql.execute('DROP VIEW IF EXISTS '+ 'v'+table, SQLITE3)
pd.io.sql.execute(create_view, SQLITE3) #create the new empty table




#df = pd.read_sql_query(sql_query, SQLITE3)

#for df in pd.read_sql_query(sql_query, SQLITE3,chunksize=chunksize): #get data from SQLITE
#    df['r']=1

#del [[create_table,period_range]]
del df


SQLITE3.close()







#
#
#
#
#column_order_json=['Bikeid',	'BirthYear',	'EndStationId',	'EndStationLatitude',
#              'EndStationLongitude',	'EndStationName',	'Gender',	'StartStationId',	
#              'StartStationLatitude','StartStationLongitude','StartStationName',	
#              'Starttime','Stoptime','Tripduration','Usertype']
# 
#
#
#
#table='test_json'
#create_table='''
#CREATE TABLE  %s 
#(
#Tripduration real,	Starttime date,	Stoptime date,	StartStationId real,
#StartStationName text,	StartStationLatitude BLOB,	StartStationLongitude BLOB,	
#EndStationId real,	EndStationName text,	EndStationLatitude BLOB,
#EndStationLongitude BLOB,	Bikeid real,	Usertype text,	BirthYear text,
#Gender int,	Gender_text text, File_Period real,Duration real
#
#)
#'''%(table)
#
#json_file=r'C:\RONSEN\PYTHON\BefolkningNy.json'
#for df in pd.read_json(json_file, lines=True, 
#                       chunksize = chunksize,encoding=encoding):
#    chuck_sum += chunksize
#    print(chuck_sum)
#    df.columns = df.columns.str.title() #fix the colums so they begins with uppercase
#    df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns}) #remove spaces in the columns
#    df=df[column_order_json].copy()
#    df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append') #a datadase
#   

#
## read the entire file into a python array
#with open(json_file) as json_file1:
#   data = json_file1.readlines()
# 
### remove the trailing "\n" from each line
#data = map(lambda x: x.rstrip(), data)
## 
### each element of 'data' is an individual JSON object.
### i want to convert it into an *array* of JSON objects
### which, in and of itself, is one large JSON object
### basically... add square brackets to the beginning
### and end, and have all the individual business JSON objects
### separated by a comma
#data_json_str = "[" + ",".join(data) + "]"
#
## now, load it into pandas
#df=pd.read_json(data_json_str)
#
#
#from pandas.io.json import json_normalize    
#df = json_normalize(df['variables'], 'title')
##title variables
#
#import json
#from pandas.io.json import json_normalize
#import pandas as pd
#
#with open(json_file) as f:
#    data = json.load(f)
#
#df = pd.DataFrame(data)   
#
#normalized_df = json_normalize(df['variables'])
#
#'''column is a string of the column's name.
#for each value of the column's element (which might be a list),
#duplicate the rest of columns at the corresponding row with the (each) value.
#'''
#
#def flattenColumn(input, column):
#    column_flat = pd.DataFrame([[i, c_flattened] for i, y in input[column].apply(list).iteritems() for c_flattened in y], columns=['I', column])
#    column_flat = column_flat.set_index('I')
#    return input.drop(column, 1).merge(column_flat, left_index=True, right_index=True)
#    
#new_df = flattenColumn(normalized_df, 'values' )
#
#
#def flattenColumn(input, column):
#    column_flat = pd.DataFrame([[i, c_flattened] for i, y in input[column].apply(list).iteritems() for c_flattened in y], columns=['I', column])
#    column_flat = column_flat.set_index('I')
#    return input.drop(column, 1).merge(column_flat, left_index=True, right_index=True)
#    
#new_df1 = flattenColumn(normalized_df, 'valueTexts' )
#
#normalized_df.info()

sql_query= """  
select * from mytest limit 18
    
     """


df = pd.read_sql_query(sql_query, SQLITE3)

view='mytest1'
create_view='''
create view v%s as SELECT
  a.HundNamn,
  a.RegNr
FROM
  dogs a
'''%(view)

#first drop and then create an empty table in sqlite3
pd.io.sql.execute('DROP VIEW IF EXISTS '+ view, SQLITE3)
pd.io.sql.execute(create_view, SQLITE3) #create the new empty table
