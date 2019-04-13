# -*- coding: utf-8 -*-
"""
Created on Sun Mar 17 16:45:55 2019

@author: F2531197
"""


import time
import os
import sqlite3
import pandas as pd
import numpy as np


'''========================================================================='''


Kund="Tenrox"

Today=time.strftime("%Y-%m-%d")
#chunksize for handle bigdata and therefore handle the memory issue
chunksize=1000*1000

encoding="ISO-8859-1" #encoding="ISO-8859-1" #'utf-8-sig'




'''=========================create folders=================================='''

Main_Path=r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\\'+Kund

lista='indata datamining  data   database '

folders = [item for item in lista.title().split(' ') if  item not in  ['']] #exclude empty



for folder_name in folders:
    newpath = Main_Path+'\\'+folder_name
    if not os.path.exists(newpath):
        os.makedirs(newpath)
        


'''==========================setup a sqlite3 database======================'''



sqlite_db = sqlite3.connect(Main_Path+'\\Database\\'+Kund+'.db')
c = sqlite_db.cursor()


'''========================================================================='''
'''  getting Networkingdays as in excel Networkingdays(a2,b2)=============='''

table='NetWorkingDays'

reddays=[20140101,20140104,20190419,20190423]
halfdays=[20140114,20190418]

lista='FirstDateOfMonth LastDateOfMonth NetWorkingDays NonWorkingDays \
WorkingDays Utalizationrate Workingdays_utilized'

keepcols = [item for item in lista.split(' ') if  item not in  ['']] #exclude empty

df = pd.DataFrame(index = pd.date_range('2014-01-01', '2019-12-31', freq='D'))
df['Date']=df.index
df["YearMonthDay"] = (df.Date.dt.strftime('%Y%m%d')).astype(int)
df['FirstDateOfMonth']= df.Date.dt.to_period("m").dt.start_time
df['LastDateOfMonth']= df.Date.dt.to_period("m").dt.end_time
df['NextDay'] = pd.DatetimeIndex(df.LastDateOfMonth) + pd.DateOffset(days=1)
df.loc[df['YearMonthDay'].isin(reddays), 'RedDays'] = 1
df.loc[df['YearMonthDay'].isin(halfdays), 'HalfDays'] = 0.5
df.update(df[['RedDays','HalfDays']].fillna(0))
df['RedDaysSum'] = df.groupby(['FirstDateOfMonth'])['RedDays'].transform(lambda x : x.sum())
df['HalfDaysSum'] = df.groupby(['FirstDateOfMonth'])['HalfDays'].transform(lambda x : x.sum())
df['NonWorkingDays']=df.RedDaysSum++df.HalfDaysSum
df["NetWorkingDays"] = np.busday_count( df.FirstDateOfMonth.values.astype('datetime64[D]'), 
  df.NextDay.values.astype('datetime64[D]'))
df['WorkingDays']=df.NetWorkingDays-df.NonWorkingDays
df['Utalizationrate']=0.92
df['Workingdays_utilized']=df.WorkingDays*df.Utalizationrate
df.drop_duplicates(['FirstDateOfMonth'],keep= 'first', inplace=True)
df = df[keepcols]
df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='replace')



'''========================================================================'''


table='crm'
pd.io.sql.execute('DROP table IF EXISTS '+str(table), sqlite_db)

for df in pd.read_csv(Main_Path+'\\Indata\\crm.csv', sep=';',encoding = encoding, 
                      low_memory=False,iterator=True,chunksize=chunksize):
    df['e']=1
    df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') 


tenroxFiles=['TENROX201607_201612']

To_date=['EntryDate','PeriodStart','PeriodEnd']
int_float=['TotalTime']

for tenroxFile in tenroxFiles:
    df = pd.read_excel(Main_Path+'\\Indata\\'+tenroxFile+'.xlsx')
    df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns})# Remove spaces from columns
    df[To_date] = df[To_date].values.astype("datetime64[s]")
    df[int_float] = df[int_float].apply(pd.to_numeric, errors='coerce')
    df.update(df[int_float].fillna(0))
    df['Date']=df['FirstDateOfMonth']= df.EntryDate.dt.to_period("m").dt.start_time 
    df[['SkillGroup', 'col2']] = df['UserName'].str.split(n=1, expand=True)
    df[['LastName','FirstName']] = df['col2'].str.rsplit(',',1, expand=True)   
    df=df.applymap(lambda x: x.strip() if type(x) is str else x) #strip all nonNumeric columns
    df['FullName']=df.FirstName.str.cat([df.LastName],sep=' ')
    df['Updated']=Today
    df=df.query('TotalTime > 0 ')
    df['Whole']=1
    df['MinDate'] = df.groupby(['Whole'])['Date'].transform(lambda x : x.min())
    df['MaxDate'] = df.groupby(['Whole'])['Date'].transform(lambda x : x.max())
    df['months_between'] = (df['MaxDate'].dt.to_period('M') -df['Date'].dt.to_period('M'))#Y M W D
    df=df.query('months_between <=13 ')
    df=df[['UserFunctionalGroup', 'UserName', 'ClientName', 'ProjectName', 
       'TaskName', 'TotalTime', 'EntryDate', 'PeriodStart','PeriodEnd',
       'Req.Org.', 'Amount', 'ChangeType', 'IsLeaveTime', 'Estimate', 
       'Date','Updated','FullName']]

df.loc[df.ClientName.isin(['CEE Services AB']) , 'ClientName'] = 'STIM' 

df.loc[df.ProjectName.isin(['Active Works - CEE']) , 'ProjectName'] = 'Active Works - STIM' 
df.loc[df.ProjectName.isin(['PEL Active works - CEE']) , 'ProjectName'] = 'PEL Active works - STIM' 
df.loc[df.ProjectName.isin(['PEL Publishing Agreements - CEE']) , 'ProjectName'] = 'PEL Publishing Agreements - STIM' 
df.loc[df.ProjectName.isin(['Publishing agreements - CEE']) , 'ProjectName'] = 'Publishing agreements - STIM' 
df.loc[df.ProjectName.isin(['Requested Work Updates - CEE']) , 'ProjectName'] = 'Requested Work Updates - STIM' 



df.info()  
    
df=df.query('FullName in ["Ludvig Frisk Brugaard","Karl Bernhard Elsner","Ronny Sentongo"]') 



from workalendar.europe import Sweden
from pandas.tseries.holiday import (AbstractHolidayCalendar , EasterMonday,GoodFriday, Holiday) #next_monday next_monday_or_tuesday,MO,DateOffset , nearest_workday
from datetime import date
from pandas.tseries.offsets import CDay

class HolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('Nyårsdagen', month=1, day=1),
        Holiday('Trettondedag jul',month=1, day=6),
        GoodFriday,
        EasterMonday,
        Holiday('Första maj',month=5, day=1),
        Holiday('Kristi himmelfärdsdag',month=5, day=30),
        Holiday('Sveriges nationaldag',month=6, day=6),
        Holiday('Julafton', month=12, day=24),
        Holiday('Juldagen', month=12, day=25),
        Holiday('Annandag jul',month=12, day=26),
        Holiday('Nyårsafton',month=12, day=31),
       #fyll in andra röda dagar tex midsommar osv
        Holiday('midsommarafton',year=2019,month=6, day=21),
        Holiday('midsommardagen',year=2019,month=6, day=22)
    ]




business = CDay(calendar=HolidayCalendar())

cal=Sweden()
df = pd.read_excel(Main_Path+'\\Indata\\Netdays.xlsx')

#excluding weekends and reddays
df['N_days_between']=(df.apply(lambda x: 
    cal.get_working_days_delta(date(x['Date1'].year, x['Date1'].month, x['Date1'].day), 
                               date(x['Date2'].year, x['Date2'].month, x['Date2'].day)), axis=1))+1

#Next working day excluding weekends and reddays
df = pd.read_excel(Main_Path+'\\Indata\\MyCalendar.xlsx',sheet_name='LastdayOfPayment')
df['Next_1_WorkingDay'] = pd.to_datetime(df.Date).apply(lambda x: cal.add_working_days(date(x.year ,x.month ,x.day), 5))

