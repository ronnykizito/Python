
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  14 13:50:35 2019

@author: F2531197 Ronny Sentongo

Created by	: Ronny Sentongo, Enfogroup AB
Contact*  	: Ronny.Sentongo@enfogroup.com
Purpose*  	: Creating an finacial report 
Costumer 	: 
Output	  	: 

Database used:  SQLITE3 and Azure
    

Notes

This Python-script will extract transform (ETL) and load to Azure.
Python and SQL will be used
Will we used SQLITE3 in development, since the database have much faster 
execution plan then others. We all tables have been developed the we migrate
to the database that the costumer wiil use.



"""

import pandas as pd
import os
import time
import datetime
from workalendar.europe import Sweden
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import CDay
from pandas.tseries.holiday import (AbstractHolidayCalendar , EasterMonday,GoodFriday, Holiday) #next_monday next_monday_or_tuesday,MO,DateOffset , nearest_workday
from datetime import date
import random 
from sqlalchemy import create_engine
import urllib
import sqlite3

'''************************************************************'''


Kund="EkonomiAppen"

Today=time.strftime("%Y-%m-%d")
#chunksize for handle bigdata and therefore handle the memory issue
chunksize=1000*100

#define the main path

Main_Path=r'C:\RONSEN\Costumers\\'+Kund


'''**************************connection to azure******************************'''


server = 'ekonomiappen.database.windows.net'
database = 'EKONOMIAPPEN'
username = 'ekonomiappen'
password = 'Enfo_2019'



driver= '{ODBC Driver 17 for SQL Server}'
#driver= '{SQL Server Native Client 11.0}'  
params = urllib.parse.quote_plus('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
azure_db = create_engine(conn_str)


'''setup a database for SQLITE3. The execution plan for SQLIT3 is much faster then other database'''



sqlite_db = sqlite3.connect(Main_Path+'\\Database\\'+Kund+'.db')
c = sqlite_db.cursor()






'''**************************create folders**********************************'''

lista='indata datamining  data   database '

folders = [item for item in lista.title().split(' ') if  item not in  ['']] #exclude empty



for folder_name in folders:
    newpath = Main_Path+'\\'+folder_name
    if not os.path.exists(newpath):
        os.makedirs(newpath)
        

        
'''**************************kontoplanbas**********************************'''
#source https://sv.wikipedia.org/wiki/BAS-kontoplan 'Indelning enligt nuvarande BAS (f d EU BAS)'
#create kontoplabas table


df = pd.DataFrame(index = pd.Series(range(0,10000)))
df['one']=1
df['Kontoplan']=df.index+1
df=df.query('Kontoplan  <10000')

#kontoplan
df.loc[df.Kontoplan.between(1, 999) | df.Kontoplan.between(9000, 9999), 'Kontogrupp'] = 'Interna konton'
df.loc[df.Kontoplan.between(1000, 1999) , 'Kontogrupp'] = 'Tillgångar'
df.loc[df.Kontoplan.between(2000, 2999) , 'Kontogrupp'] = 'Eget kapital och skulder'
df.loc[df.Kontoplan.between(3000, 3999) , 'Kontogrupp'] = 'Rörelsens inkomster/intäkter'
df.loc[df.Kontoplan.between(4000, 4999) , 'Kontogrupp'] = 'Utgifter/kostnader för varor, material och vissa köpta tjänster'
df.loc[df.Kontoplan.between(5000, 6999) , 'Kontogrupp'] = 'Övriga externa rörelseutgifter/ kostnader'
df.loc[df.Kontoplan.between(7000, 7999) , 'Kontogrupp'] = 'Utgifter/kostnader för personal, avskrivningar m.m.'
df.loc[df.Kontoplan.between(8000, 8999) , 'Kontogrupp'] = 'Finansiella och andra inkomster/ intäkter och utgifter/kostnader'
#kontoplan
df.loc[df.Kontoplan.between(1, 999) | df.Kontoplan.between(9000, 9999), 'Kontogrupp1'] = 'Interna konton'
df.loc[df.Kontoplan.between(1000, 1099) , 'Kontogrupp1'] = 'Immateriella anläggningstillgångar'
df.loc[df.Kontoplan.between(1100, 1299) , 'Kontogrupp1'] = 'Materiella anläggningstillgångar'
df.loc[df.Kontoplan.between(1300, 1399) , 'Kontogrupp1'] = 'Finansiella anläggningstillgångar (långfristiga fordringar)'
df.loc[df.Kontoplan.between(1400, 1499) , 'Kontogrupp1'] = 'Varulager m m'
df.loc[df.Kontoplan.between(1500, 1899) , 'Kontogrupp1'] = 'Kortfristiga fordringar'
df.loc[df.Kontoplan.between(1900, 1999) , 'Kontogrupp1'] = 'Kassa och bank'
df.loc[df.Kontoplan.between(2000, 2099) , 'Kontogrupp1'] = 'Eget kapital'
df.loc[df.Kontoplan.between(2100, 2199) , 'Kontogrupp1'] = 'Obeskattade reserver'
df.loc[df.Kontoplan.between(2200, 2299) , 'Kontogrupp1'] = 'Avsättningar'
df.loc[df.Kontoplan.between(2300, 2999) , 'Kontogrupp1'] = 'Långfristiga skulder'
df.loc[df.Kontoplan.between(3000, 3799) , 'Kontogrupp1'] = 'Nettoomsättning'
df.loc[df.Kontoplan.between(3800, 3999) , 'Kontogrupp1'] = 'Övriga rörelseintäkter'
df.loc[df.Kontoplan.between(4000, 4999) , 'Kontogrupp1'] = 'Rörelsekostnader'
df.loc[df.Kontoplan.between(5000, 6999) , 'Kontogrupp1'] = 'Övriga externa rörelsekostnader'
df.loc[df.Kontoplan.between(7000, 7699) , 'Kontogrupp1'] = 'Personalkostnader'
df.loc[df.Kontoplan.between(7700, 7799) , 'Kontogrupp1'] = 'Nedskrivningar'
df.loc[df.Kontoplan.between(7800, 7899) , 'Kontogrupp1'] = 'Avskrivningar'
df.loc[df.Kontoplan.between(7900, 7999) , 'Kontogrupp1'] = 'Övriga rörelsekostnader'
df.loc[df.Kontoplan.between(8000, 8499) , 'Kontogrupp1'] = 'Finansiella poster'
df.loc[df.Kontoplan.between(8500, 8799) , 'Kontogrupp1'] = 'Fri kontogrupp'
df.loc[df.Kontoplan.between(8800, 8899) , 'Kontogrupp1'] = 'Bokslutsdispositioner'
df.loc[df.Kontoplan.between(8900, 8989) , 'Kontogrupp1'] = 'Skatt'
df.loc[df.Kontoplan.between(8990, 8999) , 'Kontogrupp1'] = 'Resultat'
#kontoplan2 anläggningstillgångar, Summa omsättningstillgångar,Summa Rörelseintäkter,Summa Rörelsekostnader, Avskrivningar
df.loc[df.Kontoplan.between(1, 999) | df.Kontoplan.between(9000, 9999), 'Kontogrupp2'] = 'Interna konton'
df.loc[df.Kontoplan.between(1000, 1399) , 'Kontogrupp2'] = 'Summa anläggningstillgångar'
df.loc[df.Kontoplan.between(1400, 1999) , 'Kontogrupp2'] = 'Summa omsättningstillgångar'
df.loc[df.Kontoplan.between(2000, 2999) , 'Kontogrupp2'] = 'Eget kapital och skulder'
df.loc[df.Kontoplan.between(3000, 3999) , 'Kontogrupp2'] = 'Summa Rörelseintäkter'
df.loc[df.Kontoplan.between(4000, 7699) , 'Kontogrupp2'] = 'Summa Rörelsekostnader'
df.loc[df.Kontoplan.between(7700, 7899) , 'Kontogrupp2'] = 'Avskrivningar'
df.loc[df.Kontoplan.between(7900, 7999) , 'Kontogrupp2'] = 'Övriga rörelsekostnader'
df.loc[df.Kontoplan.between(8000, 8499) , 'Kontogrupp2'] = 'Finansiella poster'
df.loc[df.Kontoplan.between(8500, 8799) , 'Kontogrupp2'] = 'Fri kontogrupp'
df.loc[df.Kontoplan.between(8800, 8899) , 'Kontogrupp2'] = 'Bokslutsdispositioner'
df.loc[df.Kontoplan.between(8900, 8989) , 'Kontogrupp2'] = 'Skatt'
df.loc[df.Kontoplan.between(8990, 8999) , 'Kontogrupp2'] = 'Resultat'
#kontoplan3 Summa Tillgångar Bruttovinst
df.loc[df.Kontoplan.between(1000, 1999) , 'Kontogrupp3'] = 'Summa Tillgångar'
df.loc[df.Kontoplan.between(3000, 4999) , 'Kontogrupp3'] = 'Bruttovinst'
#kontoplan4 Rörelseresultat
df.loc[df.Kontoplan.between(3000, 7699) , 'Kontogrupp4'] = 'Rörelseresultat'
df['KontoplanNyckel'] = df.Kontoplan
df = df.assign(Kontogrupp1Nyckel=(df['Kontogrupp1']).astype('category').cat.codes)

#keep colums
df = df[['Kontoplan','KontoplanNyckel','Kontogrupp','Kontogrupp1','Kontogrupp2',
         'Kontogrupp3','Kontogrupp4','Kontogrupp1Nyckel']]
#create a table in the database
rKontoplan='rKontoplan'
df.to_sql(name=str(rKontoplan),con=sqlite_db,index=False,if_exists='replace')



'''**************************read verfication(transacatons) file**********************************'''

'''**************************import files ending with extension*********************'''


#to date
vars_to_date=['Datum','Kdatum']

#to float or int 
int_float=['Belopp','Konto']
def coerce_df_columns_to_numeric(df, int_float):
    df[int_float] = df[int_float].apply(pd.to_numeric, errors='coerce')
    df.update(df[int_float].fillna(0))

files_names = os.listdir(Main_Path+'\\Indata\\')
File_name = [i for i in files_names if i.endswith('.xlsx')] #txt csv xlsx xlsm json osv.
#get the main folder name
Last_path_name=os.path.basename(os.path.dirname(Main_Path+'\\Indata\\'))

rIndata='rIndata'
pd.io.sql.execute('DROP TABLE IF EXISTS '+ rIndata, sqlite_db)

for the_files in File_name:
    xlfname = Main_Path+'\\Indata\\'+'\\'+the_files
    xl = pd.ExcelFile(xlfname)
    new_sheet_list = [item for item in xl.sheet_names if  item not in  ['BokfAR','']] #exlude these sheets
    for sheet in new_sheet_list:
            df = xl.parse(sheet,encoding = 'ISO-8859-1')#,dtype='str'
            df['Sheetname']=sheet
            df['Filename']=the_files
            df['Last_Path_Name']=Last_path_name
            df['Created_Date'] = Today
            df.columns = df.columns.str.title()
            df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns})# Remove spaces from columns
            coerce_df_columns_to_numeric(df, int_float)
            df[vars_to_date] = df[vars_to_date].values.astype("datetime64[s]")
            df["DatumNyckel"] = (df.Datum.dt.strftime('%Y%m%d')).astype(int)
            df['x'] =  [ random.randint(0,10)  for k in df.index]
            df['Budget_0_10']=((df.x/100)+1 )* df.Belopp
            df=df.query('(Belopp >0 and DatumNyckel>=20120101)') 
            df.to_sql(name=str(rIndata),con=sqlite_db,index=False,if_exists='append') #replace append



'''***************************************create a view for kontonr**************'''

dKontoplan='dKontoplan'
CreateTable_dKontoplan='''
create table %s as
SELECT
      dk.Kontoplan,
      dk.KontoplanNyckel,
      dk.Kontogrupp,
      dk.Kontogrupp1,
      dk.Kontogrupp1Nyckel,
      dk.Kontogrupp2,
      dk.Kontogrupp3,
      dk.Kontogrupp4
FROM
    
    %s dk where dk.KontoplanNyckel in (select konto from %s)


'''%(dKontoplan,rKontoplan,rIndata)

pd.io.sql.execute('DROP table IF EXISTS '+ dKontoplan, sqlite_db)
pd.io.sql.execute(CreateTable_dKontoplan, sqlite_db) #create the new empty table  


'''********************************************#import calender**************'''


DateTableMinMaxPeriod='min_max_period'

CreateTableMinMaxPeriod='''
create table %s as

with q1 as (
SELECT
      Min(a.datum) AS mindate,
      max(a.datum) AS maxdate
FROM
    %s a) SELECT * from q1 

'''%(DateTableMinMaxPeriod,rIndata)



pd.io.sql.execute('DROP table IF EXISTS '+DateTableMinMaxPeriod, sqlite_db)
pd.io.sql.execute(CreateTableMinMaxPeriod, sqlite_db) #create the new empty table



#create a min date and max date for the calander

vars_to_date=['mindate','maxdate']

df = pd.read_sql_query('select * from '+DateTableMinMaxPeriod, sqlite_db)
df[vars_to_date] = df[vars_to_date].values.astype("datetime64[s]")
df['period_start']=((df.mindate - pd.DateOffset(months=0)).dt.to_period("m").dt.start_time).dt.strftime("%Y-%m-%d")
df['period_End']=((df.maxdate + pd.DateOffset(months=0)).dt.to_period("m").dt.end_time).dt.strftime("%Y-%m-%d")

Min_date="[]".join(list(df.period_start))
Max_date="[]".join(list(df.period_End))



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



df = pd.DataFrame(index = pd.date_range(Min_date, Max_date, freq='D'))
df['one']=1
df['Date']=df.index
df['DateKey'] = df.groupby(['one'])['one'].apply(lambda x: x.cumsum())
df["DatumNyckel"] = df.Date.dt.strftime('%Y%m%d')
df["YearMonthDay"] = df.Date.dt.strftime('%Y%m%d')
df["YearMonth"] = df.Date.dt.strftime('%Y%m')
df["DayDate"] = df.Date
df['YearCode']= df.Date.dt.strftime('%Y')
df["IsLeapYear"] = df.Date.dt.is_leap_year
df['FirstDateOfYear']= df.Date.dt.to_period("Y").dt.start_time
df['LastDateOfYear']= df.Date.dt.to_period("Y").dt.end_time
df['QuarterCode']= df.Date.dt.strftime('%Y').astype(str)+df.Date.dt.quarter.astype(str).str.pad(width=2,fillchar='0')
df['QuaterNumber']= df.Date.dt.quarter
df['QuaterName']= 'Q'+df.Date.dt.quarter.astype(str)
df['QuaterLongName']= df.Date.dt.strftime('%Y').astype(str)+'Q'+df.Date.dt.quarter.astype(str)
df['FirstMonthOfQuarter'] = (df.Date.dt.to_period("Q").dt.start_time).dt.strftime('%m')
df['LastMonthOfQuarter'] = (df.Date.dt.to_period("Q").dt.end_time).dt.strftime('%m')
df['FirstDateOfQuarter'] = df.Date.dt.to_period("Q").dt.start_time
df['LastDateOfQuarter'] = df.Date.dt.to_period("Q").dt.end_time
df["MonthCode"] = df.Date.dt.strftime('%Y%m')
df["MonthOfYearNumber"] = df.Date.dt.strftime('%m')
df['MonthNameEnglishLong']= df.Date.dt.strftime('%B')
df['MonthNameEnglishShort']= df.Date.dt.strftime('%b')
df.loc[df['MonthNameEnglishLong'].isin(['January']), 'MonthNameSwedishLong'] = 'Januari'
df.loc[df['MonthNameEnglishLong'].isin(['February']), 'MonthNameSwedishLong'] = 'Februari'
df.loc[df['MonthNameEnglishLong'].isin(['March']), 'MonthNameSwedishLong'] = 'Mars'
df.loc[df['MonthNameEnglishLong'].isin(['April']), 'MonthNameSwedishLong'] = 'April'
df.loc[df['MonthNameEnglishLong'].isin(['May']), 'MonthNameSwedishLong'] = 'Maj'
df.loc[df['MonthNameEnglishLong'].isin(['June']), 'MonthNameSwedishLong'] = 'Juni'
df.loc[df['MonthNameEnglishLong'].isin(['July']), 'MonthNameSwedishLong'] = 'Juli'
df.loc[df['MonthNameEnglishLong'].isin(['August']), 'MonthNameSwedishLong'] = 'Augusti'
df.loc[df['MonthNameEnglishLong'].isin(['September']), 'MonthNameSwedishLong'] = 'September'
df.loc[df['MonthNameEnglishLong'].isin(['October']), 'MonthNameSwedishLong'] = 'Oktober'
df.loc[df['MonthNameEnglishLong'].isin(['November']), 'MonthNameSwedishLong'] = 'November'
df.loc[df['MonthNameEnglishLong'].isin(['December']), 'MonthNameSwedishLong'] = 'December'
df['MonthNameSwedishShort']= df.MonthNameSwedishLong.str[0:3].str.lower() #title upper lower
df['FirstDateOfMonth']= df.Date.dt.to_period("m").dt.start_time
df['LastDateOfMonth']= df.Date.dt.to_period("m").dt.end_time
df['NumberOfDaysInMonth']= df.Date.dt.daysinmonth
df['WeekCode']= df.Date.dt.strftime('%Y').astype(str)+df.Date.dt.weekofyear.astype(str).str.pad(width=2,fillchar='0') 
df['WeekOfYearNumber']= df.Date.dt.weekofyear
df['WeekName']= 'W'+df.Date.dt.weekofyear.astype(str).str.pad(width=2,fillchar='0')
df['WeekLongName']= df.Date.dt.strftime('%Y').astype(str)+'W'+df.Date.dt.weekofyear.astype(str).str.pad(width=2,fillchar='0') 
df['FirstDateOfWeek']= df.Date.dt.to_period("W").dt.start_time
df['LastDateOfWeek']= df.Date.dt.to_period("W").dt.end_time
df["DayOfYear"] = df.Date.dt.dayofyear
df['DayOfQuarter'] = df.groupby(['QuarterCode'])['one'].apply(lambda x: x.cumsum())
df["DayOfMonth"] = df.Date.dt.day
df["DayOfWeek"] = df.Date.dt.dayofweek+1
df['DayOfWeekNameEnglishLong']= df.Date.dt.strftime('%A') #df.Date.dt.weekday_name
df['DayOfWeekNameEnglishShort']= df.Date.dt.strftime('%a')
df.loc[df['DayOfWeekNameEnglishLong'].isin(['Monday']), 'DayOfWeekNameSwedishLong'] = 'Måndag'
df.loc[df['DayOfWeekNameEnglishLong'].isin(['Tuesday']), 'DayOfWeekNameSwedishLong'] = 'Tisdag'
df.loc[df['DayOfWeekNameEnglishLong'].isin(['Wednesday']), 'DayOfWeekNameSwedishLong'] = 'Onsdag'
df.loc[df['DayOfWeekNameEnglishLong'].isin(['Thursday']), 'DayOfWeekNameSwedishLong'] = 'Torsdag'
df.loc[df['DayOfWeekNameEnglishLong'].isin(['Friday']), 'DayOfWeekNameSwedishLong'] = 'Fredag'
df.loc[df['DayOfWeekNameEnglishLong'].isin(['Saturday']), 'DayOfWeekNameSwedishLong'] = 'Lördag'
df.loc[df['DayOfWeekNameEnglishLong'].isin(['Sunday']), 'DayOfWeekNameSwedishLong'] = 'Söndag'
df['DayOfWeekNameSwedishShort']= df.DayOfWeekNameSwedishLong.str[0:3].str.lower() #title upper lower
df['PreviousDateYear'] = df.Date - pd.DateOffset(years=1)
df['PreviousDateMonth'] = df.Date - pd.DateOffset(months=1)
df['PreviousDateMonthStart'] = (df.Date - pd.DateOffset(months=1)).dt.to_period("m").dt.start_time
df['PreviousDateMonthEnd'] = (df.Date - pd.DateOffset(months=1)).dt.to_period("m").dt.end_time
df['PreviousDateWeek'] = df.Date - pd.DateOffset(weeks=1)
df['PreviousDateDay'] = df.Date - pd.DateOffset(days=1)
df['NextDateDay'] = df.Date + pd.DateOffset(days=1)
df['date1'] = pd.Timestamp('2016-12-27')
df['months_between'] = (df['date1'].dt.to_period('M') -df['Date'].dt.to_period('M'))#Y M W D
df['Datum_swe']= df.MonthNameSwedishShort+'-'+df.Date.dt.strftime('%Y')
df['IsWeekend']='False'
df.loc[df['DayOfWeek'].isin([6,7]), 'IsWeekend'] = 'True'
df.drop(['one','DateKey'], axis=1,inplace=True)


dDate='dDate'
df.to_sql(name=str(dDate),con=sqlite_db,index=False,if_exists='replace')  


'''*************************stage **************************************'''
sIndata='sIndata'

CreateTableStage_rawData_vnr='''
create table %s as

SELECT DISTINCT
       ri.DatumNyckel,
       ri.Ver_Nr,
       ri.Rad,
       ri.Belopp,
       ri.Definitiv,
       ri.Kdatum,
       ri.Konto,
       ri.Kvant,
       ri.Mstruken,
       ri.Struken,
       ri.Budget_0_10,
       dk.KontoplanNyckel,
       dk.Kontogrupp,
       dk.Kontogrupp1,
       dk.Kontogrupp1Nyckel,
       dk.Kontogrupp2,
       dk.Kontogrupp3,
       dk.Kontogrupp4
FROM
    %s ri
     left JOIN %s dk ON dk.KontoplanNyckel = ri.Konto
     left JOIN %s dd ON dd.DatumNyckel = ri.DatumNyckel

'''%(sIndata,rIndata,dKontoplan,dDate) 



pd.io.sql.execute('DROP table IF EXISTS '+ sIndata, sqlite_db)
pd.io.sql.execute(CreateTableStage_rawData_vnr, sqlite_db) #create the new empty table  


          
#
'''*************************Analys to PBI (see ekonomiapppen)**********************'''
'''https://www.hitta.se/f%C3%B6retagsinformation/enfo+sweden+ab/5565818613'''

sResultaträkning='sResultaträkning'

CreateTable_sResultaträkning='''

create table %s as
with 
q1 as (
SELECT DISTINCT
       ri.Kontogrupp1 AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ('Nettoomsättning', 'Övriga rörelseintäkter', 'Rörelsekostnader',
     'Övriga externa rörelsekostnader', 'Personalkostnader', 'Finansiella poster')),
q2 as
(
SELECT DISTINCT
       'Summa rörelseintäkter' AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ('Nettoomsättning', 'Övriga rörelseintäkter')),
q3 as
(SELECT DISTINCT
       'Bruttovinst' AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ('Nettoomsättning', 'Övriga rörelseintäkter', 'Rörelsekostnader')),
q4 as
(SELECT DISTINCT
       'Summa Rörelsekostnader' AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ( 'Rörelsekostnader','Övriga externa rörelsekostnader', 'Personalkostnader')),
q5 as
(SELECT DISTINCT
       'Rörelseresultat'  AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ('Rörelsekostnader','Övriga externa rörelsekostnader', 
     'Personalkostnader', 'Nettoomsättning','Övriga rörelseintäkter')),
q6 as
(SELECT DISTINCT
       'Resultat efter avskrivningar' AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
      ri.Kontogrupp2 in ("Avskrivningar") or
       ri.Kontogrupp1 in
   ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
   'Nettoomsättning','Övriga rörelseintäkter')),
q7 as
(SELECT DISTINCT
       'RESULTAT EFTER FINASIELLA POSTER' AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
    'Nettoomsättning','Övriga rörelseintäkter','Finansiella poster')),
q8 as
(SELECT DISTINCT
       'RESULTAT efter finansiella poster och dispositioner' AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
   'Nettoomsättning','Övriga rörelseintäkter','Finansiella poster'
    ,'Extraordinära poster','Bokslutsdispositioner')),
q9 as
(SELECT DISTINCT
       'BERÄKNAT RESULTAT'  AS Sammanfattning,
       ri.*
FROM
    sIndata ri
WHERE
     ri.Kontogrupp1 IN ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
    'Nettoomsättning','Övriga rörelseintäkter','Finansiella poster'
   ,'Extraordinära poster','Bokslutsdispositioner','Skatt') ),
q10 as     
(select * from q1
  union 
  select * from q2
union 
  select * from q3
union 
  select * from q4
union 
  select * from q5
union 
  select * from q6
union 
  select * from q7
union 
  select * from q8
union 
  select * from q9
)
select * from q10
'''%(sResultaträkning)


pd.io.sql.execute('DROP table IF EXISTS '+sResultaträkning, sqlite_db)
pd.io.sql.execute(CreateTable_sResultaträkning, sqlite_db) #create the new empty table

sql_query= """  
select * from %s
    
"""%(sResultaträkning)

Negativebelopp=['Rörelsekostnader','Övriga externa rörelsekostnader',
                'Personalkostnader','Rörelseresultat']

fResultaträkning='fResultaträkning'
pd.io.sql.execute('DROP table IF EXISTS '+fResultaträkning, sqlite_db)
for df in pd.read_sql_query(sql_query, sqlite_db,chunksize=chunksize): #get data from SQLITE
        #costs should be negative
    df.loc[df['Kontogrupp1'].isin(Negativebelopp), 'Belopp'] = -1*df.Belopp
    df.loc[df['Kontogrupp1'].isin(Negativebelopp), 'Budget_0_10'] = -1*df.Budget_0_10
    ##
    df.loc[df['Sammanfattning'].isin(['Nettoomsättning']), 'BokslutsNyckel'] = 1
    df.loc[df['Sammanfattning'].isin(['Övriga rörelseintäkter']), 'BokslutsNyckel'] = 2
    df.loc[df['Sammanfattning'].isin(['Summa rörelseintäkter']), 'BokslutsNyckel'] = 3
    df.loc[df['Sammanfattning'].isin(['Rörelsekostnader']), 'BokslutsNyckel'] = 4
    df.loc[df['Sammanfattning'].isin(['Bruttovinst']), 'BokslutsNyckel'] = 5
    df.loc[df['Sammanfattning'].isin(['Övriga externa rörelsekostnader']), 'BokslutsNyckel'] = 6
    df.loc[df['Sammanfattning'].isin(['Personalkostnader']), 'BokslutsNyckel'] = 7
    df.loc[df['Sammanfattning'].isin(['Summa Rörelsekostnader']), 'BokslutsNyckel'] = 8
    df.loc[df['Sammanfattning'].isin(['Rörelseresultat']), 'BokslutsNyckel'] = 9
    df.loc[df['Sammanfattning'].isin(['Resultat efter avskrivningar']), 'BokslutsNyckel'] = 10
    df.loc[df['Sammanfattning'].isin(['Finansiella poster']), 'BokslutsNyckel'] = 11
    df.loc[df['Sammanfattning'].isin(['RESULTAT EFTER FINASIELLA POSTER']), 'BokslutsNyckel'] = 12
    df.loc[df['Sammanfattning'].isin(['RESULTAT efter finansiella poster och dispositioner']), 'BokslutsNyckel'] = 13
    df.loc[df['Sammanfattning'].isin(['BERÄKNAT RESULTAT']), 'BokslutsNyckel'] = 14
    df.to_sql(name=str(fResultaträkning),con=sqlite_db,index=False,if_exists='append')






'''*************************create dimension tables*******************************''' 


dGrpKontoplan2='dGrpKontoplan2'

CreateTable_dGrpKontoplan2='''
create table %s as
SELECT distinct
      dk.Kontogrupp1Nyckel,
      dk.Kontogrupp1
FROM
    %s ri
    LEFT JOIN %s dk ON dk.Kontoplan = ri.Konto  
'''%(dGrpKontoplan2,rIndata,dKontoplan)

pd.io.sql.execute('DROP table IF EXISTS '+ dGrpKontoplan2, sqlite_db)
pd.io.sql.execute(CreateTable_dGrpKontoplan2, sqlite_db) #create the new empty table  

#date created
df=pd.DataFrame({'TimeCreated':[Today]})
df.to_sql(name='dDateCreated',con=sqlite_db,index=False,if_exists='replace') 





dResultaträkning='dResultaträkningNyckel'

CreateTable_dResultaträkning='''
create table %s as
SELECT DISTINCT
       Sammanfattning,
       BokslutsNyckel
FROM
    %s 

'''%(dResultaträkning,fResultaträkning)

pd.io.sql.execute('DROP table IF EXISTS '+ dResultaträkning, sqlite_db)
pd.io.sql.execute(CreateTable_dResultaträkning, sqlite_db) #create the new empty table 


#costumer

df=pd.DataFrame({'Updated':[datetime.datetime.now().strftime("%Y-%m-%d %H:%M")]})
df['Kund']=Kund
df.to_sql(name='dDateCreatedKund',con=sqlite_db,index=False,if_exists='replace') 

#KPI
ColumnLabelsNames=['Utfall','Budget','AvvikelseUB',
                   'ActUtfall','ActBudget','AckAvikelseUB',
                   'UtfallUppräknat','BudgetHelår','Avv_uppräknatUB']
df=pd.DataFrame({'ColumnLabels':ColumnLabelsNames})
df['SortColumnNames']=df.index+1
df.to_sql(name='ColumnLabels',con=sqlite_db,index=False,if_exists='replace') 


RowLabelsNames=['Nettoomsättning','Övriga rörelseintäkter','Summa rörelseintäkter',
                'Rörelsekostnader','Bruttovinst','Övriga externa rörelsekostnader',
                'Personalkostnader','Summa Rörelsekostnader','Rörelseresultat',
                'Resultat efter avskrivningar','Finansiella poster',
                'RESULTAT EFTER FINASIELLA POSTER',
                'RESULTAT efter finansiella poster och dispositioner',
                'BERÄKNAT RESULTAT']


df=pd.DataFrame({'RowLabels':RowLabelsNames})
df['SortRowNames']=df.index+1
df.to_sql(name='RowLabels',con=sqlite_db,index=False,if_exists='replace') 



'''************************************************************************'''

'''*****************************migrate to database***************************'''


start_time = time.time()
chuck_sum=0

tables_to_azure=['dKontoplan','dDate','dDateCreatedKund','ColumnLabels',
                 'RowLabels','fResultaträkning','dResultaträkning_Nyckel']

for table_to_azure in tables_to_azure:
    sql_query= """  select * from %s"""%(table_to_azure)
    pd.io.sql.execute('DROP table IF EXISTS '+str(table_to_azure), azure_db)
    for df in pd.read_sql_query(sql_query, sqlite_db,chunksize=chunksize): #get data from SQLITE
        chuck_sum += chunksize
        print(chuck_sum,' rows from SQLITE3 ','Start ',time.strftime("%H:%M:%S"),'execution time ',time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))
        df.to_sql(name=str(table_to_azure),con=azure_db,index=False,if_exists='append') #migrate to another database
        print(chuck_sum,'rows to',str(table_to_azure),'execution time ',time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))

'''*****************************extract to azure one file***************************'''

##send one table
#table_to_azure='dResultaträkningNyckel'
#sql_query= """  select * from %s"""%(table_to_azure)
#pd.io.sql.execute('DROP table IF EXISTS '+ str(table_to_azure), azure_db)
#for df in pd.read_sql_query(sql_query, sqlite_db,chunksize=chunksize): #get data from SQLITE
#    df.to_sql(name=str(table_to_azure),con=azure_db,index=False,if_exists='append')




sqlite_db.close()

print('Script successfully completed')