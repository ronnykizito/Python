# -*- coding: utf-8 -*-
"""
Created on Fri Feb  8 12:50:35 2019

@author: F2531197 Ronny Sentongo

Created by	: Ronny Sentongo, Enfogroup AB
Contact*  	: Ronny.Sentongo@enfogroup.com
Purpose*  	: Creating an finacial report for PWRBI
Costumer 	: 
Output	  	: 

Database used: SQLITE3 or SQLServer or Azure
    

Notes

This is a Python-script, therefore some downloads/installation will be needed
1. You need to have PowerBI downloaded from Microsoft Store
2. Download and install Python 3 https://www.python.org/downloads/
3. Install Pip, if not included, make sure that you computer recognise Pip
    controlpanel -system-avanced envorialment-envorialment variables- path edit (then add new path) these path below  
4. Now install the modules your using (se import): The is needed for PWRBI
5. This script can be applied directly in PWRBI, or creating a bat as python is a text file, or run it directly in SSMS
"""

import pandas as pd
import os
import time

from datetime import datetime
from workalendar.europe import Sweden
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import CDay
from pandas.tseries.holiday import (AbstractHolidayCalendar , EasterMonday,GoodFriday, Holiday) #next_monday next_monday_or_tuesday,MO,DateOffset , nearest_workday
from datetime import date


Kund="EkonomiAppen"

Today=time.strftime("%Y-%m-%d")
#chunksize for handle bigdata and therefore handle the memory issue
#chunksize=1000*50

#define the main path

Main_Path=r'C:\RONSEN\Costumers\\'+Kund




'''**************************create folders**********************************'''

lista='indata datamining  data   database '

folders = [item for item in lista.title().split(' ') if  item not in  ['']] #exclude empty



for folder_name in folders:
    newpath = Main_Path+'\\'+folder_name
    if not os.path.exists(newpath):
        os.makedirs(newpath)
        
'''**************************setup a databse**********************************'''

import sqlite3

sqlite_db = sqlite3.connect(Main_Path+'\\Database\\'+Kund+'.db')
c = sqlite_db.cursor()
        
'''**************************kontoplanbas**********************************'''
#source https://sv.wikipedia.org/wiki/BAS-kontoplan 'Indelning enligt nuvarande BAS (f d EU BAS)'
#create kontoplabas table


df = pd.DataFrame(index = pd.Series(range(0,10000)))
df['one']=1
df['Kontoplan']=df.index+1
df=df.query('Kontoplan  <10000')
df['KontoplanNyckel'] = df.groupby(['one'])['one'].apply(lambda x: x.cumsum())
df['Kontonr1'] = df.Kontoplan.astype(str).str[0:1]
df['Kontonr2'] = df.Kontoplan.astype(str).str[0:2]
#grp2
df.loc[df.Kontonr2.isin(['10']) & (df.Kontoplan>999), 'GrpKontoplan2'] = 'Immateriella anläggningstillgångar'
df.loc[df['Kontonr2'].isin(['11','12']) & (df.Kontoplan>999), 'GrpKontoplan2'] = 'Materiella anläggningstillgångar'
df.loc[df['Kontonr2'].isin(['13'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Finansiella anläggningstillgångar (långfristiga fordringar)'
df.loc[df['Kontonr2'].isin(['14'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Varulager m m'
df.loc[df['Kontonr2'].isin(['15','16','17','18'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Kortfristiga fordringar'
df.loc[df['Kontonr2'].isin(['19'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Kassa och bank'
df.loc[df['Kontonr2'].isin(['20'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Eget kapital'
df.loc[df['Kontonr2'].isin(['21'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Obeskattade reserver'
df.loc[df['Kontonr2'].isin(['22'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Avsättningar'
df.loc[df['Kontonr2'].isin(['23','24','25','26','27','28','29'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Långfristiga skulder'
df.loc[df['Kontonr2'].isin(['30','31','32','33','34','35','36','37'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Nettoomsättning'
df.loc[df['Kontonr2'].isin(['38','39'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Övriga rörelseintäkter'
df.loc[df['Kontonr1'].isin(['4'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Rörelsekostnader'
df.loc[df['Kontonr1'].isin(['5','6'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Övriga externa rörelsekostnader'
df.loc[df['Kontonr2'].isin(['70','71','72','73','74','75','76'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Personalkostnader'
df.loc[df['Kontonr2'].isin(['77'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Nedskrivningar'
df.loc[df['Kontonr2'].isin(['78'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Avskrivningar'
df.loc[df['Kontonr2'].isin(['79'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Övriga rörelsekostnader'
df.loc[df['Kontonr2'].isin(['80','81','82','83','84'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Finansiella poster'
df.loc[df['Kontonr2'].isin(['88'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Bokslutsdispositioner'
df.loc[df['Kontonr1'].isin(['9'])& (df.Kontoplan>999), 'GrpKontoplan2'] = 'Interna konton'
df.loc[df['Kontoplan']<1000, 'GrpKontoplan2'] = 'Interna konton'
#grp3 Summa anläggningstillgångar, Summa omsättningstillgångar,Summa Rörelseintäkter,Summa Rörelsekostnader, Avskrivningar
df.loc[df['Kontonr2'].isin(['10','11','12','13'])& (df.Kontoplan>999), 'GrpKontoplan3'] = 'Summa anläggningstillgångar'
df.loc[df['Kontonr2'].isin(['14','15','16','17','18','19'])& (df.Kontoplan>999), 'GrpKontoplan3'] = 'Summa omsättningstillgångar'
df.loc[df['Kontonr1'].isin(['3'])& (df.Kontoplan>999), 'GrpKontoplan3'] = 'Summa Rörelseintäkter'
df.loc[df['Kontonr1'].isin(['4','5','6'])& (df.Kontoplan>999), 'GrpKontoplan3'] = 'Summa Rörelsekostnader'
df.loc[df['Kontonr2'].isin(['70','71','72','73','74','75','76'])& (df.Kontoplan>999), 'GrpKontoplan3'] = 'Summa Rörelsekostnader'
df.loc[df['Kontonr2'].isin(['77','78'])& (df.Kontoplan>999), 'GrpKontoplan3'] = 'Avskrivningar'
#grp4 Summa Tillgångar Bruttovinst
df.loc[df['Kontonr1'].isin(['1'])& (df.Kontoplan>999), 'GrpKontoplan4'] = 'Summa Tillgångar'
df.loc[df['Kontonr1'].isin(['3','4'])& (df.Kontoplan>999), 'GrpKontoplan4'] = 'Bruttovinst'
#grp5 Summa Rörelseresultat
df.loc[df['Kontonr1'].isin(['3','4','5','6'])& (df.Kontoplan>999), 'GrpKontoplan5'] = 'Rörelseresultat'
df.loc[df['Kontonr2'].isin(['70','71','72','73','74','75','76'])& (df.Kontoplan>999), 'GrpKontoplan5'] = 'Rörelseresultat'
df = df.assign(GrpKontoplan2Nyckel=(df['GrpKontoplan2']).astype('category').cat.codes)

#keep colums
df = df[['Kontoplan','KontoplanNyckel','GrpKontoplan2','GrpKontoplan3',
         'GrpKontoplan4','GrpKontoplan5','GrpKontoplan2Nyckel']]
#create a table in the database
dKontoplan='dKontoplan'
df.to_sql(name=str(dKontoplan),con=sqlite_db,index=False,if_exists='replace')

'''**************************read verfication file**********************************'''

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

rawData_vnr='rIndata'
pd.io.sql.execute('DROP TABLE IF EXISTS '+ rawData_vnr, sqlite_db)

for the_files in File_name:
    xlfname = Main_Path+'\\Indata\\'+'\\'+the_files
    xl = pd.ExcelFile(xlfname)
    new_sheet_list = [item for item in xl.sheet_names if  item not in  ['BokfAR','']] #exlude these sheets
    for sheet in new_sheet_list:
            df = xl.parse(sheet,encoding = 'ISO-8859-1')
            df['Sheetname']=sheet
            df['Filename']=the_files
            df['Last_Path_Name']=Last_path_name
            df['Created_Date'] = Today
            df.columns = df.columns.str.title()
            df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns})# Remove spaces from columns
            coerce_df_columns_to_numeric(df, int_float)
            df[vars_to_date] = df[vars_to_date].values.astype("datetime64[s]")
            df["Date_ID"] = df.Datum.dt.strftime('%Y%m%d')
            df.to_sql(name=str(rawData_vnr),con=sqlite_db,index=False,if_exists='append') #replace append


'''********************************************#import calender**************'''


DateViewMinMaxPeriod='min_max_period'

CreateViewMinMaxPeriod='''
create view %s as

with q1 as (
SELECT
      Min(a.datum) AS mindate,
      max(a.datum) AS maxdate
FROM
    %s a) SELECT * from q1 

'''%(DateViewMinMaxPeriod,rawData_vnr)



pd.io.sql.execute('DROP VIEW IF EXISTS '+DateViewMinMaxPeriod, sqlite_db)
pd.io.sql.execute(CreateViewMinMaxPeriod, sqlite_db) #create the new empty table



#create a min date and max date for the calander

vars_to_date=['mindate','maxdate']

df = pd.read_sql_query('select * from '+DateViewMinMaxPeriod, sqlite_db)
df[vars_to_date] = df[vars_to_date].values.astype("datetime64[s]")
df['period_start']=((df.mindate - pd.DateOffset(months=1)).dt.to_period("m").dt.start_time).dt.strftime("%Y-%m-%d")
df['period_End']=((df.maxdate + pd.DateOffset(months=12)).dt.to_period("m").dt.end_time).dt.strftime("%Y-%m-%d")

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
df["Date_ID"] = df.Date.dt.strftime('%Y%m%d')
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
df["DateID"] = df.Date.dt.strftime('%Y-%m-%d')

dDate='dDate'
df.to_sql(name=str(dDate),con=sqlite_db,index=False,if_exists='replace')  

'''*************************create a vfact iew **************************************'''
FactView_rawData_vnr='v'+rawData_vnr

CreateViewStage_rawData_vnr='''
create view %s as

SELECT DISTINCT
       dd.YearMonth AS Manad,
       ri.Date_ID,
       ri.Ver_Nr,
       ri.Rad,
       ri.Belopp,
       ri.Definitiv,
       ri.Kdatum,
       ri.Konto,
       ri.Kvant,
       ri.Mstruken,
       ri.Struken,
       dk.GrpKontoplan2Nyckel,
       dk.KontoplanNyckel
FROM
    %s ri
    LEFT JOIN %s dk ON dk.KontoplanNyckel = ri.Konto
    LEFT JOIN %s dd ON dd.Date_ID = ri.Date_ID

'''%(FactView_rawData_vnr,rawData_vnr,dKontoplan,dDate) 



pd.io.sql.execute('DROP view IF EXISTS '+ FactView_rawData_vnr, sqlite_db)
pd.io.sql.execute(CreateViewStage_rawData_vnr, sqlite_db) #create the new empty table  

'''*************************create dimension tables*******************************''' 


View_dGrpKontoplan2='dGrpKontoplan2'

CreateView_dGrpKontoplan2='''
create view %s as
SELECT distinct
      dk.GrpKontoplan2Nyckel,
      dk.GrpKontoplan2
FROM
    %s ri
    LEFT JOIN %s dk ON dk.Kontoplan = ri.Konto  
'''%(View_dGrpKontoplan2,rawData_vnr,dKontoplan)

pd.io.sql.execute('DROP view IF EXISTS '+ View_dGrpKontoplan2, sqlite_db)
pd.io.sql.execute(CreateView_dGrpKontoplan2, sqlite_db) #create the new empty table  

#date created
df=pd.DataFrame({'TimeCreated':[Today]})
df.to_sql(name='dDateCreated',con=sqlite_db,index=False,if_exists='replace') 

#costumer

df=pd.DataFrame({'Kund':[Kund]})
df.to_sql(name='Kund',con=sqlite_db,index=False,if_exists='replace') 




sqlite_db.close()

print('Script successfully completed')
           

'''*************************Analys to PBI**************************************'''
#
#pwrbi_view='vPwrbiEkonomiAppen'
#sql_query='''
#create view %s as
#with 
#q1 as (
#
#SELECT
#      a.Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where a.Sammanfattning in 
#    ('Nettoomsättning','Övriga rörelseintäkter','Rörelsekostnader',
#'Övriga externa rörelsekostnader','Personalkostnader','Finansiella poster')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q2 as
#        (
#        SELECT
#      'Summa rörelseintäkter' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where a.Sammanfattning in 
#    ('Nettoomsättning', 'Övriga rörelseintäkter')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q3 as
#        (
#        SELECT
#      'Bruttovinst' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where a.Sammanfattning in 
#    ('Nettoomsättning', 'Övriga rörelseintäkter','Rörelsekostnader')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q4 as
#        (
#        SELECT
#      'Summa Rörelsekostnader' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where a.Sammanfattning in 
#    ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q5 as
#        (
#        SELECT
#      'Rörelseresultat' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where a.Sammanfattning in 
#    ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
#    'Nettoomsättning','Övriga rörelseintäkter')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q6 as
#        (
#        SELECT
#      'Resultat efter avskrivningar' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where a.Sammanfattning1 in ("Avskrivningar") or
#        a.Sammanfattning in
#    ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
#    'Nettoomsättning','Övriga rörelseintäkter')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q7 as
#        (
#        SELECT
#      'RESULTAT EFTER FINASIELLA POSTER' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where 
#        a.Sammanfattning in
#    ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
#    'Nettoomsättning','Övriga rörelseintäkter','Finansiella poster')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q8 as
#        (
#        SELECT
#      'RESULTAT efter finansiella poster och dispositioner' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where 
#        a.Sammanfattning in
#    ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
#    'Nettoomsättning','Övriga rörelseintäkter','Finansiella poster'
#    ,'Extraordinära poster','Bokslutsdispositioner')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q9 as
#        (
#       SELECT
#      'BERÄKNAT RESULTAT' as Sammanfattning,
#      a.Manad,
#      Sum(a.Summa) AS Sum_Summa
#FROM
#    %s a
#    where 
#        a.Sammanfattning in
#    ('Rörelsekostnader','Övriga externa rörelsekostnader','Personalkostnader',
#    'Nettoomsättning','Övriga rörelseintäkter','Finansiella poster'
#    ,'Extraordinära poster','Bokslutsdispositioner','Skatt')
#GROUP BY
#        a.Sammanfattning,
#        a.Manad),
#q10 as     
#(select * from q1
#  union 
#  select * from q2
#union 
#  select * from q3
#union 
#  select * from q4
#union 
#  select * from q5
#union 
#  select * from q6
#union 
#  select * from q7
#union 
#  select * from q8
#union 
#  select * from q9
#) 
#select *,
#case when Sammanfattning='Nettoomsättning' then 1
#      when Sammanfattning='Övriga rörelseintäkter' then 2
#      when Sammanfattning='Summa rörelseintäkter' then 3
#      when Sammanfattning='Rörelsekostnader' then 4
#      when Sammanfattning='Bruttovinst' then 5
#      when Sammanfattning='Övriga externa rörelsekostnader' then 6
#      when Sammanfattning='Personalkostnader' then 7
#      when Sammanfattning='Summa Rörelsekostnader' then 8
#      when Sammanfattning='Rörelseresultat' then 9
#      when Sammanfattning='Resultat efter avskrivningar' then 10
#      when Sammanfattning='Finansiella poster' then 11
#      when Sammanfattning='RESULTAT EFTER FINASIELLA POSTER' then 12
#      when Sammanfattning='RESULTAT efter finansiella poster och dispositioner' then 13
#      when Sammanfattning='BERÄKNAT RESULTAT' then 14
#      end as Rank_var from q10 
#
#'''%(pwrbi_view,
#fMaster_view,
#fMaster_view,
#fMaster_view,
#fMaster_view,
#fMaster_view,
#fMaster_view,
#fMaster_view,
#fMaster_view,
#fMaster_view)
#
#
#pd.io.sql.execute('DROP VIEW IF EXISTS '+pwrbi_view, sqlite_db)
#pd.io.sql.execute(sql_query, sqlite_db) #create the new empty table


'''****************************************end*********************************************************************************************'''
#drop tables views not needed

#pd.io.sql.execute('DROP VIEW IF EXISTS '+date_view, sqlite_db)


