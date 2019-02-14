# -*- coding: utf-8 -*-
"""
Created on Sat Feb  9 17:31:52 2019

@author: F2531197
"""


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
from datetime import datetime
from workalendar.europe import Sweden
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import CDay
from pandas.tseries.holiday import (AbstractHolidayCalendar , EasterMonday,GoodFriday, Holiday) #next_monday next_monday_or_tuesday,MO,DateOffset , nearest_workday
from datetime import date
import time


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

import pyodbc
from sqlalchemy import create_engine
import urllib

server = 'ronsen.database.windows.net'
database = 'AdventureWorksLT'
username = 'ronsen'
password = 'Kate1299se'

driver= '{ODBC Driver 17 for SQL Server}'
#driver= '{SQL Server Native Client 11.0}'  
params = urllib.parse.quote_plus('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
azure_db = create_engine(conn_str)


        
'''**************************kontoplanbas**********************************'''
#source https://sv.wikipedia.org/wiki/BAS-kontoplan 'Indelning enligt nuvarande BAS (f d EU BAS)'
#create kontoplabas table







df = pd.DataFrame(index = pd.Series(range(1000,1100)))
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


#create a table in the database
dKontoplan='dKontoplan'

#pd.io.sql.execute('DROP table IF EXISTS '+ dKontoplan, azure_db)
df.to_sql(name=str(dKontoplan),con=azure_db,index=False,if_exists='replace')

