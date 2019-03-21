
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
chunksize=1000*1000

#define the main path

Main_Path=r'C:\RONSEN\Costumers\\'+Kund


'''**************************connection to azure******************************'''


server = 'ekonomiappen.database.windows.net'
database = 'EKONOMIAPPEN'
username = 'ekonomiappen'
password = 'Enfo_2019'

migrate_to_databse='azure_db' #specify the database to migrate to

driver= '{ODBC Driver 17 for SQL Server}'

params = urllib.parse.quote_plus('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
migrate_to_databse = create_engine(conn_str)



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
        

#get the main folder name
Last_path_name=os.path.basename(os.path.dirname(Main_Path+'\\Indata\\'))


'''========================================================================'''
#source https://sv.wikipedia.org/wiki/BAS-kontoplan 'Indelning enligt nuvarande BAS (f d EU BAS)'
#create kontoplabas table




'''=========================Balansrakning==================================='''

balansrakning=list(range(1000, 3000 ))

Immateriella=['Immateriella anläggningstillgångar']
Materiella=['Materiella anläggningstillgångar']
FinansiellaAnlaggningstillgangar=['Finansiella anläggningstillgångar (långfristiga fordringar)']
SummaAnlaggningstillgangar=Immateriella+Materiella+FinansiellaAnlaggningstillgangar
SummaAnlaggningstillgangarName=['Summa anläggningstillgångar']

Varulager=['Varulager m m']
KortfristigaFordringar=['Kortfristiga Fordringar']
KortfristigaPlaceringar=['Kortfristiga Placeringar']
KassaOchBank=['Kassa och bank']
SummaOmsattningstillgangar=Varulager+KortfristigaFordringar+KortfristigaPlaceringar \
+KassaOchBank
SummaOmsattningstillgangarName=['Summa omsättningstillgångar']

SummaTillgangar=SummaAnlaggningstillgangar+SummaOmsattningstillgangar
SummaTillgangarName=['Summa Tillgångar']

Egetkapital=['Eget kapital']
ObeskattadeReserver=['Obeskattade reserver']
Avsattningar=['Avsättningar']
LangfristigaSkulder=['Långfristiga skulder']
KortfristigaSkulder=['Kortfristiga skulder']
SummaEgetKapitalAvsattningarSkulder=Egetkapital+ObeskattadeReserver+Avsattningar \
+LangfristigaSkulder+KortfristigaSkulder

SummaEgetKapitalAvsattningarSkulderName=['Summa Eget Kapital, Avsättningar och Skulder']

BeraknatResultatBalans=SummaTillgangar+SummaEgetKapitalAvsattningarSkulder

BeraknatResultatBalansName=['Beräknat resultat Balans']
'''============================Resultatrakning=============================='''

Nettoomsattning=['Nettoomsättning']
OvrigaRorelseintakter=['Övriga rörelseintäkter']
SummaRorelseintakter=Nettoomsattning+OvrigaRorelseintakter
SummaRorelseintakterName=['Summa rörelseintäkter']

Rorelsekostnader=['Rörelsekostnader']

Bruttovinst=SummaRorelseintakter+Rorelsekostnader
BruttovinstName=['Bruttovinst']

OvrigaExternaRorelsekostnader=['Övriga externa rörelsekostnader']
Personalkostnader=['Personalkostnader']
Nedskrivningar=['Nedskrivningar']
Avskrivningar=['Avskrivningar']
OvrigaRorelsekostnader=['Övriga rörelsekostnader']

Rorelseresultat=Bruttovinst+OvrigaExternaRorelsekostnader+Personalkostnader\
+Nedskrivningar+Avskrivningar+OvrigaRorelsekostnader
RorelseresultatName=['Rörelseresultat']

FinansiellaPoster=['Finansiella poster']

ResultatEfterFinansiellaPoster=Rorelseresultat+FinansiellaPoster
ResultatEfterFinansiellaPosterName=['Resultat efter finansiella poster']

Bokslutsdispositioner=['Bokslutsdispositioner']
Skatt=['Skatt']

SummaRorelsekostnader=Rorelsekostnader+OvrigaExternaRorelsekostnader\
+Personalkostnader+Nedskrivningar+Avskrivningar+OvrigaRorelsekostnader\
+FinansiellaPoster+Bokslutsdispositioner+Skatt
SummaRorelsekostnaderName=['Summa Rörelsekostnader']

Resultat=['Resultat']

BeraknatResultat=SummaRorelsekostnader+SummaRorelseintakter
BeraknatResultatName=['Beräknat resultat']
'''========================================================================'''
'''============================Read infile=============================='''


infiles_xlsx=['vernr2017','vernr2018'] #verrad vernr2018

To_date=['Bokföringsdatum','Registreringsdatum']
int_float=['Konto','Debet','Kredit']

table='rawIndata'
pd.io.sql.execute('DROP table IF EXISTS '+table, sqlite_db)










    



for infile in infiles_xlsx:
    df=pd.read_excel(Main_Path+'\\Indata\\'+str(infile)+'.xlsx')
    df['MainPath']=Main_Path
    df['Last_Path_Name']=Last_path_name
    df['Filename']=infile
    df.columns = df.columns.str.title()
    df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns})# Remove spaces from columns
    df[int_float] = df[int_float].apply(pd.to_numeric, errors='coerce')
    df.update(df[int_float].fillna(0))
    df[To_date] = df[To_date].values.astype("datetime64[s]")
    df.rename(columns={'Bokföringsdatum':'Datum'},inplace=True)
    df["DateKey"] = df.Datum.dt.strftime('%Y-%m-01')
    df["YearMonthDay"] = (df.Datum.dt.strftime('%Y%m%d')).astype(int)
    df['BalansResultat']='Resultaträkning'
    df.loc[df.Konto.isin(balansrakning), 'BalansResultat'] = 'Balansräkning'
    df['BalansResultatSort']=2
    df.loc[df.Konto.between(1000, 2999) , 'BalansResultatSort'] =1
    df['Belopp']=-1*df.Debet+df.Kredit
    df['x'] =  [ random.randint(0,10)  for k in df.index]
    df['Budget_0_10']=((df.x/100)+1 )* df.Belopp
    df.loc[df.Konto.between(1000, 1099) , 'Kontogrupp1'] = Immateriella
    df.loc[df.Konto.between(1100, 1299) , 'Kontogrupp1'] = Materiella
    df.loc[df.Konto.between(1300, 1399) , 'Kontogrupp1'] = FinansiellaAnlaggningstillgangar
    df.loc[df.Konto.between(1400, 1499) , 'Kontogrupp1'] = Varulager
    df.loc[df.Konto.between(1500, 1799) , 'Kontogrupp1'] = KortfristigaFordringar
    df.loc[df.Konto.between(1800, 1899) , 'Kontogrupp1'] = KortfristigaPlaceringar
    df.loc[df.Konto.between(1900, 1999) , 'Kontogrupp1'] = KassaOchBank
    df.loc[df.Konto.between(2000, 2099) , 'Kontogrupp1'] = Egetkapital
    df.loc[df.Konto.between(2100, 2199) , 'Kontogrupp1'] = ObeskattadeReserver
    df.loc[df.Konto.between(2200, 2299) , 'Kontogrupp1'] = Avsattningar
    df.loc[df.Konto.between(2300, 2399) , 'Kontogrupp1'] = LangfristigaSkulder
    df.loc[df.Konto.between(2400, 2999) , 'Kontogrupp1'] = KortfristigaSkulder
    df.loc[df.Konto.between(3000, 3799) , 'Kontogrupp1'] = Nettoomsattning
    df.loc[df.Konto.between(3800, 3999) , 'Kontogrupp1'] = OvrigaRorelseintakter
    df.loc[df.Konto.between(4000, 4999) , 'Kontogrupp1'] = Rorelsekostnader
    df.loc[df.Konto.between(5000, 6999) , 'Kontogrupp1'] = OvrigaExternaRorelsekostnader
    df.loc[df.Konto.between(7000, 7699) , 'Kontogrupp1'] = Personalkostnader
    df.loc[df.Konto.between(7700, 7799) , 'Kontogrupp1'] = Nedskrivningar
    df.loc[df.Konto.between(7800, 7899) , 'Kontogrupp1'] = Avskrivningar
    df.loc[df.Konto.between(7900, 7999) , 'Kontogrupp1'] = OvrigaRorelsekostnader
    df.loc[df.Konto.between(8000, 8999) , 'Kontogrupp1'] = FinansiellaPoster
    df.loc[df.Konto.between(8500, 8799) , 'Kontogrupp1'] = 'Fri kontogrupp'
    df.loc[df.Konto.between(8800, 8899) , 'Kontogrupp1'] = Bokslutsdispositioner
    df.loc[df.Konto.between(8900, 8989) , 'Kontogrupp1'] = Skatt
    df.loc[df.Konto.between(8990, 8999) , 'Kontogrupp1'] = Resultat
    df['DebetKredit']=1
    df.loc[(df.Konto.between(2000,2999) | df.Konto.between(4000,8499) ) | 
            (df.Konto.between(8800,8989)),"DebetKredit"]=-1  #-0.1
    '''==================balansrakning======================================'''
    df.loc[df.Kontogrupp1.isin(SummaAnlaggningstillgangar), 'Kontogrupp2'] = SummaAnlaggningstillgangarName
    df.loc[df.Kontogrupp1.isin(SummaOmsattningstillgangar), 'Kontogrupp2'] = SummaOmsattningstillgangarName
    df.loc[df.Kontogrupp1.isin(SummaTillgangar), 'Kontogrupp3'] = SummaTillgangarName
    '''====================================================================='''
    df.loc[df.Kontogrupp1.isin(SummaEgetKapitalAvsattningarSkulder), 'Kontogrupp2'] =SummaEgetKapitalAvsattningarSkulderName
    df.loc[df.Kontogrupp1.isin(BeraknatResultatBalans) , 'Kontogrupp4'] =BeraknatResultatBalansName
    '''====================================================================='''
    '''=================Resultrakning======================================='''
    df.loc[df.Kontogrupp1.isin(SummaRorelseintakter), 'Kontogrupp2'] = SummaRorelseintakterName
    df.loc[df.Kontogrupp1.isin(Bruttovinst), 'Kontogrupp3'] = BruttovinstName
    df.loc[df.Kontogrupp1.isin(Rorelseresultat), 'Kontogrupp4'] = RorelseresultatName
    
    df.loc[df.Kontogrupp1.isin(ResultatEfterFinansiellaPoster), 'Kontogrupp5'] = ResultatEfterFinansiellaPosterName
    
    df.loc[df.Kontogrupp1.isin(SummaRorelsekostnader), 'Kontogrupp6'] = SummaRorelsekostnaderName
    df.loc[df.Kontogrupp1.isin(BeraknatResultat), 'Kontogrupp7'] = BeraknatResultatName
    

    
    df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') #replace append




Kontogrps=['Kontogrupp1','Kontogrupp2','Kontogrupp3','Kontogrupp4','Kontogrupp5',
           'Kontogrupp6','Kontogrupp7']

table='StageIndata'
pd.io.sql.execute('DROP table IF EXISTS '+table, sqlite_db)
for Kontogrp in Kontogrps:
    read_sql_query='''
SELECT 
    ri.Kontogrupp1,
      ri.Konto,
      ri.DebetKredit,
      ri.DateKey,
      ri.%s AS KontoGrp,
      ri.BalansResultat,
      ri.BalansResultatSort,
      Sum(ri.Belopp) AS Utfall1,
      Sum(ri.Budget_0_10) AS Budget1
FROM
    rawIndata ri where %s>' '
GROUP BY
ri.Kontogrupp1,
        ri.Konto,
        ri.DebetKredit,
        ri.DateKey,
        ri.%s,
        ri.BalansResultat,
        ri.BalansResultatSort
'''%(Kontogrp,Kontogrp,Kontogrp)
    for df in pd.read_sql_query(read_sql_query, sqlite_db,chunksize=chunksize):
        df.loc[df['KontoGrp'].isin(Immateriella), 'KontoGrpKey'] = 1
        df.loc[df['KontoGrp'].isin(Materiella), 'KontoGrpKey'] = 2
        df.loc[df['KontoGrp'].isin(FinansiellaAnlaggningstillgangar),  'KontoGrpKey'] = 3
        df.loc[df['KontoGrp'].isin( SummaAnlaggningstillgangarName), 'KontoGrpKey'] = 4
        df.loc[df['KontoGrp'].isin(Varulager), 'KontoGrpKey'] = 5
        df.loc[df['KontoGrp'].isin(KortfristigaFordringar), 'KontoGrpKey'] = 6
        df.loc[df['KontoGrp'].isin(KortfristigaPlaceringar), 'KontoGrpKey'] = 7
        df.loc[df['KontoGrp'].isin(KassaOchBank), 'KontoGrpKey'] = 8
        df.loc[df['KontoGrp'].isin(SummaOmsattningstillgangarName), 'KontoGrpKey'] = 9
        df.loc[df['KontoGrp'].isin(SummaTillgangarName), 'KontoGrpKey'] =10
        df.loc[df['KontoGrp'].isin(Egetkapital), 'KontoGrpKey'] =11
        df.loc[df['KontoGrp'].isin(ObeskattadeReserver), 'KontoGrpKey'] =12
        df.loc[df['KontoGrp'].isin(Avsattningar), 'KontoGrpKey'] =13
        df.loc[df['KontoGrp'].isin(LangfristigaSkulder), 'KontoGrpKey'] =14
        df.loc[df['KontoGrp'].isin(KortfristigaSkulder), 'KontoGrpKey'] =15
        df.loc[df['KontoGrp'].isin(SummaEgetKapitalAvsattningarSkulderName), 'KontoGrpKey'] =16
        df.loc[df['KontoGrp'].isin(BeraknatResultatBalansName), 'KontoGrpKey'] =17
        df.loc[df['KontoGrp'].isin(Nettoomsattning), 'KontoGrpKey'] =18
        df.loc[df['KontoGrp'].isin(OvrigaRorelseintakter), 'KontoGrpKey'] =19
        df.loc[df['KontoGrp'].isin(SummaRorelseintakterName), 'KontoGrpKey'] =20
        df.loc[df['KontoGrp'].isin(Rorelsekostnader), 'KontoGrpKey'] =21
        df.loc[df['KontoGrp'].isin(BruttovinstName), 'KontoGrpKey'] =22
        df.loc[df['KontoGrp'].isin(OvrigaExternaRorelsekostnader), 'KontoGrpKey'] =23
        df.loc[df['KontoGrp'].isin(Personalkostnader), 'KontoGrpKey'] =24
        df.loc[df['KontoGrp'].isin(Nedskrivningar), 'KontoGrpKey'] =25
        df.loc[df['KontoGrp'].isin(Avskrivningar), 'KontoGrpKey'] =26
        df.loc[df['KontoGrp'].isin(OvrigaRorelsekostnader), 'KontoGrpKey'] =27
        df.loc[df['KontoGrp'].isin(RorelseresultatName), 'KontoGrpKey'] =28
        df.loc[df['KontoGrp'].isin(FinansiellaPoster), 'KontoGrpKey'] =29
        df.loc[df['KontoGrp'].isin(ResultatEfterFinansiellaPosterName), 'KontoGrpKey'] =30
        df.loc[df['KontoGrp'].isin(Bokslutsdispositioner), 'KontoGrpKey'] =31
        df.loc[df['KontoGrp'].isin(Skatt), 'KontoGrpKey'] =32
        df.loc[df['KontoGrp'].isin(SummaRorelsekostnaderName), 'KontoGrpKey'] =33
        df.loc[df['KontoGrp'].isin(Resultat), 'KontoGrpKey'] =34
        df.loc[df['KontoGrp'].isin(BeraknatResultatName), 'KontoGrpKey'] =35
        df.rename(columns={'KontoGrp':'RedovisningRubrik','KontoGrpKey':'RedovisningRubrikKey'},inplace=True)
        df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') #replace append
 
  

'''=========================FactTable================================'''




table='FactVerifikationer'

read_sql_query='''
SELECT distinct datekey FROM StageIndata
'''

df = pd.read_sql_query(read_sql_query, sqlite_db)


periods=list(df.DateKey)


pd.io.sql.execute('DROP table IF EXISTS '+table, sqlite_db)



for period in periods:
    read_sql_query='''

SELECT DISTINCT
       si.DateKey,
       si.Konto,
       si.DebetKredit,
       si.Utfall1 as UtfallSEK,
       si.Budget1  as BudgetSEK
FROM
    StageIndata si  
    where si.datekey="%s"
    
    
  '''%(period)    
    for df in pd.read_sql_query(read_sql_query, sqlite_db,chunksize=chunksize):    
#        df.drop(['DebetKredit','Utfall1','Budget1'], axis=1,inplace=True)
        df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') #replace append




'''===============================FactNyckelTal=========================='''

table='FactNyckelTal'

read_sql_query='''

SELECT
      si.DateKey,
      si.RedovisningRubrik,
      Sum(si.Utfall1) AS UtfallSEK
FROM
    StageIndata si 
GROUP BY
        si.DateKey,
        si.RedovisningRubrik

'''




df =pd.read_sql_query(read_sql_query, sqlite_db)
df['RedovisningRubrik']=df['RedovisningRubrik'].str.replace("(","")
df['RedovisningRubrik']=df['RedovisningRubrik'].str.replace(")","")
df.RedovisningRubrik= df.RedovisningRubrik.str.title()
df['RedovisningRubrik'] = df['RedovisningRubrik'].str.replace(r'[^\x00-\x7f,]', '_')
df['RedovisningRubrik'] = df['RedovisningRubrik'].replace({',': ''}, regex=True)

df = df.pivot(index='DateKey', columns='RedovisningRubrik')
df['DateKey']=df.index
df.columns = ["_".join(x) for x in df.columns.ravel()]
df.columns = [col.replace('UtfallSEK_','').title() for col in df.columns]

df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns})# Remove spaces from columns
df.rename(columns={'Datekey_':'DateKey'},inplace=True)
ReplaceNullZero=list(df.columns)
ReplaceNullZero.remove('DateKey')
df.update(df[ReplaceNullZero].fillna(0))

df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='replace') #replace append

'''==========================DimTables==============================================='''
'''**************************kontoplanbas**********************************'''
table='DimKontoplan'
read_sql_query='''
SELECT DISTINCT
       si.Konto,
       si.RedovisningRubrikKey
FROM
    StageIndata si
'''

pd.io.sql.execute('DROP table IF EXISTS '+table, sqlite_db)
for df in pd.read_sql_query(read_sql_query, sqlite_db,chunksize=chunksize):
    df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') #replace append





table='DimRedovisningRubrik' 
read_sql_query='''

SELECT distinct
      si.RedovisningRubrik, 
      si.RedovisningRubrikKey , 
      si.BalansResultat,
      si.BalansResultatSort
FROM
    StageIndata si 
'''

pd.io.sql.execute('DROP table IF EXISTS '+table, sqlite_db)
for df in pd.read_sql_query(read_sql_query, sqlite_db,chunksize=chunksize):
    df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') #replace append




read_sql_query='''


with q1 as (
SELECT
      Min(a.datum) AS mindate,
      max(a.datum) AS maxdate
FROM
    rawIndata a) SELECT * from q1 

'''

df =pd.read_sql_query(read_sql_query, sqlite_db)

vars_to_date=['mindate','maxdate']

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
df["DateKey"] = df.Date.dt.strftime('%Y-%m-%d')
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
df["MonthOfYearNumber"] = (df.Date.dt.strftime('%m')).astype(int)
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
df.drop(['one','date1'], axis=1,inplace=True)

dDate='DimDate'
df.to_sql(name=str(dDate),con=sqlite_db,index=False,if_exists='replace')  






#KPI
ColumnLabelsNames=['UtfallPeriod_TY','BudgetPeriod_TY','AvvikelseUBPeriod_TY',
                   'UtfallAck_TY','BudgetAck_TY','AvvikelseUB_TY',
                   'UtfallUppräknat_TY','BudgetHelår_TY','AvvikelseUppräknat_TY',
                   'UtfallPeriod_LY','BudgetPeriod_LY','AvvikelseUBPeriod_LY',
                   'UtfallAck_LY','BudgetAck_LY','AvvikelseUB_LY',
                   'UtfallUppräknat_LY','BudgetHelår_LY','AvvikelseUppräknat_LY']
df=pd.DataFrame({'ColumnLabels':ColumnLabelsNames})
df['SortColumnNames']=df.index+1
df.to_sql(name='ColumnLabels',con=sqlite_db,index=False,if_exists='replace') 






for df in pd.read_sql_query('select * from DimKontoplan', sqlite_db,chunksize=chunksize):
    df.to_sql(name='RowLabels',con=sqlite_db,index=False,if_exists='replace') #replace append



#costumer

df=pd.DataFrame({'Updated':[datetime.datetime.now().strftime("%Y-%m-%d %H:%M")]})
df['Kund']=Kund
df.to_sql(name='DimDateCreatedKund',con=sqlite_db,index=False,if_exists='replace') 

DaxMeasures=['CoreMeasures','AbsoluteMesures']


for DaxMeasure in DaxMeasures:
    df=pd.DataFrame({'Col':[1]})
    df.to_sql(name=str(DaxMeasure),con=sqlite_db,index=False,if_exists='replace') 


'''************************************************************************'''

'''*****************************migrate to database***************************'''


'''************************Help tables***************************************'''

lista='ColumnLabels RowLabels  DimDateCreatedKund  \
CoreMeasures AbsoluteMesures'

tables = [item for item in lista.split(' ') if  item not in  ['']] #exclude empty

for table in tables:
    sql_query= """  select * from %s"""%(table)
    pd.io.sql.execute('DROP table IF EXISTS '+str(table), migrate_to_databse)
    for df in pd.read_sql_query(sql_query, sqlite_db,chunksize=chunksize): #get data from SQLITE
        start_time = time.time()
        count_row = df.shape[0]  # gives number of row count
        print('reading ',str(table))
        print(count_row,' rows from sqlite_db, StartTime ',time.strftime("%H:%M:%S"))
        df.to_sql(name=str(table),con=migrate_to_databse,index=False,if_exists='append') #migrate to another database
        print(str(table),'is excecuted to the new Database! DurationTime ',time.strftime("%H:%M:%S", time.gmtime(time.time() - start_time)))

'''===================DimDate=============================================='''

table='DimDate'
print('Executing '+str(table))

df=pd.read_sql_query('select * from %s limit 1'%(table), sqlite_db)

dates=['Date','DateKey','DayDate','FirstDateOfYear','LastDateOfYear',
       'FirstDateOfQuarter','LastDateOfQuarter','FirstDateOfMonth',
       'LastDateOfMonth','FirstDateOfWeek','LastDateOfWeek','PreviousDateYear',
       'PreviousDateMonth','PreviousDateMonthStart','PreviousDateMonthEnd',
       'PreviousDateWeek','PreviousDateDay','NextDateDay']

numeric=['YearMonthDay','YearMonth','YearCode','QuarterCode','QuaterNumber',
         'FirstMonthOfQuarter','LastMonthOfQuarter','MonthCode','MonthOfYearNumber',
         'NumberOfDaysInMonth','WeekCode','WeekOfYearNumber','DayOfYear',
         'DayOfQuarter','DayOfMonth','DayOfWeek','months_between']


df=pd.DataFrame({'Cols':list(df.columns)})

df['Datatype']=df.Cols+' NVARCHAR(max),'
df.loc[df.Cols.isin(dates), 'Datatype'] = df.Cols+' DATE,'
df.loc[df.Cols.isin(numeric), 'Datatype'] = df.Cols+' float(53),'
df['ID'] = 1
df['IDx'] = range(1, 1+len(df) )
df['IDMax'] = df.groupby(['ID'])['IDx'].transform(lambda x : x.max())
#df.loc[df.IDx==df.IDMax, 'Datatype'] = df.Cols+' NVARCHAR(max)'



ColsToDatabase="".join([str(s) for s in list(list(df.Datatype))])


create_table='''
CREATE TABLE %s(
%s
)
'''%(table,str(ColsToDatabase))
pd.io.sql.execute('DROP table IF EXISTS '+str(table), migrate_to_databse)
pd.io.sql.execute(create_table, migrate_to_databse)

for df in pd.read_sql_query('select * from %s '%(table), sqlite_db,chunksize=chunksize): #get data from SQLITE
    df.to_sql(name=str(table),con=migrate_to_databse,index=False,if_exists='append')   

print(str(table),'is excecuted to the new Database!')
'''===================DimKontoplan=========================================='''

table='DimKontoplan'
print('Executing '+str(table))

df=pd.read_sql_query('select * from %s limit 1'%(table), sqlite_db)


numeric=['Konto','RedovisningRubrikKey']


df=pd.DataFrame({'Cols':list(df.columns)})

df['Datatype']=df.Cols+' float(53),'


ColsToDatabase="".join([str(s) for s in list(list(df.Datatype))])


create_table='''
CREATE TABLE %s(
%s
)
'''%(table,str(ColsToDatabase))
pd.io.sql.execute('DROP table IF EXISTS '+str(table), migrate_to_databse)
pd.io.sql.execute(create_table, migrate_to_databse)

for df in pd.read_sql_query('select * from %s '%(table), sqlite_db,chunksize=chunksize): #get data from SQLITE
    df.to_sql(name=str(table),con=migrate_to_databse,index=False,if_exists='append') 
    
    
print(str(table),'is excecuted to the new Database!')    
'''===================DimRedovisningsrubrik=========================================='''

table='DimRedovisningRubrik'
print('Executing '+str(table))

df=pd.read_sql_query('select * from %s limit 1'%(table), sqlite_db)


numeric=['BalansResultatSort','RedovisningRubrikKey']


df=pd.DataFrame({'Cols':list(df.columns)})
df['Datatype']=df.Cols+' NVARCHAR(max),'
df.loc[df.Cols.isin(numeric), 'Datatype'] = df.Cols+' float(53),'




ColsToDatabase="".join([str(s) for s in list(list(df.Datatype))])


create_table='''
CREATE TABLE %s(
%s
)
'''%(table,str(ColsToDatabase))
pd.io.sql.execute('DROP table IF EXISTS '+str(table), migrate_to_databse)
pd.io.sql.execute(create_table, migrate_to_databse)

for df in pd.read_sql_query('select * from %s '%(table), sqlite_db,chunksize=chunksize): #get data from SQLITE
    df.to_sql(name=str(table),con=migrate_to_databse,index=False,if_exists='append') 

print(str(table),'is excecuted to the new Database!')

'''===================FactVerifikationer=========================================='''

table='FactVerifikationer'
print('Executing '+str(table))

df=pd.read_sql_query('select * from %s limit 1'%(table), sqlite_db)

dates=['DateKey']


numeric=['Konto','UtfallSEK','BudgetSEK']


df=pd.DataFrame({'Cols':list(df.columns)})
df['Datatype']=df.Cols+' float(53),'
df.loc[df.Cols.isin(dates), 'Datatype'] = df.Cols+' DATE,'



ColsToDatabase="".join([str(s) for s in list(list(df.Datatype))])


create_table='''
CREATE TABLE %s(
%s
)
'''%(table,str(ColsToDatabase))
pd.io.sql.execute('DROP table IF EXISTS '+str(table), migrate_to_databse)
pd.io.sql.execute(create_table, migrate_to_databse)

for df in pd.read_sql_query('select * from %s '%(table), sqlite_db,chunksize=chunksize): #get data from SQLITE
    df.to_sql(name=str(table),con=migrate_to_databse,index=False,if_exists='append')  

print(str(table),'is excecuted to the new Database!')



'''===================FactNyckelTal=========================================='''

table='FactNyckelTal'
print('Executing '+str(table))

df=pd.read_sql_query('select * from %s limit 1'%(table), sqlite_db)

dates=['DateKey']

df=pd.DataFrame({'Cols':list(df.columns)})
df['Datatype']=df.Cols+' float(53),'
df.loc[df.Cols.isin(dates), 'Datatype'] = df.Cols+' DATE,'



ColsToDatabase="".join([str(s) for s in list(list(df.Datatype))])


create_table='''
CREATE TABLE %s(
%s
)
'''%(table,str(ColsToDatabase))
pd.io.sql.execute('DROP table IF EXISTS '+str(table), migrate_to_databse)
pd.io.sql.execute(create_table, migrate_to_databse)

for df in pd.read_sql_query('select * from %s '%(table), sqlite_db,chunksize=chunksize): #get data from SQLITE
    df.to_sql(name=str(table),con=migrate_to_databse,index=False,if_exists='append')  

print(str(table),'is excecuted to the new Database!')
'''======================end Program================================================='''
sqlite_db.close()


print('Script successfully completed')


#count_col = df.shape[1]  # gives number of col count

#
#
#df = pd.read_sql_query('select distinct Konto from v1', sqlite_db)
#
#kontonrs=list(df.Konto)
#
#read_sql_query='''
#
#
#select min(datekey) as mindate, max(datekey) as maxdate from v1
#
#'''
#
#df =pd.read_sql_query(read_sql_query, sqlite_db)
#
#Min_date="[]".join(list(df.mindate))
#Max_date="[]".join(list(df.maxdate))
# 
#df = pd.DataFrame(index = pd.date_range(Min_date, Max_date, freq='M'))
# 
# 
#for kontonr in kontonrs:
#    df['Konto']=kontonr
#    df['DateKey']=df.index
#    df.to_sql(name='v2',con=sqlite_db,index=False,if_exists='append')
# 
#
#read_sql_query='''
#
#select v2.Datekey,v2.Konto, v1.RedovisningRubrikKey,v1.UtfallSEK,v1.BudgetSEK
#from v2 left join v1 on v2.Datekey=v1.Datekey and v2.konto=v1.konto
#'''
#
#
#df =pd.read_sql_query(read_sql_query, sqlite_db)
#
#
#df.update(df['UtfallSEK','BudgetSEK'].fillna(0))
#
#df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append')
#
#pd.io.sql.execute('DROP table IF EXISTS v1', sqlite_db)
#pd.io.sql.execute('DROP table IF EXISTS v2', sqlite_db)