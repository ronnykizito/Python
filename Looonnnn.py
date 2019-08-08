# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 10:11:00 2019

@author: F2531197
"""



import pandas as pd
import os
import time
import datetime

import sqlite3


'''************************************************************'''


Kund="Loonnn"

Today=time.strftime("%Y-%m-%d")
#chunksize for handle bigdata and therefore handle the memory issue
chunksize=1000*1000




'''**************************create folders**********************************'''

Main_Path=r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\\'+Kund

lista='indata datamining  data   database '

folders = [item for item in lista.title().split(' ') if  item not in  ['']] #exclude empty



for folder_name in folders:
    newpath = Main_Path+'\\'+folder_name
    if not os.path.exists(newpath):
        os.makedirs(newpath)
        

#get the main folder name
Last_path_name=os.path.basename(os.path.dirname(Main_Path+'\\Indata\\'))







'''setup a database for SQLITE3. The execution plan for SQLIT3 is much faster then other database'''



sqlite_db = sqlite3.connect(Main_Path+'\\Database\\'+Kund+'.db')
c = sqlite_db.cursor()



Kund_logo={'Kund_logo':['http://www.dinlon.se/wp-content/themes/din-lon/img/logo%20-%20din%20lon%20-%20blue.svg']}
Kund_logo = pd.DataFrame(Kund_logo)

BI_Logo={'BI_Logo':['http://bianalyzr.se/images/templates/palett/logo.png']}
BI_Logo = pd.DataFrame(BI_Logo)


'''====================================================================='''







# get all files in the folder with ext 
files_names = os.listdir(Main_Path+'\\Indata\\')
File_name = [i for i in files_names if i.endswith('.xlsm')] #txt csv xlsx xlsm osv.
#get the main folder name
Last_path_name=os.path.basename(os.path.dirname(Main_Path+'\\Indata\\'))



'''=============================create a tbale with needed info============='''
table='rawIndata'






''' read the max range in the exelfile====================================='''
usecols = [i for i in range(0,13)] 
Addusecols=['Filename','Sheetname','titles1','Ar']

usecols=usecols+Addusecols

df=pd.DataFrame({'usecols':usecols})
df['Quote']="'"
df['Datatype']="'"+(df.usecols).astype(str)+"'"+' text,'
df['ID'] = 1
df['IDx'] = range(1, 1+len(df) )
df['IDMax'] = df.groupby(['ID'])['IDx'].transform(lambda x : x.max())
df['RemoveComma']=df.Quote+(df.usecols).astype(str)+df.Quote+' text'
df.loc[df.IDx==df.IDMax, 'Datatype'] = df.RemoveComma



ColsToDatabase="".join([str(s) for s in list(list(df.Datatype))])

create_table='''
CREATE TABLE %s  (
%s
)
'''%(table,str(ColsToDatabase))

pd.io.sql.execute('DROP table IF EXISTS '+str(table), sqlite_db)
pd.io.sql.execute(create_table, sqlite_db)

xlfname=r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\Loonnn\Indata\1. Acne Studios fakturamall 2018.xlsm'






df = pd.concat(pd.read_excel(xlfname, sheet_name=None, header=None,usecols=usecols,dtype='str',nrows=150))

xlsx_files=[r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\Loonnn\Indata\1. Acne Studios fakturamall 2018.xlsm',
           r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\Loonnn\Indata\46. Sweden Rent A Car (AVIS) fakturamall 2018.xlsm']


outputFile=r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\Loonnn\Indata\t.csv'
import os
os.remove(outputFile)
for file in xlsx_files:
    df = pd.concat(pd.read_excel(file, sheet_name=None, header=None,
                                 usecols=usecols,dtype='str',nrows=150))
    df['file']=file
    df['Sheet']=df.index
    df.to_csv(outputFile,index=False, mode='a',sep=';',encoding = 'ISO-8859-1') 



  

encoding='ISO-8859-1'
separator=';'
chunksize=1000*100
for df in pd.read_csv(outputFile,sep=separator,encoding = encoding, low_memory=False,iterator=True,chunksize=chunksize):
    df['sheet'] = df['ff'].str.replace('(', '').str.replace("'", '')
    df = df.join(df['x'].str.split(',', expand=True).add_prefix('col'))
    
for the_files in File_name:
    xlfname = Main_Path+'\\Indata'+'\\'+the_files
    xl = pd.ExcelFile(xlfname)
    for sheet in [x for x in xl.sheet_names if x not in  [' ']] :
            df = pd.read_excel(xlfname,sheet_name=str(sheet),nrows=150,header=None,
            usecols=usecols)
            df['Filename']=the_files
            df['Sheetname']=sheet
            df['titles1'] = df['Filename'].str.split('.xlsm', 1).str[0].str.strip()
            df['Ar'] = df['titles1'].str.split('fakturamall', 1).str[1].str.strip()
            df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') #replace append





month=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
int_float=['Pris','Antal','Summa','Nummer']
read_sql_query='''
with q1 as (SELECT 
      ri."8" as Artikel,
      ri."9" as Pris,
      ri."10" as Antal,
      ri."11" as Summa,
      ri."12" as Nummer,
      ri.Filename,
      ri.Sheetname,
      ri.Ar
FROM
    %s ri) select * from q1 
'''%(table)


table='StageIndata'
pd.io.sql.execute('DROP table IF EXISTS '+str(table), sqlite_db)

lista='Artikel Pris Antal Summa Nummer Filename Sheetname'

UniqueRows = [item for item in lista.title().split(' ') if  item not in  ['']] #exclude empty


for df in pd.read_sql_query(read_sql_query, sqlite_db,chunksize=chunksize):
    df=df.query('(Artikel !="Artikel" )')
    df['man'] = df.Sheetname.str[0:3]
    df.man= df.man.str.title()
    df.loc[df['man'].isin(['Maj']), 'man'] = 'May'
    df.loc[df['man'].isin(['Okt']), 'man'] = 'Oct'  
    df.loc[df['man'].isin(month), 'test'] = 1
    df=df.query('(test ==1 )')
    df.drop(['test'], axis=1,inplace=True)
    df['Status1']=0
    df.loc[df.Pris.isin(['EJ KLAR']), 'Status1'] = 1
    df['IDMax'] = df.groupby(['Filename','man'])['Status1'].transform(lambda x : x.max())
    df['Status']='KLAR'
    df.loc[(df.IDMax ==1), 'Status'] = 'EJ KLAR'
    df[int_float] = df[int_float].apply(pd.to_numeric, errors='coerce')
    df=df.query('(Nummer >0 )') 
    df.loc[df.Artikel.isnull(), 'test'] =1
    df=df.query('(test !=1 )')
    df=df.rename(columns={cols: cols.replace(' ','') for cols in df.columns})# Remove spaces from columns
    df.columns = df.columns.str.title()
    df['Manad']=(pd.to_datetime(df.Man, format='%b').dt.month).astype(str).str.pad(width=2,fillchar='0')
    df['Datekey']=(df.Ar.astype(str)+'-'+df.Manad+'-01').values.astype("datetime64[s]")  
    df.update(df[int_float].fillna(0))
    df['IDMaxx'] = df.groupby(['Filename','Man'])['Summa'].transform(lambda x : x.max())
    df['CostumerStatus'] = 'Active'
    df.loc[df.IDMaxx==0, 'CostumerStatus'] = 'Not Active'
    df['CostumerID'] = (df['Filename'].str.split('.', 1).str[0].str.strip()).astype(int)
    df.drop_duplicates(UniqueRows,keep= 'last', inplace=True)
#    df=df.query('(Summa >0 )')
    df.drop(['Test','Man','Manad','Ar',
             'Sheetname','Status1','Idmax','IDMaxx'], axis=1,inplace=True)
    df.to_sql(name=str(table),con=sqlite_db,index=False,if_exists='append') #replace append



