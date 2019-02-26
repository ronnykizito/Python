

import pyodbc
import pandas as pd
import sqlite3
import time
import os
#from dateutil.relativedelta import relativedelta
from datetime import datetime
#import dateutil





Today=time.strftime("%Y-%m-%d")
start_time = time.time()

df=pd.DataFrame({'Datex':['0000-01-01']})
df['Date']=datetime.strptime(time.strftime("%Y-%m-%d"), '%Y-%m-%d')
df['WeekNo']= df.Date.dt.strftime('%W')
df['period_start'] = ((df.Date - pd.DateOffset(months=2)).dt.to_period("m").dt.start_time).dt.strftime('%Y%m01')
df['FirstDateOfMonth']= (df.Date.dt.to_period("m").dt.start_time).dt.strftime('%Y%m01')
df['EndPeriod'] = (df.Date + pd.DateOffset(months=2)).dt.to_period("m").dt.start_time
df['LastDateOfMonth_N']= (df.EndPeriod.dt.to_period("m").dt.end_time).dt.strftime('%Y%m%d')
df['period_startDate'] = ((df.Date - pd.DateOffset(months=2)).dt.to_period("m").dt.start_time).dt.strftime("%Y-%m-01")
df['period_EndDate'] = ((df.Date + pd.DateOffset(months=12)).dt.to_period("m").dt.start_time).dt.strftime("%Y-%m-01")
df['Period1'] = ((df.Date - pd.DateOffset(months=0)).dt.to_period("m").dt.start_time).dt.strftime('%Y%m')
df['Period2'] = ((df.Date + pd.DateOffset(months=1)).dt.to_period("m").dt.start_time).dt.strftime('%Y%m')
df['Period3'] = ((df.Date + pd.DateOffset(months=2)).dt.to_period("m").dt.start_time).dt.strftime('%Y%m')
df['x'] = ((df.Date - pd.DateOffset(months=0)).dt.to_period("m").dt.start_time)
df['Period1_excel'] = ((df.x - pd.DateOffset(months=0)).dt.to_period("m").dt.start_time).dt.strftime('%B %Y')
df['Period2_excel'] = ((df.x + pd.DateOffset(months=1)).dt.to_period("m").dt.start_time).dt.strftime('%B %Y')
df['Period3_excel'] = ((df.x + pd.DateOffset(months=2)).dt.to_period("m").dt.start_time).dt.strftime('%B %Y')







period_start="[]".join(list(df.period_start))
period_start_hTable="[]".join(list(df.FirstDateOfMonth))
period_end_hTable="[]".join(list(df.LastDateOfMonth_N))
period_startDate="[]".join(list(df.period_startDate)) 
period_EndDate="[]".join(list(df.period_EndDate))
Period1=list(df.Period1)
Period2="[]".join(list(df.Period2))
Period3="[]".join(list(df.Period3))
week="[]".join(list(df.WeekNo))
Period1_excel="[]".join(list(df.Period1_excel))
Period2_excel="[]".join(list(df.Period2_excel))
Period3_excel="[]".join(list(df.Period3_excel))


Period1.append(Period2)
Period1.append(Period3)

                         
chuck_sum=0
chunksize=1000*100
print('Program begins' )

#..........difine the costumer and the path for your report....................
costumer='BRA'
path=r'C:\Users\F2531197\Enfo Oyj\Robert Granfors - BRA flyg-Ronny'

lista='powerbi database outfiles'

folders = [item for item in lista.title().split(' ') if  item not in  ['']] #exclude empty


#create the folders for the rapport

for num, folder_name in enumerate(folders, start=1):
    newpath = path+'\\'+folder_name
    if not os.path.exists(newpath):
        os.makedirs(newpath)

#.................this is the connection to the costumer.......................
       
#connection in
server = ''
database = ''
username = ''
password = 'B'

#SQL Server 
SQLServer = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};' 
                              'Server=ALSSQLP012.mcn.malmoaviation.se;' 
                              'Database=bradw_dm;' 
                              'uid=DWH-reader;' 
                              'pwd=BM@vat1000')

SQLServer_cursor = SQLServer.cursor()


#create a database

SQLITE3 = sqlite3.connect(path+'\\Database\\'+costumer+'.db')      

SQLITE3_cursor = SQLITE3.cursor()



'''............................................................................
#..............................................................................
#....read the tables exactly how they were read in PWRBI....................'''

'''.............................read dim tables............................'''

lista='vdCitypair vdExchangeRate'

tables = [item for item in lista.split(' ') if  item not in  ['']]

for table in tables:
    sql_query= """  select * from pbiFB.%s"""%(table)
    pd.io.sql.execute('DROP table IF EXISTS '+table, SQLITE3)
    for df in pd.read_sql_query(sql_query, SQLServer,chunksize=chunksize): #get data from SQLITE
        df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append')


'''.............................read fact tables............................'''


table='vfFlights'
sql_query= """

SELECT
     *
FROM
    pbiFB.vfFlights vf
where DepartureDate_ID>=%s and AreaRegionCode>999
    
    """%(period_start)
pd.io.sql.execute('DROP TABLE IF EXISTS '+ table, SQLITE3)
for df in pd.read_sql_query(sql_query, SQLServer,chunksize=chunksize):
    df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append')


table='fFlights'
sql_query= """

SELECT
      vcp.CitypairBKEY,
      vcp.CitypairSortOrder,
      vcp.CitypairOriginCountryCode,
      vcp.CitypairDestinationCountryCode,
      vf.DepartureDate_ID,
      vf.SeatCountTotal
FROM
    vfFlights vf
    join vdCitypair vcp ON vcp.Citypair_ID = vf.Citypair_ID
    
    """

pd.io.sql.execute('DROP TABLE IF EXISTS '+ table, SQLITE3)
for df in pd.read_sql_query(sql_query, SQLITE3,chunksize=chunksize):
    df['Date'] =(df.DepartureDate_ID.astype(str).str[0:4]+'-'+
    df.DepartureDate_ID.astype(str).str[4:6]+'-'+
    df.DepartureDate_ID.astype(str).str[6:8]).values.astype("datetime64[s]")
    df['DomesticOrInternational']='International'
    df.loc[(df.CitypairOriginCountryCode == 'SE') & 
           (df.CitypairDestinationCountryCode =='SE'), 'DomesticOrInternational'] = 'Domestic'
    df['Today']=Today
    df['Today']=df.Today.astype("datetime64[s]")
    df['FirstDateOfWeek'] = ((df.Today.dt.to_period('W').apply(lambda r: r.start_time).dt.date)
    - pd.DateOffset(days=1)- pd.DateOffset(weeks=1))+pd.DateOffset(days=1)
#    df['LastDateOfWeek'] = (df.FirstDateOfWeek+ pd.DateOffset(days=7))-pd.DateOffset(days=1)
#    df['IsPreviousWeek']=0
#    df.loc[(df.Date >=df.FirstDateOfWeek) & (df.Date<=df.LastDateOfWeek), 'IsPreviousWeek'] = 1
    df["MonthNameYearEnglish"] = df.Date.dt.strftime('%B')+' '+ df.Date.dt.strftime('%Y').astype(str)
    df["DepartureMonthCode"] = df.Date.dt.strftime('%Y%m')
    df.rename(columns={'Date':'DepartureDate'},inplace=True)
    df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append')


    
''' ..........................................................'''

table='vfTicketCoupons'
sql_query= """

SELECT distinct *
FROM
    pbiFB.vfTicketCoupons vtc
WHERE
     (vtc.DepartureDate_ID >= %s) AND
     (vtc.IsIZRClass = 'False') AND
     (vtc.IsCrew = 'False')   
"""%(period_start)

pd.io.sql.execute('DROP TABLE IF EXISTS '+ table, SQLITE3)
for df in pd.read_sql_query(sql_query, SQLServer,chunksize=chunksize):
    df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append')

table='fTicketCoupons'
sql_query= """

SELECT 
       vtc.DepartureDate_ID,
       vtc.BookedDate_ID,
       vcp.CitypairBKEY,
       vcp.CitypairSortOrder,
       vcp.CitypairOriginCountryCode,
       vcp.CitypairDestinationCountryCode,
       ver.ExchangeRate,
       vtc.Amount
FROM
    vfTicketCoupons vtc
    INNER JOIN vdCitypair vcp ON vcp.Citypair_ID = vtc.Citypair_ID
    INNER JOIN vdExchangeRate ver ON ver.CurrencyBKEY = vtc.CurrencyBKEY
"""

pd.io.sql.execute('DROP TABLE IF EXISTS '+ table, SQLITE3)
for df in pd.read_sql_query(sql_query, SQLITE3,chunksize=chunksize):
    df['Date'] =(df.DepartureDate_ID.astype(str).str[0:4]+'-'+
    df.DepartureDate_ID.astype(str).str[4:6]+'-'+
    df.DepartureDate_ID.astype(str).str[6:8]).values.astype("datetime64[s]")
    df['DomesticOrInternational']='International'
    df.loc[(df.CitypairOriginCountryCode == 'SE') & 
           (df.CitypairDestinationCountryCode =='SE'), 'DomesticOrInternational'] = 'Domestic'
    df['Today']=Today
    df['Today']=df.Today.astype("datetime64[s]")
    df['FirstDateOfWeek'] = ((df.Today.dt.to_period('W').apply(lambda r: r.start_time).dt.date)
    - pd.DateOffset(days=1)- pd.DateOffset(weeks=1))+pd.DateOffset(days=1)
    df['LastDateOfWeek'] = (df.FirstDateOfWeek+ pd.DateOffset(days=7))-pd.DateOffset(days=1)
    df['BookedDate'] =(df.BookedDate_ID.astype(str).str[0:4]+'-'+
    df.BookedDate_ID.astype(str).str[4:6]+'-'+
    df.BookedDate_ID.astype(str).str[6:8]).values.astype("datetime64[s]")
    df['IsPreviousWeek']=0
    df.loc[(df.BookedDate >=df.FirstDateOfWeek) & (df.BookedDate<=df.LastDateOfWeek), 'IsPreviousWeek'] = 1
    df["MonthNameYearEnglish"] = df.Date.dt.strftime('%B')+' '+ df.Date.dt.strftime('%Y').astype(str)
    df["DepartureMonthCode"] = df.Date.dt.strftime('%Y%m')
    df['AmountLCY']=df.Amount
    df['AmountLCY'] = pd.to_numeric(df['AmountLCY'], errors='coerce')
    df['ExchangeRate'] = pd.to_numeric(df['ExchangeRate'], errors='coerce')
    df['AmountSEK']=df.AmountLCY*df.ExchangeRate
    df['Bdkpax']=1
    df['AmountBLW']=0
    df.loc[(df.IsPreviousWeek==1) , 'AmountBLW'] = df.AmountSEK
    df['BdkpaxBLW']=0
    df.loc[(df.IsPreviousWeek==1) , 'BdkpaxBLW'] = 1
    df.rename(columns={'Date':'DepartureDate'},inplace=True)
    df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append')
    

df = pd.DataFrame(index = pd.date_range(period_startDate, period_EndDate, freq='M'))
df['Date']=df.index
df["MonthNameYearEnglish"] = df.Date.dt.strftime('%B')+' '+ df.Date.dt.strftime('%Y').astype(str)
df["DepartureMonthCode"] = df.Date.dt.strftime('%Y%m')

periods=list(df.MonthNameYearEnglish)
periodsCodes=list(df.DepartureMonthCode)


table='hTable'
sql_query='''
WITH
    q1 AS (SELECT DISTINCT
                  ftc.CitypairBKEY
           FROM
               fTicketCoupons ftc where ftc.DepartureDate_ID between %s and %s),
    q2 AS (SELECT
                 cvp.CitypairBKEY,
                 cvp.CitypairSortOrder,
                 cvp.CitypairOriginCountryCode,
                 cvp.CitypairDestinationCountryCode
           FROM
               vdCitypair cvp)
SELECT
      q1.CitypairBKEY,
      q2.CitypairSortOrder,
      q2.CitypairOriginCountryCode,
      q2.CitypairDestinationCountryCode
FROM
    q2
    INNER JOIN q1 ON q1.CitypairBKEY = q2.CitypairBKEY
    
    '''%(period_start_hTable,period_end_hTable)

pd.io.sql.execute('DROP TABLE IF EXISTS '+ table, SQLITE3)
for period,periodsCode in zip(periods,periodsCodes):
    df = pd.read_sql_query(sql_query, SQLITE3)
    df['DomesticOrInternational']='International'
    df.loc[(df.CitypairOriginCountryCode == 'SE') & 
       (df.CitypairDestinationCountryCode =='SE'), 'DomesticOrInternational'] = 'Domestic'
    df['MonthNameYearEnglish']=period
    df["DepartureMonthCode"] = periodsCode  
    df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append')

table='master'
sql_query= """

WITH
    q1 AS (SELECT
      Sum(ftc.AmountSEK) AS BkdRev,
      Sum(ftc.Bdkpax) AS BkdPax,
      Sum(ftc.AmountBLW) AS BkdRevWeek,
      Sum(ftc.BdkpaxBLW) AS BkdPaxWeek,
      ht.CitypairBKEY,
      ht.CitypairSortOrder,
      ht.DomesticOrInternational,
      ht.MonthNameYearEnglish,
      ht.DepartureMonthCode
FROM
    hTable ht
    LEFT JOIN fTicketCoupons ftc ON (ht.CitypairBKEY = ftc.CitypairBKEY) AND
            (ht.DepartureMonthCode = ftc.DepartureMonthCode)
GROUP BY
        ht.CitypairBKEY,
        ht.CitypairSortOrder,
        ht.DomesticOrInternational,
        ht.MonthNameYearEnglish,
        ht.DepartureMonthCode),
    q2 AS (SELECT
                 ff.CitypairBKEY,
                 ff.DepartureMonthCode,
                 ff.MonthNameYearEnglish,
                 Sum(ff.SeatCountTotal) AS Capacity,
                 ff.DomesticOrInternational
           FROM
               fFlights ff
           GROUP BY
                   ff.CitypairBKEY,
                   ff.DepartureMonthCode,
                   ff.MonthNameYearEnglish,
                   ff.DomesticOrInternational),
    q3 AS (SELECT
                 q1.CitypairBKEY,
                 q1.DepartureMonthCode,
                 q1.MonthNameYearEnglish,
                 q1.BkdRev,
                 q2.Capacity,
                 q1.BkdPax,
                 q1.CitypairSortOrder,
                 q1.DomesticOrInternational,
                 q1.BkdRevWeek,
                 q1.BkdPaxWeek
           FROM
               q1
               left JOIN q2 ON (q2.CitypairBKEY = q1.CitypairBKEY) AND
                        (q2.DepartureMonthCode = q1.DepartureMonthCode) AND
                        (q2.MonthNameYearEnglish = q1.MonthNameYearEnglish) AND
                        (q2.DomesticOrInternational = q1.DomesticOrInternational))
SELECT
      q3.CitypairBKEY,
      q3.DomesticOrInternational,
      q3.CitypairSortOrder,
      q3.DepartureMonthCode,
      q3.MonthNameYearEnglish,
      q3.BkdRev,
      q3.Capacity,
      q3.BkdPax,
      q3.BkdRevWeek,
      q3.BkdPaxWeek
FROM
    q3
"""
pd.io.sql.execute('DROP TABLE IF EXISTS '+ table, SQLITE3)
for df in pd.read_sql_query(sql_query, SQLITE3,chunksize=chunksize):
    df['Loadfact']=df.BkdPax/df.Capacity
    df['BkdRev_Pax']=df.BkdRev/df.BkdPax
    df['BkdRev_PaxWeek']=df.BkdRevWeek/df.BkdPaxWeek
    df.to_sql(name=table,con=SQLITE3,index=False,if_exists='append')


''' ..................... to excel ............................'''

table='ToExcel'

for num, period in enumerate(Period1, start=1):
    sql_query='''
with q1 as (SELECT
      hp.CitypairBKEY AS CitypairBKEY,
      hp.DomesticOrInternational,
      hp.CitypairSortOrder AS CitypairSortOrder,
      hp.MonthNameYearEnglish,
      hp.DepartureMonthCode,
      Sum(m.BkdRev) AS BkdRev,
      Sum(m.Capacity) AS Capacity,
      Sum(m.BkdPax) AS BkdPax,
      Sum(m.BkdPax) / Sum(m.Capacity) AS Loadfact,
      Sum(m.BkdRev) / Sum(m.BkdPax) AS BkdRev_Pax,
      Sum(m.BkdRevWeek) AS BkdRevWeek,
      Sum(m.BkdPaxWeek) AS BkdPaxWeek,
      Sum(m.BkdRevWeek) / Sum(m.BkdPaxWeek) AS BkdRev_PaxWeek
FROM
    hTable hp
    LEFT JOIN master m ON (hp.CitypairBKEY = m.CitypairBKEY) AND
            (hp.MonthNameYearEnglish = m.MonthNameYearEnglish)
WHERE
     hp.DomesticOrInternational = 'Domestic'
GROUP BY
        hp.CitypairBKEY,
        hp.DomesticOrInternational,
        hp.CitypairSortOrder,
        hp.MonthNameYearEnglish,
        hp.DepartureMonthCode), q2 as 
        (SELECT
      'Domestic' AS CitypairBKEY,
      hp.DomesticOrInternational,
      100000 AS CitypairSortOrder,
      hp.MonthNameYearEnglish,
      hp.DepartureMonthCode,
      Sum(m.BkdRev) AS BkdRev,
      Sum(m.Capacity) AS Capacity,
      Sum(m.BkdPax) AS BkdPax,
      Sum(m.BkdPax)/Sum(m.Capacity) as Loadfact,
      Sum(m.BkdRev)/Sum(m.BkdPax) as BkdRev_Pax,
      Sum(m.BkdRevWeek) AS BkdRevWeek,
      Sum(m.BkdPaxWeek) AS BkdPaxWeek,
      Sum(m.BkdRevWeek)/Sum(m.BkdPaxWeek) as BkdRev_PaxWeek
FROM
    hTable hp
    LEFT JOIN master m ON (hp.CitypairBKEY = m.CitypairBKEY) AND
            (hp.MonthNameYearEnglish = m.MonthNameYearEnglish)
WHERE
     hp.DomesticOrInternational = 'Domestic'
GROUP BY
        hp.DomesticOrInternational,
        hp.MonthNameYearEnglish,
        hp.DepartureMonthCode), q3 as 
        (SELECT
      hp.CitypairBKEY AS CitypairBKEY,
      hp.DomesticOrInternational,
      hp.CitypairSortOrder AS CitypairSortOrder,
      hp.MonthNameYearEnglish,
      hp.DepartureMonthCode,
      Sum(m.BkdRev) AS BkdRev,
      Sum(m.Capacity) AS Capacity,
      Sum(m.BkdPax) AS BkdPax,
      Sum(m.BkdPax) / Sum(m.Capacity) AS Loadfact,
      Sum(m.BkdRev) / Sum(m.BkdPax) AS BkdRev_Pax,
      Sum(m.BkdRevWeek) AS BkdRevWeek,
      Sum(m.BkdPaxWeek) AS BkdPaxWeek,
      Sum(m.BkdRevWeek) / Sum(m.BkdPaxWeek) AS BkdRev_PaxWeek
FROM
    hTable hp
    LEFT JOIN master m ON (hp.CitypairBKEY = m.CitypairBKEY) AND
            (hp.MonthNameYearEnglish = m.MonthNameYearEnglish)
WHERE
     hp.DomesticOrInternational = 'International'
GROUP BY
        hp.CitypairBKEY,
        hp.DomesticOrInternational,
        hp.CitypairSortOrder,
        hp.MonthNameYearEnglish,
        hp.DepartureMonthCode), q4 as 
        (SELECT
      'International' AS CitypairBKEY,
      hp.DomesticOrInternational,
      1000000 AS CitypairSortOrder,
      hp.MonthNameYearEnglish,
      hp.DepartureMonthCode,
      Sum(m.BkdRev) AS BkdRev,
      Sum(m.Capacity) AS Capacity,
      Sum(m.BkdPax) AS BkdPax,
      Sum(m.BkdPax)/Sum(m.Capacity) as Loadfact,
      Sum(m.BkdRev)/Sum(m.BkdPax) as BkdRev_Pax,
      Sum(m.BkdRevWeek) AS BkdRevWeek,
      Sum(m.BkdPaxWeek) AS BkdPaxWeek,
      Sum(m.BkdRevWeek)/Sum(m.BkdPaxWeek) as BkdRev_PaxWeek
FROM
    hTable hp
    LEFT JOIN master m ON (hp.CitypairBKEY = m.CitypairBKEY) AND
            (hp.MonthNameYearEnglish = m.MonthNameYearEnglish)
WHERE
     hp.DomesticOrInternational = 'International'
GROUP BY
        hp.DomesticOrInternational,
        hp.MonthNameYearEnglish,
        hp.DepartureMonthCode), q5 as 
        (SELECT
      'Total' AS CitypairBKEY,
     'Total' AS DomesticOrInternational,
      10000000 AS CitypairSortOrder,
      hp.MonthNameYearEnglish,
      hp.DepartureMonthCode,
      Sum(m.BkdRev) AS BkdRev,
      Sum(m.Capacity) AS Capacity,
      Sum(m.BkdPax) AS BkdPax,
      Sum(m.BkdPax)/Sum(m.Capacity) as Loadfact,
      Sum(m.BkdRev)/Sum(m.BkdPax) as BkdRev_Pax,
      Sum(m.BkdRevWeek) AS BkdRevWeek,
      Sum(m.BkdPaxWeek) AS BkdPaxWeek,
      Sum(m.BkdRevWeek)/Sum(m.BkdPaxWeek) as BkdRev_PaxWeek
FROM
    hTable hp
    LEFT JOIN master m ON (hp.CitypairBKEY = m.CitypairBKEY) AND
            (hp.MonthNameYearEnglish = m.MonthNameYearEnglish)
           
GROUP BY
        hp.MonthNameYearEnglish,
        hp.DepartureMonthCode), q6 as (
        
        select * from q1
        union 
         select * from q2
          union 
         select * from q3
          union 
         select * from q4
          union 
         select * from q5)
        select * from q6 where DepartureMonthCode=%s
        
'''%(period)
    df =pd.read_sql_query(sql_query, SQLITE3)
    df['Period']=period
    df.to_sql(name='v'+str(num),con=SQLITE3,index=False,if_exists='replace')
    

sql_query='''
SELECT
      v1.CitypairBKEY,
      v1.BkdRev,
      v1.Capacity,
      v1.BkdPax,
      v1.Loadfact,
      v1.BkdRev_Pax,
      v1.BkdRevWeek,
      v1.BkdPaxWeek,
      v1.BkdRev_PaxWeek,
      '' AS x,
      v2.BkdRev AS BkdRev1,
      v2.Capacity AS Capacity1,
      v2.BkdPax AS BkdPax1,
      v2.Loadfact AS Loadfact1,
      v2.BkdRev_Pax AS BkdRev_Pax1,
      v2.BkdRevWeek AS BkdRevWeek1,
      v2.BkdPaxWeek AS BkdPaxWeek1,
      v2.BkdRev_PaxWeek AS BkdRev_PaxWeek1,
      '' AS y,
      v3.BkdRev AS BkdRev2,
      v3.Capacity AS Capacity2,
      v3.BkdPax AS BkdPax2,
      v3.Loadfact AS Loadfact2,
      v3.BkdRev_Pax AS BkdRev_Pax2,
      v3.BkdRevWeek AS BkdRevWeek2,
      v3.BkdPaxWeek AS BkdPaxWeek2,
      v3.BkdRev_PaxWeek AS BkdRev_PaxWeek2
FROM
    v1
    LEFT JOIN v2 ON v2.CitypairBKEY = v1.CitypairBKEY
    LEFT JOIN v3 ON v3.CitypairBKEY = v1.CitypairBKEY
ORDER BY
        v1.CitypairSortOrder
   
'''   
df =pd.read_sql_query(sql_query, SQLITE3)
##......................begin stracture for xlsxfile.........................






##......................create xlsxfile.........................................



sheet_name='fbwv'+str(week)

updated=time.strftime("%Y%m%d")
OutFileName='Outfile'
#writer = pd.ExcelWriter('C:\RONSEN\Costumers\BRA\3. Outdata\Outfile.xlsx', engine='xlsxwriter')
writer = pd.ExcelWriter(path+'\\Outfiles\\'+OutFileName+'_'+updated+'_'+sheet_name+'.xlsx', engine='xlsxwriter')
df.to_excel(writer, index=False, sheet_name=sheet_name,startrow=4)
workbook = writer.book
worksheet = writer.sheets[sheet_name]
merge_format  = workbook.add_format({'bold': True,'align': 'center'})

percent_fmt = workbook.add_format({'num_format': '0.0%'})

num_cols=['E:E','N:N','W:W']
for num_col in num_cols:
    worksheet.set_column(num_col, None, percent_fmt)
 

format1 = workbook.add_format({'num_format': '#,##0'})
num_cols=['B:B','C:C','B:B' ,'C:C' ,'D:D', 'F:F', 'G:G', 'H:H', 
          'I:I', 'J:J' ,'K:K', 'L:L', 'M:M', 'O:O', 'P:P' ,
          'Q:Q', 'R:R', 'S:S', 'T:T', 'U:U' ,'V:V', 'X:X', 'Y:Y', 'Z:Z', 'AA:AA']
for num_col in num_cols:
    worksheet.set_column(num_col, 18, format1)

worksheet.set_column('A:A', 20)
worksheet.merge_range('A2:AA2', 'Forward Bookings Weekly Report',merge_format )
worksheet.merge_range('B4:I4', str(Period1_excel),merge_format )
worksheet.merge_range('K4:R4', str(Period2_excel),merge_format ) 
worksheet.merge_range('T4:AA4', str(Period3_excel),merge_format )  
writer.save()



