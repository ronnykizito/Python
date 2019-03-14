# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 11:23:24 2019

@author: F2531197
"""







import pandas as pd
from datetime import datetime
from workalendar.europe import Sweden
from pandas.tseries.offsets import BDay
from pandas.tseries.offsets import CDay
from pandas.tseries.holiday import (AbstractHolidayCalendar , EasterMonday,GoodFriday, Holiday) #next_monday next_monday_or_tuesday,MO,DateOffset , nearest_workday
from datetime import date
import time

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






#cal.holidays(2019)

qlist=pd.DataFrame({'Date':[datetime.today()]})
qlist['Date'] = pd.Timestamp('2016-05-01')
qlist['period_start']=((qlist.Date - pd.DateOffset(months=1)).dt.to_period("m").dt.start_time).dt.strftime("%Y-%m-%d")
qlist['period_End']=((qlist.Date + pd.DateOffset(months=13)).dt.to_period("m").dt.end_time).dt.strftime("%Y-%m-%d")

Min_date="[]".join(list(qlist.period_start))
Max_date="[]".join(list(qlist.period_End))

df = pd.DataFrame(index = pd.date_range(Min_date, Max_date, freq='D'))
df['Date']=df.index
df['Date_Key'] = range(1, 1+len(df) ) #increasing by 1
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
df['DayOfQuarter'] = df.groupby(['QuarterCode']).cumcount()+1
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
df['IsWorkingDay'] = df.Date.apply(lambda x: cal.is_working_day(pd.to_datetime(x)))
df['xx']=time.strftime("%Y-%m-%d")
df['Updated']=df.xx.values.astype("datetime64[s]")
df['Offsetdays'] = df.Updated - pd.DateOffset(days=1)

df.drop(['DateID','Date_Key','xx'], axis=1,inplace=True)
#dont change Next_N_days_Ex_Weekend Next_N_days_Ex_Weekend_Holidays
#df['Next_N_days_Ex_Weekend'] = df.Date + BDay(1)
#df['xx'] = (df['date1'].dt.to_period('B') -df['Date'].dt.to_period('B'))#B =Weekdays
df['Next_N_WorkingDay'] = pd.to_datetime(df.Date).apply(lambda x: cal.add_working_days(date(x.year ,x.month ,x.day), 6))
##Calculate the number or working days between two given days
df['N_days_between']=df.apply(lambda x: 
    cal.get_working_days_delta(date(x['Date'].year, x['Date'].month, x['Date'].day), 
                               date(x['date1'].year, x['date1'].month, x['date1'].day)), axis=1)
df['TestFloat']=0.12345678985968
    
df.info()
    
dates=['Date','DateKey','DayDate','FirstDateOfYear','LastDateOfYear',
       'FirstDateOfQuarter','LastDateOfQuarter','FirstDateOfMonth',
       'LastDateOfMonth','FirstDateOfWeek','LastDateOfWeek','PreviousDateYear',
       'PreviousDateMonth','PreviousDateMonthStart','PreviousDateMonthEnd',
       'PreviousDateWeek','PreviousDateDay','NextDateDay','Offsetdays','Updated']

'''==============all numeric vars + floats==============================='''
numeric=['YearMonthDay','YearMonth','YearCode','QuarterCode','QuaterNumber',
         'FirstMonthOfQuarter','LastMonthOfQuarter','MonthCode','MonthOfYearNumber',
         'NumberOfDaysInMonth','WeekCode','WeekOfYearNumber','DayOfYear',
         'DayOfQuarter','DayOfMonth','DayOfWeek','months_between','N_days_between',
         'TestFloat']


df=pd.DataFrame({'Cols':list(df.columns)})

df['Datatype']=df.Cols+' NVARCHAR(max),'
df.loc[df.Cols.isin(dates), 'Datatype'] = df.Cols+' DATE,'
df.loc[df.Cols.isin(numeric), 'Datatype'] = df.Cols+' float(53),'
df['ID'] = 1
df['IDx'] = range(1, 1+len(df) )
df['IDMax'] = df.groupby(['ID'])['IDx'].transform(lambda x : x.max())
#df.loc[df.IDx==df.IDMax, 'Datatype'] = df.Cols+' NVARCHAR(max)'



ColsToDatabase="".join([str(s) for s in list(list(df.Datatype))])

table='dDate'
create_table='''
CREATE TABLE %s(
%s
)
'''%(table,str(ColsToDatabase))
pd.io.sql.execute('DROP table IF EXISTS '+str(table), migrate_to_databse)
pd.io.sql.execute(create_table, migrate_to_databse)

df.to_sql(name=str(table),con=migrate_to_databse,index=False,if_exists='append')    
    
    
#To_date=['Date','DateKey']
#int_float=['YearMonth']
#df[int_float] = df[int_float].apply(pd.to_numeric, errors='coerce')
#df[To_date] = df[To_date].values.astype("datetime64[s]")

    
#df.to_sql(name='DimDate',con=sqlite_db,index=False,if_exists='replace')  
#del qlist





#from datetime import date
#
#df['Next_N_WorkingDay'] = df.Date + (1 * business)
#
##https://peopledoc.github.io/workalendar/
#
#df['cc'] = cal.add_working_days(date(2017 ,12 ,1), 5)
#
#
#df['ccz'] = cal.add_working_days(df.Date, 5)
#
#df['cc'] = df.Date.apply(lambda x: cal.add_working_days(date(x.strftime('%Y').astype(int),12,31),1))
#
#df['cc'] =df.apply(cal.add_working_days(date(2017 ,12 ,1), 5), axis = 1)
#
#
#df['cc'] = cal.add_working_days(date(df.Date.dt.year ,df.Date.dt.month ,df.Date.dt.day), 5)
#
#df['date_column_trunc'] = df.Date.apply(lambda s: pd.to_datetime(s.dt.year, s.dt.month, 1))
#
#df['cc'] =df.Date.dt.month
#df.info()
#
#df['cc'] = cal.add_working_days(date(df.Date.map(lambda x: x.strftime('%Y,%m,%d'))), 5)
#
#
#df['cc'] = cal.add_working_days(datetime(pd.Series([df.Date.dt.strftime('%Y').astype(int)]), 12, 1), 5, keep_datetime=True)
#
#pd.Series([df.Date.dt.strftime('%Y').astype(int)])
#
##Calculate the number or working days between two given days
#cal.get_working_days_delta(date(2018, 4, 2), date(2018, 6, 17))
#Property 	Description
#year 	The year of the datetime
#month 	The month of the datetime
#day 	The days of the datetime
#hour 	The hour of the datetime
#minute 	The minutes of the datetime
#second 	The seconds of the datetime
#microsecond 	The microseconds of the datetime
#nanosecond 	The nanoseconds of the datetime
#date 	Returns datetime.date (does not contain timezone information)
#time 	Returns datetime.time (does not contain timezone information)
#dayofyear 	The ordinal day of year
#weekofyear 	The week ordinal of the year
#week 	The week ordinal of the year
#dayofweek 	The number of the day of the week with Monday=0, Sunday=6
#weekday 	The number of the day of the week with Monday=0, Sunday=6
#weekday_name 	The name of the day in a week (ex: Friday)
#quarter 	Quarter of the date: Jan-Mar = 1, Apr-Jun = 2, etc.
#days_in_month 	The number of days in the month of the datetime
#is_month_start 	Logical indicating if first day of month (defined by frequency)
#is_month_end 	Logical indicating if last day of month (defined by frequency)
#is_quarter_start 	Logical indicating if first day of quarter (defined by frequency)
#is_quarter_end 	Logical indicating if last day of quarter (defined by frequency)
#is_year_start 	Logical indicating if first day of year (defined by frequency)
#is_year_end 	Logical indicating if last day of year (defined by frequency)
#is_leap_year 	Logical indicating if the date belongs to a leap year
#














