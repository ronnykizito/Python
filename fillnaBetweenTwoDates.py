# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 13:25:47 2019

@author: F2531197
"""
filename=r'C:\Users\F2531197\OneDrive - Enfo Oyj\RONSEN\Costumers\1. Coop\ListBetweenTwoDates.xlsx'

df=pd.read_excel(filename,sheet_name='Data Sheet TEST')
df.rename(columns={'Start Date':'StartDate','End Date':'EndDate',
                   'Resource Name':'ResourceName'},inplace=True)

dates = pd.DataFrame(pd.date_range(start=df.min().StartDate, 
                     end=df.max().EndDate), columns=['Date'])

df=pd.merge(left=dates, right=df, left_on='Date', right_on='StartDate', 
         how='outer').fillna(method='ffill')
df=df.query('ResourceName =="Johan Michel" ')
df['WeekOfYearNumber']= df.Date.dt.weekofyear

