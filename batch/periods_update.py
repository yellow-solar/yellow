import os
import pandas as pd
import numpy as np
import re
from pandas.tseries.offsets import MonthEnd
from datetime import timedelta
from datetime import datetime
import time
import itertools
import sqlite3
import requests
from requests.auth import HTTPBasicAuth
from io import StringIO

# Print timestamp for log
print("Running periods calculation and account enrichment:", datetime.now().strftime("%H:%M:%S"))

# Config month and timestamps
today_ts = pd.to_datetime('today').round('1s')
today_string = pd.to_datetime('today').strftime('%Y-%m-%d')

# Path to data
data_path = '../data'

# Custom Functions 
def AreaBelowTriangles(start,end,eff_period):
    if end >= 0:
        area = eff_period*(end + 0.5*(start-end))
    elif start <= 0:
        area = eff_period*(start + 0.5*(end-start))
    elif (start > 0) & (end < 0):
        m = (end-start)/eff_period
        b1 = -start/m
        area = 0.5*(b1*start + (eff_period-b1)*end)
    else:
        area = 0
    return (area)

def PositiveAreaBelowTriangles(start,end,eff_period):
    if end >= 0:
        area = eff_period*(end + 0.5*(start-end))
    elif start <= 0:
        area = 0.0
    elif (start > 0) & (end < 0):
        m = (end-start)/eff_period
        b1 = -start/m
        area = 0.5*b1*start
    else:
        area = 0.0
    return (area)

def NegAreaBelowTriangles(start,end,eff_period):
    if end >= 0:
        area = 0.0
    elif start <= 0:
        area = eff_period*(start + 0.5*(end-start))
    elif (start > 0) & (end < 0):
        m = (end-start)/eff_period
        b1 = -start/m
        area = 0.5*(eff_period-b1)*end
    else:
        area = 0.0
    return (area)

# Set the columns we want from the data sets
account_columns = ['account_number','previous_account_number','angaza_id','owner_msisdn','registration_date_utc','group_name',
              'date_of_latest_payment_utc','date_of_disablement_utc', 'date_of_repossession_utc','account_status',
              'upfront_price','total_paid','unlock_price','minimum_payment_amount']
payment_columns = ['angaza_id','account_number','account_angaza_id','type','down_payment','effective_utc','recorded_utc','amount','phone','payment_note','reversal','reversal_note']
acc_date_cols = ['date_of_latest_payment_utc','date_of_disablement_utc','registration_date_utc', 'date_of_repossession_utc']
pay_date_cols = ['effective_utc','recorded_utc']

# Print current path
print(os.listdir())

# Import account and payments data
accounts_per_month = pd.DataFrame()
for f in os.listdir(data_path+'/'):
    data = pd.read_csv(data_path+'/'+f)
    print(f)
    if re.search("[0-9]{6}", f):
        month_key = re.findall("[0-9]{6}", f)[0]
        account_data = pd.read_csv(data_path+'/'+f, parse_dates = acc_date_cols)
        account_data['account_timestamp'] = month_key+'01'
        accounts_per_month = accounts_per_month.append(account_data[account_columns+['account_timestamp']])
    elif re.search("payments.csv", f):
        payments_data = pd.read_csv(data_path+'/'+f, parse_dates = pay_date_cols)[payment_columns]
        payments_data['reversal_amount'] = payments_data.amount*(~payments_data.reversal.isna()*1)
        payments_data['amount'] = payments_data.amount*(payments_data.reversal.isna()*1)

# Accounts per month dataset with date and month keys
accounts_per_month.account_timestamp = pd.to_datetime(accounts_per_month.account_timestamp)+ MonthEnd(1)
accounts_per_month['eom_timestamp'] = pd.to_datetime(accounts_per_month.account_timestamp)+ MonthEnd(1)
# Shorten the account timestamp until today
accounts_per_month.account_timestamp = accounts_per_month.account_timestamp.apply(lambda x: x if x < today_ts else today_ts)
accounts_per_month['registration_date'] = accounts_per_month.registration_date_utc.dt.strftime('%Y-%m-%d')
accounts_per_month['month_key'] = accounts_per_month.account_timestamp.dt.strftime('%Y%m')
accounts_per_month['registration_month'] = accounts_per_month.registration_date_utc.dt.strftime('%Y%m')

accounts_latest = accounts_per_month[accounts_per_month.account_timestamp == today_ts]

# Payments data needs the angaza id renamed to match the account datasets for merging
payments_data=payments_data.rename(columns = {'angaza_id':'payment_angaza_id','account_angaza_id':'angaza_id'})
payments_data['effective_date'] = payments_data.effective_utc.dt.strftime('%Y-%m-%d')
payments_data['effective_month'] = payments_data.effective_utc.dt.strftime('%Y%m')
payments_data['recorded_date'] = payments_data.recorded_utc.dt.strftime('%Y-%m-%d')
# NB the month key is for the data as at the end of that month
# payments_data[payments_data.angaza_id == 'AC830625']


# Create an account at registration row with the angaza ID to which it is linked
# July and September 2018 are missing on the angaza links - therefore use the minimum date at which the account appears
account_at_registration = accounts_per_month.groupby(['angaza_id']).aggregate({'month_key':'min'}).reset_index()
account_at_registration = account_at_registration.merge(accounts_per_month,on=['angaza_id','month_key'])
account_at_registration = account_at_registration[['angaza_id','registration_date_utc','registration_date','registration_month','upfront_price','group_name']]

# --If you had all the data you could do this where you select rows where account registration year and month is the dataset year and month 
# accounts_per_month[(accounts_per_month.month_key.dt.month == accounts_per_month.registration_date_utc.dt.month) &
# (accounts_per_month.month_key.dt.year  == accounts_per_month.registration_date_utc.dt.year)]

print('accounts at registration: ' + str(len(account_at_registration.angaza_id.unique())))
# print('accounts other: ')

#  Create an expected payment date dataframe
days_diff_list = []
payment_terms = 24
for x in range(0,payment_terms+1):
    days_diff_list.append(x*30)
    schedules = pd.DataFrame({'days_diff':days_diff_list})
    
# account_at_registration[account_at_registration.account_number==9210576.0].merge(schedules,right_index=False)

l=list(itertools.product(account_at_registration.angaza_id.values.tolist(),days_diff_list))

account_schedule = pd.DataFrame(l).rename(columns = {0:'angaza_id',1:'days_diff'})
account_schedule = account_at_registration.merge(account_schedule)

# Set a start and end date of the payment period
account_schedule['begin_payment_period_utc'] = (account_schedule.registration_date_utc + 
                                            pd.to_timedelta(pd.np.ceil(account_schedule.days_diff), unit="D"))


# Filter to periiods that have happened or started
account_schedule = account_schedule[(account_schedule.begin_payment_period_utc<today_ts)]

# Create an end period date as: beginning + 30 days - 1 second
account_schedule['end_payment_period_utc'] = (account_schedule.begin_payment_period_utc 
                                            + pd.to_timedelta(30,unit='D')
                                            + pd.to_timedelta(-1,unit='S'))

# Set the latest payment period to end on today/current timestamp 
account_schedule['end_payment_period_utc'] = account_schedule.end_payment_period_utc.apply(lambda x: x if x < today_ts else today_ts)                                

# Create date string and end date thing
account_schedule['begin_payment_period_date'] = account_schedule['begin_payment_period_utc'].dt.strftime('%Y-%m-%d')
account_schedule['month_key'] = account_schedule['begin_payment_period_utc'].dt.strftime('%Y%m')
account_schedule['end_payment_period_date'] = account_schedule['end_payment_period_utc'].dt.strftime('%Y-%m-%d')
account_schedule['end_payment_period'] = account_schedule['end_payment_period_utc'].dt.strftime('%Y%m')

# Create an indicator for if the period is the first - i.e. registration
account_schedule['Registration'] = (account_schedule.registration_date_utc==account_schedule.begin_payment_period_utc)*1
# account_schedule

# Put into a sqllite so we can join between and get payments before the end of the period etc. and join the event dates
#Make the db in memory
conn = sqlite3.connect(':memory:')
cur = conn.cursor()

#write the tables
account_schedule.to_sql('account_schedule', conn, index=False)
account_at_registration.to_sql('account_at_registration', conn, index=False)
accounts_per_month.to_sql('accounts_monthly', conn, index = False)
payments_data.to_sql('payments', conn, index=False)

# orange.to_sql('orange', conn, index=False)
# red.to_sql('red', conn, index=False)
# black.to_sql('black', conn, index=False)

# See all table names
cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()

# Create a payment date and end of payment period date table per Account ID
payment_and_periodstart_dates = '''
    create table payment_periods_step as
    select distinct AngazaID, 
        -- case when y.EffectiveFrom <= r.registration_date_utc then r.registration_date_utc else y.EffectiveFrom end EffectiveFrom
        EffectiveFrom
    from (
        select 
            a.angaza_id AngazaID,
            begin_payment_period_utc EffectiveFrom
            
        from account_schedule a

        union all

        select 
            x.angaza_id AngazaID,
            x.effective_utc EffectiveFrom
            
        from payments x
   
   ) y
   left join account_at_registration r
        on y.AngazaID = r.angaza_id
'''

if 'payment_periods_step' not in [a[0] for a in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
    cur.execute(payment_and_periodstart_dates)
    print('Payment period and end dates created')
else:
    if 'payment_periods' in [a[0] for a in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
        cur.execute('drop table payment_periods')
    cur.execute('drop table payment_periods_step')
    cur.execute(payment_and_periodstart_dates)
    print('Payment period and end dates dropped and re-created')
    

payment_periods_step = pd.read_sql_query("select * from payment_periods_step",conn).sort_values(['AngazaID','EffectiveFrom']).reset_index(drop=True)

payment_periods_step['EffectiveFrom'] = pd.to_datetime(payment_periods_step.EffectiveFrom)
payment_periods_step['EffectiveTo'] = (pd.to_datetime(payment_periods_step.groupby('AngazaID')['EffectiveFrom'].shift(-1)) - pd.to_timedelta(1,unit='S'))
payment_periods_step['EffectiveTo'] = payment_periods_step.EffectiveTo.fillna(today_ts)
payment_periods_step['EffectiveDays'] = (payment_periods_step.EffectiveTo - payment_periods_step.EffectiveFrom).dt.days.round(2)
payment_periods_step['CumEffectiveDaysEnd'] = payment_periods_step.groupby(['AngazaID']).aggregate({'EffectiveDays':'cumsum'})
payment_periods_step['CumEffectiveDaysStart'] = payment_periods_step.CumEffectiveDaysEnd - payment_periods_step.EffectiveDays 

# Put the payment periods table into the SQL database
payment_periods_step.to_sql('payment_periods', conn, index=False)

# payment_periods_step.head()
# payment_periods_step[payment_periods_step.AngazaID == 'AC913462']

q = '''
    select distinct
        pp.*,
        
        begin_payment_period_utc PPStartTimestamp,
        end_payment_period_utc PPEndTimestamp,
        r.registration_date_utc RegistrationTimestamp,
        m.date_of_repossession_utc RepossessionTimestamp,
        m.group_name PaymentGroup,
        m.account_number AccountNumber,
        m.previous_account_number PreviousAccountNumber,
        r.upfront_price UpfrontPrice,
        m2.unlock_price UnlockPrice,
        m.minimum_payment_amount/1.0 PeriodMPA,
        case when r.group_name in ('Biolite - Urban or Rural - Cash Upfront') then 1 else 0 end CashSale,
        coalesce(p.amount,0) AmountPaid,
        p.reversal Reversal,
        m.account_timestamp AccountDataTimestamp
        
    from payment_periods pp
    
    left join account_at_registration r
        on pp.AngazaID = r.angaza_id
    
    left join account_schedule s
        on pp.AngazaID = s.angaza_id
        and pp.EffectiveTo between s.begin_payment_period_utc and s.end_payment_period_utc
                      
    left join accounts_monthly m
        on pp.AngazaID = m.angaza_id
        --and m.eom_timestamp between s.begin_payment_period_utc and s.end_payment_period_utc
        and m.month_key = s.month_key
        
    left join accounts_monthly m2
        on pp.AngazaID = m2.angaza_id
        and m2.month_key = strftime("%Y%m", 'now')
        
    left join payments p
        on pp.AngazaID = p.angaza_id
        and pp.EffectiveFrom = p.effective_utc 
    --    and p.effective_utc <> r.registration_date_utc
    
    left join (select i.angaza_id,
                        i.effective_utc, 
                        sum(i.amount) amount 
                from payments i
                group by 1,2)  p2
        on pp.AngazaID = p2.angaza_id
        and p2.effective_utc = r.registration_date_utc
        and p2.amount > 0
    
'''

df = pd.read_sql_query(q,conn, parse_dates=['EffectiveFrom','EffectiveTo','PPStartTimestamp','PPEndTimestamp','RegistrationTimestamp']).sort_values(['AngazaID','EffectiveFrom']).reset_index(drop=True)

# Fill previous months that were missing 
df[['AccountNumber','PreviousAccountNumber','UnlockPrice','PeriodMPA','AccountDataTimestamp']] = df.groupby('AngazaID')[['AccountNumber','PreviousAccountNumber','UnlockPrice','PeriodMPA','AccountDataTimestamp']].ffill().bfill()
len(df[df.PeriodMPA.isna()].index)

# Calculate a cumulative owed amount
# Needs to be calculated based on days passed and the Monthly payment amount

# Backfill reposession dates based on AngazaID - the ordering in the groupby is different to how you are viewing it - investigate!
df.RepossessionTimestamp = pd.to_datetime(df.RepossessionTimestamp)
df = df.sort_values(['AngazaID','EffectiveFrom'])
# df.RepossessionTimestamp = df.groupby('AngazaID')['RepossessionTimestamp'].bfill()
df.RepossessionTimestamp = df.groupby('AngazaID')['RepossessionTimestamp'].ffill()

# Filter the dates after reposession
df = df[df.RepossessionTimestamp.isna() | (df.EffectiveFrom<=df.RepossessionTimestamp)]

# df[df.AngazaID == test_case]

# Create a payment line indicator
df['Payment'] = 1*(df.AmountPaid>0)

# Calculate total cumulative paid at end and start of period, and the cumulative amount paid within a perid
df['CumPaid'] = df.groupby(['AngazaID']).aggregate({'AmountPaid':'cumsum'})
df['PrevCumPaid'] = df['CumPaid'] - df['AmountPaid']
df['CumPaidMonth'] = df.groupby(['AngazaID','PPEndTimestamp']).aggregate({'AmountPaid':'cumsum'}).fillna(0)
df['CumPaidLessUpfront'] = df['CumPaid'] - df['UpfrontPrice']
df['DepositPaidInd'] = (df['CumPaidLessUpfront']>=0)*1
# df['TotalPaid'] = df.CumPaid + df.DepositAmount

# TO BE ADDED TO THE SQL FOR FUTURE
# Get the unlock date and merge to the dataset to filter out after unlock
df['Unlocked'] = 1*(df.CumPaid >= df.UnlockPrice)
Unlocked = df.groupby(['AngazaID','Unlocked']).aggregate({'EffectiveFrom':'min'}).reset_index()
Unlocked = Unlocked[Unlocked.Unlocked == 1].rename(columns = {'EffectiveFrom':'UnlockDate'}).reset_index(drop=True)
df = df.merge(Unlocked[['AngazaID','UnlockDate']], how='left')

# Filter all dates after unlock
df = df[(df.EffectiveFrom <= df.UnlockDate) | (df.UnlockDate.isna())]


# Days calculations
df['DaysPaid'] = (30 +(df.CumPaidLessUpfront/(df.PeriodMPA/30)))*df.DepositPaidInd.round(2)
# df['DaysPaidStart'] = 30*df.DepositAmount+(df.PrevCumPaid/(df.PeriodMPA/30)).fillna(0)

df['ArrearsIndicator'] = (df.DaysPaid < df.CumEffectiveDaysEnd)*1
df['DaysInArrearsEnd'] = (df.CumEffectiveDaysEnd - df.DaysPaid)*df.ArrearsIndicator
# df['DaysEarly'] = ((df.DaysPaid - df.CumEffectiveDaysStart - 30)*df.Payment)
# df['DaysEarly'] = df.DaysEarly*(df.DaysEarly>0)

df['DaysEnabledEnd'] = (df.DaysPaid - df.CumEffectiveDaysEnd)*(1-df.ArrearsIndicator)
df['DaysEnabledStart'] = (df.DaysPaid - df.CumEffectiveDaysStart)*((df.DaysPaid > df.CumEffectiveDaysStart)*1)


# Working out the area under curves
df['PaymentTrackingStart'] = ((df.DaysPaid - df.CumEffectiveDaysStart)/30.0).round(4)
df['PaymentTrackingEnd'] = ((df.DaysPaid - df.CumEffectiveDaysEnd)/30.0).round(4)

v = np.vectorize(AreaBelowTriangles)
p = np.vectorize(PositiveAreaBelowTriangles)
n = np.vectorize(NegAreaBelowTriangles) 
df['Area'] = v(df.PaymentTrackingStart, df.PaymentTrackingEnd, df.EffectiveDays)
df['PositiveArea'] = p(df.PaymentTrackingStart, df.PaymentTrackingEnd, df.EffectiveDays)
df['NegativeArea'] = n(df.PaymentTrackingStart, df.PaymentTrackingEnd, df.EffectiveDays)

# Event days - orange, red, black and REPO
codes = [-1000,0.1,7,21,31,10000]
code_names = ['1 Green','2 Orange','3 Red','4 Black','5 Repo']
df['CodeStatus'] = pd.cut(df.DaysInArrearsEnd,codes, labels=code_names)

# Split data into two sets: 
#   - first view as at the end of each month
#   - second the aggregate view over the 30 day period

# 1.) As at end of period
end_of_period_only = df[(df.PPEndTimestamp == df.EffectiveTo)].copy()

end_of_period_only = end_of_period_only[['AngazaID','PPStartTimestamp','PPEndTimestamp','CumEffectiveDaysEnd',
                                         'RegistrationTimestamp','RepossessionTimestamp',
                                         'AccountNumber', 'PreviousAccountNumber','AccountDataTimestamp','PaymentGroup',
                                         'UpfrontPrice', 'UnlockPrice', 'PeriodMPA', 
                                         'CashSale','CumPaid', 'PrevCumPaid','CumPaidLessUpfront',
                                         'DaysPaid','DaysEnabledEnd',
                                         'ArrearsIndicator','DaysInArrearsEnd', 
                                         'Unlocked', 'UnlockDate',
                                         'PaymentTrackingEnd', 'CodeStatus']]
end_of_period_only['NextCodeStatus'] = end_of_period_only.groupby('AngazaID')['CodeStatus'].shift(-1)

# 2.) Groupby account and month in order to sum up the areas
account_monthly_profile = df.groupby(['AngazaID', 'PPStartTimestamp', 'PPEndTimestamp']).aggregate({
    'EffectiveDays':'sum',
    'Unlocked':'max',
    'DaysInArrearsEnd':'max',
    'DaysEnabledStart':'max',
    'Area':'sum',
    'PositiveArea':'sum',
    'NegativeArea':'sum',
    'CodeStatus':'max'
}).reset_index().rename(columns = {
    'EffectiveDays':'EffectiveDaysPeriod',
    'Unlocked':'UnlockedMax',
    'DaysInArrearsEnd':'MaxDaysInArrearsEnd',
    'DaysEnabledStart':'MaxDaysEnabled',
    'Area':'AreaSum',
    'PositiveArea':'PositiveAreaSum',
    'NegativeArea':'NegativeAreaSum',
    'CodeStatus':'MaxCodeStatus'
})

# Max days enabled previous month
account_monthly_profile['MaxDaysEnabledPrevPeriod'] = account_monthly_profile.groupby('AngazaID')['MaxDaysEnabled'].shift(1).fillna(0)
account_monthly_profile['MaxDaysEnabled_IP2M'] = account_monthly_profile[['MaxDaysEnabled','MaxDaysEnabledPrevPeriod']].max(axis=1)

# Arrears past 3 months
account_monthly_profile['MaxDaysInArrearsEndPrevPeriod1'] = account_monthly_profile.groupby('AngazaID')['MaxDaysInArrearsEnd'].shift(1).fillna(0)
account_monthly_profile['MaxDaysInArrearsEndPrevPeriod2'] = account_monthly_profile.groupby('AngazaID')['MaxDaysInArrearsEnd'].shift(2).fillna(0)
account_monthly_profile['MaxDaysInArrearsEnd_IP3M'] = account_monthly_profile[['MaxDaysInArrearsEnd','MaxDaysInArrearsEndPrevPeriod1','MaxDaysInArrearsEndPrevPeriod2']].max(axis=1)

print("Rows per dataset:")
print(len(df),len(end_of_period_only),len(account_monthly_profile))

# Merge 1 and 2 together to get a single line per account per payment period with every piece of data needed 
# (either the values are the total/average or as at end of month)
account_periods = end_of_period_only.merge(account_monthly_profile,how='left')

# Calculate Dolo wa Yellow score
# If customer is unlocked - how many days ahead? have to unlock within x days
# If customer is not unlocked - 
# Customer has been min 180 days ahead in current period or previous period + currently above 150 - Dolo wa Yellow
# Customer has been min 90 days ahead in current period or previous period + currently above 60 - Moto Wochuluka
# Customer enabled + no arrears more than 1 day in past 3 periods (incl current ) - Moto Wapakati
# Customer enabled had arrears > 1 day in past 3 months - Moto Ochepa
# Customer disabled - Mulibe Moto

def DoloScore(row):
    # Cash sales are tier 5
    if row['CashSale'] == 1:
        return(5)
    
    # Unlocked: depends on how quickly it was unlocked and then if you had arrears at the end
    elif row['Unlocked'] == 1:
        days_to_unlock = (row['UnlockDate'] - row['RegistrationTimestamp']).days
        if days_to_unlock < 540:
            return(5)
        elif days_to_unlock < 630:
            return(4)
        elif (row['MaxDaysInArrearsEnd_IP3M']>=-1):
            return(3)
        else:
            return(2) 
    
    # Current accounts
    else:
        if (row['DaysInArrearsEnd'] > 1):
            return(1)
        elif (row['MaxDaysEnabled_IP2M'] >= 180) and (row['DaysEnabledEnd'] >=150):
            return(5)
        elif (row['MaxDaysEnabled_IP2M'] >= 90) and (row['DaysEnabledEnd'] >=60):
            return(4)
        elif (row['DaysEnabledEnd'] > 0) and (row['MaxDaysInArrearsEnd_IP3M']<=1):
            return(3)
        else:
            return(2)

# Calculate tier per person
account_periods['DoloTier'] = account_periods.apply(DoloScore, axis=1)

# Tier names
tiers = [0,1,2,3,4,5]
tier_names = ['Mulibe Moto','Moto Ochepa','Moto Wapakati','Moto Wochuluka','Dolo wa Yellow!']
account_periods['DoloStatus'] = pd.cut(account_periods.DoloTier, tiers, labels=tier_names)

# Create new dataset for live row - including date of unlock, repossession - use max PP end timestamp for each angazaID
max_ts_per_id = account_periods.groupby('AngazaID')['PPEndTimestamp'].max().reset_index().rename(columns ={'PPEndTimestamp':'MaxPPEndTimestamp'})
live_accounts_period = account_periods.merge(max_ts_per_id, how = 'left')

#  Select live row and drop Max
live_accounts_period = live_accounts_period[live_accounts_period.MaxPPEndTimestamp == live_accounts_period.PPEndTimestamp]
live_accounts_period = live_accounts_period.drop('MaxPPEndTimestamp',axis=1)

# Payment event periods table is the table where you can see the payment events and what the status of the account was on that day
account_payment_event_periods = df

# Store the datasets ready for database/analysis
account_payment_event_periods.to_csv(data_path+'/accounts_payment_event_periods.csv',index=False)
account_periods.to_csv(data_path+'/accounts_periods.csv',index=False)
live_accounts_period.to_csv(data_path+'/accounts_enriched.csv',index=False)

# Put the full payment history table into the SQL database
if 'accounts_payment_periods' not in [a[0] for a in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
    account_payment_event_periods.to_sql('accounts_payment_periods', conn, index=False)
    account_periods.to_sql('accounts_periods', conn, index=False)
    print('Account payment profile created')
else:
    cur.execute('drop table account_payment_profile')
    df.to_sql('account_payment_profile', conn, index=False)
    print('Account payment profile dropped and created') 