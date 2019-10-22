# Drop view if exists
drop view if exists Finance.All_Acounts_Daily_Movements;
# Create view of all accounts daily movements
create view Finance.All_Acounts_Daily_Movements as
select y.*
	, round(sum(Daily_Movement) over (partition by Account_Name, Currency order by y.Trn_Date),2) Calculated_Balance
	, z.Account_Balance_MWK Actual_Balance
    , round(z.Account_Balance_MWK - sum(Daily_Movement) over (partition by Account_Name, Currency order by y.Trn_Date)) Account_Balance_Surplus_Deficit
from (
	select d.Account_Name
		, d.Trn_Date
		, d.Currency
        , coalesce(x.Daily_Movement,0) Daily_Movement
	from (select date_d Trn_Date, x.Account_Name, x.Currency
		from Dimensions.date_d d 
		left join (select distinct Account_Paid_From Account_Name, Currency
					from Zoho.Historic_Cashflows_Report b
                    where b.Account_Paid_From not in ('Third Party', 'MH Local Cash Account', 'MS Personal','Yellow Airtel Merchant Account','PAC')) x
			on 1 = 1
        where d.date_d between '2018-01-01' and CURDATE()    
		) d
    left join
	(
		select Account_Name
		, Trn_Date
		, Currency
        , sum(Amount) Daily_Movement
        from 
        (
			(
			select Account_Paid_From Account_Name
				, Currency
				, 	Trn_Date
				,  	-Amount Amount
			from Zoho.Historic_Cashflows_Report h
			)
			union all
			(
			select Account_Paid_Into Account_Name
				  , Currency
				  , Trn_Date
				, Amount
			from Zoho.Historic_Cashflows_Report h
			) 
			union all
			(
			select Account_Paid_From Account_Name
				, "MWK" Currency
				,	Payment_Date Trn_Date
				, 	-Amount_Paid Amount
			from Zoho.All_Agent_Payments a
			where a.Account_Paid_From = 'Yellow Standard Bank Account'
			)
		) u
		group by 1,2,3
	) x
    on d.Trn_Date = x.Trn_Date
		and x.Account_Name = d.Account_Name
) y
left join (
	select *
		, max(ID) over (partition by Date, Account_Name) max_ID
		, max(b.Date) over (partition by Account_Name) Max_Date
	from Zoho.All_Account_Balances b
	) z 
	on z.Account_Name = y.Account_Name
	and y.Trn_Date = z.Date
	and z.ID = z.max_ID
order by 1,2,3
;
