-- Running balances
select x.BalanceDate
	, Amount_MWK BalanceMovement
	, round(sum(Amount_MWK) over (order by x.BalanceDate),2) CalcBalance
    , y.Account_Balance_MWK ActualBalance
    , round(y.Account_Balance_MWK - sum(Amount_MWK) over (order by x.BalanceDate),2) AccountBalanceDeficit
	from (
	select d.date_d BalanceDate
		, coalesce(sum(Amount_MWK),0) Amount_MWK
	from Dimensions.date_d d
	left join Finance.standard_bank_account_trns s
		on d.date_d = s.Trn_Date
	where d.date_d between '2018-01-01' and CURDATE()
    group by 1
	) x
left join  (
	select *, max(ID) over (partition by Date, Account_Name) max_ID
	from Zoho.All_Account_Balances b	
	where b.Account_Name = 'Yellow Standard Bank Account'
	
	) y
on y.Date = x.BalanceDate
	and y.ID = y.max_ID
/* having y.Account_Balance_MWK is not null */
order by 1 desc
;
