select Account_Paid_From
	,	'Agent Account' Account_Paid_Into 
	,	Payment_Date Trn_Date
	, 	-Amount_Paid Amount_MWK
	, 	Payment_Type Cashflow_Category
	,	concat(Agent_Angaza_ID,'-', Yellow_Agent_ID,'-', Agent_Name) Description
	,	Allowance_Type Comments
from Zoho.All_Agent_Payments a
where a.Account_Paid_From = 'Yellow Standard Bank Account'
;

select *
from Zoho.All_Agent_Payments a
order by a.Payment_Date desc
limit 200
;

select a.Account_Paid_From
	, sum(Amount_Paid)
from Zoho.All_Agent_Payments a
group by 1
order by a.Payment_Date desc
limit 500
;