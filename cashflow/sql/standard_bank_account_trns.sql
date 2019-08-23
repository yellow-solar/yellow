drop view if exists Finance.standard_bank_account_trns;
create view Finance.standard_bank_account_trns as
select x.*
from
	(
	select Account_Paid_From
		,	Account_Paid_Into 
        ,	Trn_Date
		, 	case 	
				when h.Account_Paid_From = 'Yellow Standard Bank Account' then -h.Amount_in_MK
				when h.Account_Paid_Into = 'Yellow Standard Bank Account' then h.Amount_in_MK
				else 0 
			end Amount_MWK
		, 	Cashflow_Category
		,	Description
		,	Comments
	from Zoho.Historic_Cashflows_Report h
	where h.Account_Paid_From = 'Yellow Standard Bank Account'
		or h.Account_Paid_Into = 'Yellow Standard Bank Account'
        
	union all

	select Account_Paid_From
		,	'Agent Account' Account_Paid_Into 
		,	Payment_Date Trn_Date
		, 	-Amount_Paid Amount_MWK
		, 	Payment_Type Cashflow_Category
		,	concat(Agent_Angaza_ID,'-', Yellow_Agent_ID,'-', Agent_Name) Description
		,	Allowance_Type Comments

	from Zoho.All_Agent_Payments a
	where a.Account_Paid_From = 'Yellow Standard Bank Account'
	) x

order by x.Trn_Date desc
;
