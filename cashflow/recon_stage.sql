-- Transaction lines for airtel recon
drop table if exists Sandbox.mobile_transactions_recon;
create table Sandbox.mobile_transactions_recon as
select *
	, cast(r.trn_timestamp as date) trn_date
	, row_number() OVER (partition by r.provider, r.provider_acc_no order by r.provider_id) id_seq_no
    , row_number() OVER (partition by r.provider, r.provider_acc_no order by r.trn_timestamp) trn_seq_no
from 
	(
    -- reconciled payments 
		select * from Finance.mobile m
		join Angaza.payments p on p.provider_transaction = m.provider_id
		where trn_type = 'MR'
			and p.reversal is null
	UNION ALL
	(
    -- payments in airtel, not angaza
		select y.*
		from
			( 
			select * from Finance.mobile m
			left join Angaza.payments p on p.provider_transaction = m.provider_id
				 and p.reversal is null
			where trn_type = 'MR'
				and m.trn_status <> 'Transaction Failed'
				and m.trn_ref_number not like 'RO%%'    
			) y
            
		 where y.angaza_id is null
	 )
     ) r
where r.reversal is null
;
