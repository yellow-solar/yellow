-- Transaction lines for airtel recon
-- THIS SHOULD PROBABLY BE A VIEW RATHER
drop table if exists Finance.mobile_trn_recon;
create table Finance.mobile_trn_recon as
select x.*
	, case when x.trn_timestamp is not null then cast(x.trn_timestamp as date) else cast(x.recorded_utc as date) end trn_date
	, row_number() OVER (partition by x.provider, x.provider_acc_no order by x.provider_id) id_seq_no
    , row_number() OVER (partition by x.provider, x.provider_acc_no order by x.trn_timestamp) trn_seq_no
    
	, case when x.provider_transaction is null then 
			concat('MP', REGEXP_SUBSTR(x.payment_note, '[123][0-9]{5}'), '.', REGEXP_SUBSTR(x.payment_note, '[0-9]{4}.[a-zA-Z][0-9]{5}')) 
		else x.provider_transaction end provider_transaction_manual
	
    , case when x.provider_id is not null then 1 else 0 end StmntPmt
    , case when x.provider_id is not null then x.trn_amount else 0 end StmntPmtAmnt
	, case when x.provider_id is not null and (x.angaza_id is not null or r.angaza_id is not null) then 1 else 0 end MatchedPmt
    , case when x.provider_id is not null and (x.angaza_id is not null or r.angaza_id is not null) then x.trn_amount else 0 end MatchedPmtAmt
	, case when x.provider_id is null and x.reversal is null and x.type = 'Manual (Hub)' then 1 else 0 end UnmatchedManualPmt
    , case when x.provider_id is null and x.reversal is null and x.type = 'Manual (Hub)' then x.amount else 0 end UnmatchedManualPmtAmt
    , case when x.provider_id is null and x.reversal is null and x.type = 'Airtel Malawi (via GSMA)' then 1 else 0 end MissingStmntPmt
    , case when x.provider_id is null and x.reversal is null and x.type = 'Airtel Malawi (via GSMA)' then x.amount else 0 end MissingStmntPmtAmt
	, case when x.provider_id is null and x.reversal is not null then 1 else 0 end UnmatchedReversal
    , case when x.provider_id is null and x.reversal is not null then x.amount else 0 end UnmatchedReversalAmt
    
    , case when x.provider_id is not null and x.angaza_id is null and r.angaza_id is null then 1 else 0 end ToProcessPmt
    , case when x.provider_id is not null and x.angaza_id is null and r.angaza_id is null then x.trn_amount else 0 end ToProcessPmtAmt
    
    , case when  r.angaza_id is not null then 1 else 0 end UnassignedPmt
    , r.amount UnassignedPmtAmt
	, r.angaza_id unassigned_angaza_id
	, r.recorded_utc unassigned_recorded_utc
	, r.type unassigned_type
	, r.reference unassigned_ref
from
	(
	select *
	from Finance.mobile m
    
	left join Angaza.payments p 
		on (p.provider_transaction = m.provider_id
			or concat('MP', REGEXP_SUBSTR(p.payment_note, '[123][0-9]{5}'), '.', REGEXP_SUBSTR(p.payment_note, '[0-9]{4}.[a-zA-Z][0-9]{5}')) = m.provider_id)
		and p.reversal is null
	where m.trn_timestamp >= CURDATE() - INTERVAL 60 DAY
		and m.trn_type = 'MR'
		and m.trn_status not like 'Transaction F%'
		and m.trn_ref_number not like 'R%'	
        
	union all

	select *
	from  Finance.mobile m
	right join (select * from Angaza.payments i where i.recorded_utc > cast('2019-07-11 00:00:00' as datetime)) p 
		on (p.provider_transaction = m.provider_id
			or concat('MP', REGEXP_SUBSTR(p.payment_note, '[123][0-9]{5}'), '.', REGEXP_SUBSTR(p.payment_note, '[0-9]{4}.[a-zA-Z][0-9]{5}')) = m.provider_id)
	where m.trn_timestamp >= CURDATE() - INTERVAL 60 DAY
		and m.provider_id is null
	) x
    
left join Angaza.receipts r
		on r.provider_transaction_id = x.provider_id
	
;