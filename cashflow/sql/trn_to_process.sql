-- To process manual payments
select m.trn_ref_number AccountNumber
	, s.StmntPmtAmnt
	, s.ToProcessPmtAmt
	, m.*
from Finance.mobile_trn_recon s
join Finance.mobile m 
	on s.provider_id = m.provider_id
where s.ToProcessPmt = 1
    and s.trn_timestamp > '2019-07-10'
	and s.provider_id not in (
		select man.provider_id from Finance.mobile_manual_recon_trns man
	)
order by s.id_seq_no desc
;


