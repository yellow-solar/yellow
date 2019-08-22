-- To process manual payments
select m.*
	, s.StmntPmtAmnt
	, s.ToProcessPmtAmt
    , m.trn_ref_number AccountNumber
from Finance.mobile_trn_recon s
join Finance.mobile m 
	on s.provider_id = m.provider_id
where s.ToProcessPmt = 1
	and s.trn_date < curdate()
order by s.id_seq_no desc
;


