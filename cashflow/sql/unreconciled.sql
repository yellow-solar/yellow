-- Unmatched manual payments and reversals
select p.*
from Finance.mobile_trn_recon s
join Angaza.payments p
	on p.angaza_id = s.angaza_id
where s.UnmatchedManualPmt = 1
	 or s.UnmatchedReversal = 1
order by s.id_seq_no desc
;