select *
from Finance.mobile_trn_recon s
where s.UnassignedPmt = 1
order by s.id_seq_no desc
;
