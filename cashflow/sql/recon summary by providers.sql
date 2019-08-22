
### Finance report
drop table if exists Finance.mobile_trn_recon_summary;
create table Finance.mobile_trn_recon_summary as 
with max_seq as (
-- max seq trn of the day summary 
select *	
	, min(id_seq_no) over (partition by provider, provider_acc_no) min_id_seq_no
	, max(id_seq_no) over (partition by provider, provider_acc_no) max_id_seq_no
	from Finance.mobile_trn_recon 
), std_bank_airtel_sweeps as (
	select *
	from Zoho.Historic_Cashflows_Report a
	where a.Account_Paid_From in ('Yellow Airtel Merchant Account')
		and a.Account_Paid_Into = 'Yellow Standard Bank Account'
), daily_agg as (
-- recon summary per day
select 
	r.provider Provider
	, r.provider_acc_no AccountNo
	, r.trn_date TrnDate
	, s.sweep_amount SweepAmount
    , sum(StmntPmt) StmntPmt
    , sum(StmntPmtAmnt) StmntPmtAmnt
	, sum(MatchedPmt) MatchedPmts
    , sum(MatchedPmtAmt) MatchedPmtAmt
    , sum(UnmatchedManualPmt) UnmatchedManualPmt
    , sum(UnmatchedManualPmtAmt) UnmatchedManualPmtAmt
    , sum(MissingStmntPmt) MissingStmntPmt
    , sum(MissingStmntPmtAmt) MissingStmntPmtAmt
    , sum(UnmatchedReversal) UnmatchedReversal
    , sum(UnmatchedReversalAmt) UnmatchedReversalAmt
    , sum(ToProcessPmt) ToProcessPmt
    , sum(ToProcessPmtAmt) ToProcessPmtAmt
    , sum(UnassignedPmt) UnassignedPmt
    , sum(UnassignedPmtAmt) UnassignedPmtAmt
from Finance.mobile_trn_recon r
left join (
	select m.trn_timestamp, sum(trn_amount) sweep_amount
	from Finance.mobile m
	where m.provider_id like 'RW%%'
	group by 1
	) s 
	on cast(s.trn_timestamp as date) = r.trn_date
group by 1,2,3,4
 -- add more if you put the provider and account back in 
),
report as (
select d.*
	, dup.trns DuplicatePmts
	, std.Amount_in_MK StdBnkReceived
	,  m.provider_post_bal MobileFinalBalance
	,  sum(d.StmntPmtAmnt) over (partition by d.Provider, d.AccountNo order by d.TrnDate) as CumStmntPmtAmnt
    ,  sum(d.SweepAmount) over (partition by d.Provider, d.AccountNo order by d.TrnDate) as CumSweepAmount
    , m.provider_post_bal 
		+ (sum(d.SweepAmount) over (partition by d.Provider, d.AccountNo order by d.TrnDate))  
        - (sum(d.StmntPmtAmnt) over (partition by d.Provider, d.AccountNo order by d.TrnDate))  
		as ProviderSurplus
from daily_agg d
	left join (select r.provider_id, trn_date, count(*) trns
		from Finance.mobile_trn_recon r
		where r.provider_id is not null
		group by 1,2
		having trns > 1
	) dup on dup.trn_date =  d.TrnDate
    
left join max_seq m 
	on m.trn_date = d.TrnDate
    and m.max_id_seq_no = m.id_seq_no

left join std_bank_airtel_sweeps std
	on std.Trn_Date = d.TrnDate
    and d.Provider = 'Airtel Malawi'
) 
select rp.*
	, case 
		when rp.SweepAmount is not null and rp.StdBnkReceived is null then "BNK NOT YET CAPTURED" 
		when rp.SweepAmount <> rp.StdBnkReceived then "SWEEP NOT RECONCILED" 
		when rp.SweepAmount is null and rp.StdBnkReceived is null then "NO SWEEP" 
		else "" end SweepRecon
	-- place other summary columns here
from report rp
order by rp.TrnDate desc
	, rp.Provider
;
