-- Finance report
-- drop table if exists Finance.mobile_trn_recon_summary;
-- create table Finance.mobile_trn_recon_summary as 
with max_seq as (
-- max seq trn of the day summary 
select *	
	, min(id_seq_no) over (partition by trn_date) min_id_seq_no
	, max(id_seq_no) over (partition by trn_date) max_id_seq_no
	from Finance.mobile_trn_recon 
), std_bank_airtel_sweeps as (
	select *
	from Zoho.Historic_Cashflows_Report a
	where a.Account_Paid_From in ('Yellow Airtel Merchant Account')
		and a.Account_Paid_Into = 'Yellow Standard Bank Account'
), daily_agg as (
-- recon summary per day
select 
	r.trn_date TrnDate
    , DATE_FORMAT(r.trn_date, '%Y%m') trn_monthkey
    , sum(StmntPmt) StmntPmt
    , sum(StmntPmtAmnt) StmntPmtAmnt
	, sum(MatchedPmt) MatchedPmt
    , sum(MatchedPmtAmt) MatchedPmtAmt
    , sum(UnmatchedManualPmt) UnmatchedManualPmt 
    , sum(UnmatchedManualPmtAmt) UnmatchedManualPmtAmt
    , sum(MissingStmntPmt) MissingStmntPmt
    , sum(MissingStmntPmtAmt) MissingStmntPmtAmt
    , sum(dup.DupsRecorded) DupPmts
    , sum(UnmatchedReversal) UnmatchedReversal
    , sum(UnmatchedReversalAmt) UnmatchedReversalAmt
    , sum(ToProcessPmt) PaymentsToProcess
    , sum(ToProcessPmtAmt) PaymentsToProcessAmt
    , sum(UnassignedPmt) UnassignedPmt
    , sum(UnassignedPmtAmt) UnassignedPmtAmt
   
from Finance.mobile_trn_recon r

left join (select distinct x.provider_id
		, max(recorded_utc) max_recorded_utc
		, cast(max(recorded_utc) as date) DupDateRecorded
		, count(*)-1 DupsRecorded
		from Finance.mobile_trn_recon r
			join (
				select r.provider_id, trn_date, count(*) trns
				from Finance.mobile_trn_recon r
				where r.provider_id is not null
				group by 1,2
				having trns > 1
			) x on r.provider_id =  x.provider_id
	group by 1 ) dup 
		on dup.DupDateRecorded = r.trn_date
        and dup.provider_id = r.provider_id
        and dup.max_recorded_utc = r.recorded_utc

group by 1,2
 -- add more if you put the provider and account back in 
),
report as (
select d.*
	, dup.trns DuplicatePmts
    , s.sweep_amount SweepAmount 
	, std.Amount_in_MK StdBnkReceived
	,  m.provider_post_bal MobileFinalBalance
	,  sum(d.StmntPmtAmnt) over (partition by d.trn_monthkey order by d.TrnDate) as MonthPmtAmnt
    , m.provider_post_bal 
		+ (sum(s.sweep_amount) over (order by d.TrnDate))  
        - (sum(d.StmntPmtAmnt) over (order by d.TrnDate))  
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

left join (
	select m.trn_timestamp, sum(trn_amount) sweep_amount
	from Finance.mobile m
	where m.provider_id like 'RW%'
	group by 1
	) s 
	on cast(s.trn_timestamp as date) = d.TrnDate
) 
select rp.*
	, case 
		when rp.SweepAmount is not null and rp.StdBnkReceived is null then "BNK NOT YET CAPTURED" 
		when rp.SweepAmount <> rp.StdBnkReceived then "SWEEP NOT RECONCILED" 
		when rp.SweepAmount is null and rp.StdBnkReceived is null then "NO SWEEP" 
		else "" end SweepRecon
	, case when PaymentsToProcess=0 and UnmatchedManualPmt=0 and UnmatchedReversal=0 and DupPmts is null and MissingStmntPmt=0 then "OK" 
		when MissingStmntPmt > 0 then "ISSUE - DAYS STATEMENT NOT COMPLETE"
		when PaymentsToProcess > 0 then 'ISSUE - PAYMENTS TO PROCESS'
        else "ISSUE - TO INVESTIGATE UNRECONCILED PAYMENTS" end TrnRecon 
	, PaymentsToProcess AngazaPaymentsToProcess
    , MissingStmntPmt HasMissingMobileMoneyTrns
	, UnmatchedManualPmt + UnmatchedReversal HasUnmatchedPayments
	, case when DupPmts > 0  then 'USERS HAVE CREATED DUPLICATE PAYMENTS' else '' end HasDuplicate
	-- place other summary columns here
from report rp
where rp.TrnDate < curdate()
order by rp.TrnDate desc
;

