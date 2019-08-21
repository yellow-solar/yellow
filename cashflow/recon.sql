### Finance report
with max_seq as (
-- max seq trn of the day summary 
select *	
	, min(id_seq_no) over (partition by provider, provider_acc_no order by trn_date) min_id_seq_no
	, max(id_seq_no) over (partition by provider, provider_acc_no order by trn_date) max_id_seq_no
	from Sandbox.mobile_transactions_recon 
), std_bank_sweeps as (
	select *
	from Zoho.Historic_Cashflows_Report a
	where a.Account_Paid_From = 'Yellow Airtel Merchant Account'
		and a.Account_Paid_Into = 'Yellow Standard Bank Account'
), daily_agg as (
-- recon summary per day
select r.provider
	, r.provider_acc_no
	, cast(r.trn_timestamp as date) trn_date
	, s.sweep_amount
	, sum(r.trn_amount) day_trn_amount
	, count(r.provider_id) day_trn_count
	, sum(case when r.angaza_id is null then r.trn_amount else 0 end) unprocessed_trn_amount
	, sum(case when r.angaza_id is null then 1 else 0 end) unprocessed_trn_count
from Sandbox.mobile_transactions_recon r
left join (
	select m.trn_timestamp, sum(trn_amount) sweep_amount
	from Finance.mobile m
	where m.provider_id like 'RW%%'
	group by 1
	) s 
	on cast(s.trn_timestamp as date) = r.trn_date
group by 1,2,3,4
),
report as (
select d.*
	, std.Amount_in_MK std_bnk_received
	,  m.provider_post_bal provider_final_balance
	,  sum(day_trn_amount) over (partition by provider, provider_acc_no order by trn_date) as trn_amount_cum
    ,  sum(sweep_amount) over (partition by provider, provider_acc_no order by trn_date) as sweep_amount_cum
    , m.provider_post_bal 
		+ (sum(sweep_amount) over (partition by provider, provider_acc_no  order by trn_date)) 
        - (sum(day_trn_amount) over (partition by provider, provider_acc_no order by trn_date))  
		as provider_surplus_deficit
from daily_agg d
left join max_seq m 
	on m.trn_date = d.trn_date
    and m.max_id_seq_no = m.id_seq_no
left join std_bank_sweeps std
	on std.Trn_Date = d.trn_date
) 
select rp.provider Provider, 
	rp.provider_acc_no Account, 
	rp.trn_date TrnDate, 
	rp.sweep_amount ProviderSweep, 
	rp.std_bnk_received BankSweep, 
    case 
		when rp.sweep_amount is not null and rp.std_bnk_received is null then "BANK NOT YET CAPTURED" 
		when rp.sweep_amount <> rp.std_bnk_received then "SWEEP NOT RECONCILED" 
		when rp.sweep_amount is null and rp.std_bnk_received is null then "NO SWEEP" 
		else "RECONCILES" end ReconCheck,
	rp.provider_final_balance EndofDayBal, 
	rp.provider_surplus_deficit SurplusOrDeficit, 
/* need to get total for month */
	rp.day_trn_amount TrnAmt, 
	rp.day_trn_count TrnCount, 
	rp.unprocessed_trn_amount ToProcessTrnAmt, 
	rp.unprocessed_trn_count ToProcessTrnCount
from report rp
order by rp.provider
	, rp.trn_date desc
limit 14
;