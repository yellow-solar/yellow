select distinct x.provider_id
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
group by 1