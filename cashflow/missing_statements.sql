-- Airtel missing statement days -- create new for new providers
select distinct cast(x.effective_utc as date) statement_date
	from (
		select *  from Angaza.payments p
		left join Finance.mobile m on p.provider_transaction = m.provider_id
		where p.recorded_utc > '2019-07-11'
			 -- and p.type in ('Manual (Activator)','Manual (Hub)')
             and p.type = 'Airtel Malawi (via GSMA)'
	) x
where x.provider_id is null
    and cast(x.effective_utc as date) < curdate()
;