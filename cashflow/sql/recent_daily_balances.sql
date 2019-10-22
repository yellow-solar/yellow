# Past 14 days
select *
from Finance.All_Acounts_Daily_Movements a
where a.Trn_Date > (curdate() - 14)
;

# All days where there is an actual daily balance
select *
from Finance.All_Acounts_Daily_Movements a
where a.Actual_Balance is not null;

# All days where there is an actual daily balance - for table
select *
from Finance.All_Acounts_Daily_Movements a
where a.Actual_Balance is not null
    and a.Trn_Date > date_sub(curdate(), interval 30 day);