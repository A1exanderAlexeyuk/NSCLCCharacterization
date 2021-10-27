with ctt as (select r1.person_id, r1.regimen_start_date, r1.regimen_end_date,  r1.regimen,
			  min(r2.regimen_start_date) as regimen_start_date_new,
			  max(r2.regimen_end_date) as regimen_end_date_new
			  from regimeningredienttable r1
			  left join regimeningredienttable r2 on r1.person_id = r2.person_id 
			 AND r2.regimen_start_date <= r1.regimen_start_date
			 AND r2.regimen_end_date <=
			 r1.regimen_end_date
			  group by r1.person_id, r1.regimen_start_date, r1.regimen_end_date, r1.regimen
			  order by 1,2,3,4),
			  
cte as (select *, 
coalesce(lag(regimen, 1) over (order by person_id) != regimen, TRUE) as New_regimen
from ctt),

sss as (select cte.person_id, cte.regimen_start_date, cte.regimen_end_date, cte.regimen, 
coalesce(lag(cte.regimen, 1) over (order by cte.person_id) != cte.regimen, TRUE) as New_regimen,
ctt.regimen_start_date_new  ,ctt.regimen_end_date_new 
--row_number() over (PARTITION BY  New_regimen,person_id
--Order by person_id, regimen_start_date) as Line_of_therapy  f
from cte   join ctt
ON cte.person_id = ctt.person_id 
AND cte.regimen = ctt.regimen 
AND cte.regimen_start_date = ctt.regimen_start_date_new 
AND cte.regimen_end_date <= ctt.regimen_end_date_new 
order by 1,2,3)

select person_id, regimen, regimen_start_date_new as regimen_start_date, regimen_end_date_new as regimen_end_date,
row_number() over (PARTITION BY  New_regimen,person_id
Order by person_id, regimen_start_date) as Line_of_therapy
from sss where New_regimen != false 
