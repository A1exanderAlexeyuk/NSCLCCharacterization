with temp_1 as (select r1.person_id, r1.regimen_start_date, r1.regimen_end_date,  r1.regimen,
			  min(r2.regimen_start_date) as regimen_start_date_new,
			  max(r2.regimen_end_date) as regimen_end_date_new
			  from regimeningredienttable r1
			  left join regimeningredienttable r2 on r1.person_id = r2.person_id 
			 AND r2.regimen_start_date <= r1.regimen_start_date
			 AND r2.regimen_end_date <=
			 r1.regimen_end_date
			  group by r1.person_id, r1.regimen_start_date, r1.regimen_end_date, r1.regimen
			  order by 1,2,3,4),
			  
temp_2 as (select *, 
coalesce(lag(regimen, 1) over (order by person_id) != regimen, TRUE) as New_regimen
from temp_1),

temp_3 as (select temp_2.person_id, temp_2.regimen_start_date, temp_2.regimen_end_date, temp_2.regimen, 
coalesce(lag(temp_2.regimen, 1) over (order by temp_2.person_id) != temp_2.regimen, TRUE) as New_regimen,
temp_1.regimen_start_date_new  ,temp_1.regimen_end_date_new 
from temp_2   join temp_1
ON temp_2.person_id = temp_1.person_id 
AND temp_2.regimen = temp_1.regimen 
AND temp_2.regimen_start_date = temp_1.regimen_start_date_new 
AND temp_2.regimen_end_date <= temp_1.regimen_end_date_new 
order by 1,2,3),
temp_31 as (
select person_id, regimen, regimen_start_date_new as regimen_start_date, 
regimen_end_date_new as regimen_end_date,
row_number() over (PARTITION BY  person_id
Order by person_id, regimen_start_date) as Line_of_therapy 
from temp_3 where New_regimen != false)

select *, 
	case when Line_of_therapy != 1 then 
	abs(coalesce(lag(regimen_end_date, 1) over (order by person_id, regimen))
	                                                     - regimen_start_date) 
	else -1 end as interval_beetwen_lines
from temp_31 order by 5

