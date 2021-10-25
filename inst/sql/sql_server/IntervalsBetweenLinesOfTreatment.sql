with cte as (select *, 
            --here we check is regimen unique 
                coalesce(lag(regimen, 1) over (order by regimen) != regimen, TRUE) 
                as New_regimen
            from (select  rt.person_id, regimen_start_date, 
            regimen_end_date,  regimen, d.death_date 
            from @cohort_database_schema@regimeningredienttable rt
            left join cdm_datbase_schema@death d
            on rt.person_id = d.person_id) as ct),
            
-- if regimen is unique we assign as LoT
cte_lines as (select *, row_number() over (PARTITION BY person_id
              Order by person_id, regimen_start_date) as Line_of_therapy
              from cte where New_regimen != FALSE)

--if LoT not the first we can compute interval between lines 
select *, case when Line_of_therapy != 1 then 
---check it! if no previous regimen_end_date - use death_date
 -- DATEDIFF(day, coalesce(lag(regimen_end_date, 1) 
 -- over (order by regimen), death_date), regimen_start_date)
	abs(coalesce(lag(regimen_end_date, 1) over (order by regimen), death_date)
	                                                     - regimen_start_date) 
	else 0 end as interval_beetwen_lines
from cte_lines;
