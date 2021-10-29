with temp_0 as (
                	select person_id, regimen_start_date, 
                        coalesce(regimen_end_date,observation_end_date,
                        death_date) as  regimen_end_date,
                        regimen, observation_end_date, death_date
                	from @cohort_database_schema.@regimenTable
),

ctt as (select r1.person_id, r1.regimen_start_date, r1.regimen_end_date,  r1.regimen,
      			  min(r2.regimen_start_date) as regimen_start_date_new,
      			  max(r2.regimen_end_date) as regimen_end_date_new
      			  from temp_0 r1
      			  left join temp_0 r2 on r1.person_id = r2.person_id 
      		 		AND r2.regimen_start_date <= r1.regimen_start_date
      			 AND r2.regimen_end_date <=	 r1.regimen_end_date
			  group by r1.person_id, r1.regimen_start_date, r1.regimen_end_date, r1.regimen
		
		--,r1.observation_end_date, r1.death_date
			  order by 1,2,3,4),

sss as (select cte.person_id, cte.regimen_start_date, cte.regimen_end_date, cte.regimen, 
            coalesce(lag(cte.regimen, 1) over (PARTITION BY cte.person_id order by cte.person_id) != cte.regimen, TRUE) as New_regimen,
            ctt.regimen_start_date_new  ,ctt.regimen_end_date_new 
            from ctt cte join ctt
            ON cte.person_id = ctt.person_id 
            AND cte.regimen = ctt.regimen 
            AND cte.regimen_end_date <= ctt.regimen_end_date_new 
        order by 1,2,3)


select person_id, regimen, case when regimen_start_date > regimen_start_date_new
    then regimen_start_date
    else regimen_start_date_new end  as regimen_start_date,
    regimen_end_date_new as regimen_end_date,
    row_number() over (PARTITION BY  New_regimen,person_id
    Order by person_id, regimen_start_date) as Line_of_therapy
from sss where 
    New_regimen != false 
