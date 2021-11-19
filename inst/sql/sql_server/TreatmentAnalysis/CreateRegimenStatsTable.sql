DROP TABLE IF
EXISTS @cohortDatabaseSchema.@regimenStatsTable;

with temp_ as (select DISTINCT c.cohort_definition_id, c.subject_id as person_id,
            c.cohort_start_date, c.cohort_end_date,
            op.observation_period_end_date,
              d.death_date, regimen_start_date, regimen_end_date, regimen
  			  FROM @cohortDatabaseSchema.@cohortTable c
          LEFT JOIN @cohortDatabaseSchema.@regimenIngredientsTable r
            on r.person_id = c.subject_id
            and r.regimen_start_date >= DATEADD(day, -14, c.cohort_start_date)
            and r.regimen_end_date >= c.cohort_start_date
            and r.regimen_start_date <= c.cohort_end_date
          LEFT JOIN @cdmDatabaseSchema.observation_period op
            on op.person_id = c.subject_id
            and op.observation_period_start_date <= c.cohort_start_date
            and op.observation_period_end_date >= c.cohort_end_date
          LEFT JOIN @cdmDatabaseSchema.death d on d.person_id = c.subject_id
          LEFT JOIN @cdmDatabaseSchema.person p on c.subject_id = p.person_id
          ORDER BY c.cohort_definition_id, c.subject_id, r.regimen_start_date),


temp_0 as(
        select  cohort_definition_id, person_id, cohort_start_date, regimen_start_date,
          coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
          death_date ) as  regimen_end_date,
          regimen, observation_period_end_date, death_date
        	from temp_ ORDER BY 1,2,3,4
),

with temp_0 as(
  select  distinct cohort_definition_id, person_id, cohort_start_date, regimen_start_date,
          coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
          death_date ) as  regimen_end_date,
          regimen, observation_period_end_date, death_date,
		coalesce(lag(regimen, 1) over (order by person_id) != regimen, TRUE) as New_regimen
        	from regimen_stats_schema.rst2
			ORDER BY 1,2,3,9 desc, 4

),

temp_t as (
          select  distinct cohort_definition_id, person_id,
	 	min(regimen_start_date) over
          (PARTITION BY person_id, cohort_definition_id, regimen)  as regimen_start_date,
          max(regimen_end_date) over
		  (PARTITION BY person_id, cohort_definition_id,regimen) as regimen_end_date,
           regimen, observation_period_end_date, death_date, cohort_start_date
          FROM temp_0
		ORDER BY 1, 2, 3

),

temp_1 as (
         select  cohort_definition_id, person_id, regimen_start_date, regimen_end_date,
          regimen, row_number() over (PARTITION BY  person_id, cohort_definition_id
          ORDER BY cohort_definition_id, person_id, regimen_start_date) as Line_of_therapy,
          observation_period_end_date, death_date, cohort_start_date
          FROM temp_t ORDER BY 1,2,3,6
),

-- creation a table for analysis



temp_2 as (SELECT cohort_definition_id,
       person_id,
       Line_of_therapy,
       regimen,
       regimen_start_date,
       regimen_end_date,
  	   cohort_start_date,
  	   observation_period_end_date,
	     death_date,
	     sum(Line_of_therapy)

from temp_1
group by cohort_definition_id,
       person_id, Line_of_therapy,
       regimen_start_date,
       regimen_end_date, regimen,
  	   cohort_start_date,
  	   observation_period_end_date,
  	   death_date
       order by 1,2,3,4),

temp_3 as (SELECT cohort_definition_id,
       person_id,
       Line_of_therapy,
       regimen,
       regimen_start_date,
       regimen_end_date,
	   case when lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
      	person_id order by person_id) - regimen_end_date < 0 then NULL
	   else lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
      	person_id order by person_id) - regimen_end_date end
			            as Treatment_free_Interval,

	CASE when lead(regimen_start_date, 1) over (PARTITION BY
	               cohort_definition_id,	person_id
	               order by cohort_definition_id,
							   person_id) - regimen_start_date >= 120
							   OR lead(regimen_start_date, 1) over (PARTITION BY
							   cohort_definition_id,person_id
							   order by cohort_definition_id,person_id) IS NULL
							   then abs(regimen_start_date - regimen_end_date)
							   end as Time_to_Treatment_Discontinuation,

	CASE when Line_of_therapy = 1 AND
	  lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
	order by cohort_definition_id, person_id) IS NOT NULL AND
	lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
	  order by cohort_definition_id, person_id) - cohort_start_date > 0
	   then lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
	  order by cohort_definition_id, person_id) - cohort_start_date
		end
		as Time_to_Next_Treatment
from temp_2
order by  1,2,3, 5)


select * from ww
INTO @cohortDatabaseSchema.@regimenStatsTable
from temp_2 order by 1,2,3,5

