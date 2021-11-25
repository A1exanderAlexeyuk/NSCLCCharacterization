DROP TABLE IF
EXISTS @cohortDatabaseSchema.@regimenStatsTable;

with temp_ as (select DISTINCT c.cohort_definition_id,
          c.subject_id as person_id_,
            c.cohort_start_date, c.cohort_end_date,
            op.observation_period_end_date,
              d.death_date, r.*
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
          ORDER BY c.cohort_definition_id, c.subject_id, r.regimen_start_date),


temp_0 as(
        select distinct  cohort_definition_id, person_id_ as person_id, cohort_start_date, regimen_start_date,
          coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
          death_date) as  regimen_end_date,
          regimen, observation_period_end_date, death_date , cohort_end_date,
          ingredient_end_date, ingredient_start_date
        	from temp_ ORDER BY 1,2,3,4
),


t2 as (
    select cohort_definition_id, person_id,
    	 max(ingredient_end_date)  regimen_end_date,
    	 regimen, regimen_start_date,
    	 ingredient_start_date, death_date,
    	 cohort_start_date
    	 from temp_0
    	group by cohort_definition_id, person_id,
    	cohort_start_date, death_date, regimen,
    	regimen_start_date,ingredient_start_date
    	order by 1,2,5
	),

t3 as (
	select *,
	coalesce(lag(regimen, 1) over
	(order by person_id, regimen_start_date) != regimen, True) as New_regimen
		from t2
	group by cohort_definition_id, person_id,
	 death_date, regimen, regimen_start_date,
	 ingredient_start_date,	 regimen_end_date,
	 cohort_start_date
	order by 2,5
	),

t4 as (
  select *,
	case when New_regimen = True then
	 row_number() over (PARTITION BY  person_id, cohort_definition_id, New_regimen
     ORDER BY cohort_definition_id, person_id, regimen_start_date)
     end as Line_of_therapy
from t3
 order by 2,5
 ),

t5 as (
	select  cohort_definition_id, person_id,
	regimen_start_date,regimen_end_date, death_date, regimen,
	count(Line_of_therapy) over
	(partition by person_id order by regimen_start_date)
	as Line_of_therapy,
	cohort_start_date
	from t4
	order by 2,3
),

temp_2 as (select   cohort_definition_id,
	person_id,Line_of_therapy,regimen,
	min(regimen_start_date) over
	(partition by cohort_definition_id, person_id, Line_of_therapy)
	as regimen_start_date,
	max(regimen_end_date)
	over (partition by
	cohort_definition_id, person_id, Line_of_therapy) as  regimen_end_date,
	cohort_start_date
	from t5
	order by 2,5),

temp_3 as (SELECT cohort_definition_id,
       person_id,
       Line_of_therapy,
       regimen,
       regimen_start_date,
       regimen_end_date,

	   case when lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
      	person_id order by person_id) - regimen_end_date <= 0 then NULL
	   else lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
      	person_id order by person_id) - regimen_end_date end
			            as Treatment_free_Interval,

	CASE when lead(regimen_start_date, 1) over (PARTITION BY
	               cohort_definition_id,	person_id
	               order by cohort_definition_id,
							   person_id) - regimen_start_date >= @gapBetweenTreatment
							   OR lead(regimen_start_date, 1) over (PARTITION BY
							   cohort_definition_id,person_id
							   order by cohort_definition_id,person_id) IS NULL
							   then abs(regimen_start_date - regimen_end_date)
							   end as Time_to_Treatment_Discontinuation,

	CASE when Line_of_therapy = 1 AND
	  lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
	order by cohort_definition_id, person_id) IS NOT NULL AND
	lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
	  order by cohort_definition_id, person_id) - regimen_start_date > 0
	   then lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
	  order by cohort_definition_id, person_id) - regimen_start_date
		end
		as Time_to_Next_Treatment

from temp_2
order by  1,2,3,5)


select *
INTO @cohortDatabaseSchema.@regimenStatsTable
from temp_3 order by 1,2,3,5

