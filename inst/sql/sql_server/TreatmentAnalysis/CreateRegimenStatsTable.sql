DROP TABLE IF
EXISTS @cohortDatabaseSchema.@regimenStatsTable;

CREATE table @cohortDatabaseSchema.@regimenStatsTable (
             cohort_definition_id int,
             person_id bigint,
             Line_of_therapy int,
             regimen text,
             regimen_start_date date,
             regimen_end_date date,
             Treatment_free_Interval int,
             Time_to_Treatment_Discontinuation int,
             Time_to_Next_Treatment int
);

INSERT INTO @cohortDatabaseSchema.@regimenStatsTable (
             cohort_definition_id,
             person_id,
             Line_of_therapy,
             regimen,
             regimen_start_date,
             regimen_end_date,
             Treatment_free_Interval,
             Time_to_Treatment_Discontinuation,
             Time_to_Next_Treatment
)

with temp_ as (SELECT DISTINCT c.cohort_definition_id,
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
        SELECT  cohort_definition_id, person_id_ as person_id, cohort_start_date, regimen_start_date,
          coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
          death_date) as  regimen_end_date,
          regimen, observation_period_end_date, death_date , cohort_end_date,
          ingredient_end_date, ingredient_start_date
        	FROM temp_ ORDER BY 1,2,3,4
),


temp_1 as (
          SELECT cohort_definition_id, person_id,
        	 max(ingredient_end_date)  regimen_end_date,
        	 regimen, regimen_start_date,
        	 ingredient_start_date, death_date,
        	 cohort_start_date
        	 FROM temp_0
        	group by cohort_definition_id, person_id,
        	cohort_start_date, death_date, regimen,
        	regimen_start_date,ingredient_start_date
        	order by 1,2,5
	),


temp_2 as (
      	SELECT distinct cohort_definition_id, person_id,
        	regimen_end_date,
        	 regimen, regimen_start_date,
        	 death_date,    	 cohort_start_date,
      	coalesce(lag(regimen, 1) over
      	(PARTITION BY cohort_definition_id, person_id, regimen_start_date
      	order by person_id, regimen_start_date) != regimen, True) as New_regimen
      		FROM temp_1
      	group by cohort_definition_id, person_id,
      	 death_date, regimen, regimen_start_date,
      	 regimen_end_date,
      	 cohort_start_date
      	order by 2,5
	),


temp_3 as (
          SELECT *,
        	case WHEN New_regimen = True then
        	 row_number() over (PARTITION BY  person_id, cohort_definition_id, New_regimen
         ORDER BY cohort_definition_id, person_id, regimen_start_date)
         end as Line_of_therapy
FROM temp_2
      ORDER BY 2,5
 ),

temp_4 as (
      	SELECT  cohort_definition_id, person_id,
      	regimen_start_date,regimen_end_date, death_date, regimen,
      	count(Line_of_therapy) over
      	(partition by cohort_definition_id, person_id order by regimen_start_date)
      	as Line_of_therapy,
      	cohort_start_date
      	FROM temp_3
      	order by cohort_definition_id, person_id, regimen_start_date
),

temp_5 as (SELECT  distinct cohort_definition_id,
      	person_id,
      	CASE WHEN Line_of_therapy = 0 then 1
      	else Line_of_therapy end as Line_of_therapy
      	,regimen,
      	min(regimen_start_date) over
      	(partition by cohort_definition_id, person_id, Line_of_therapy)
      	AS regimen_start_date,
      	max(regimen_end_date)
      	over (partition by
      	cohort_definition_id, person_id, Line_of_therapy) as  regimen_end_date,
      	cohort_start_date
      	FROM temp_4
      	order by cohort_definition_id, person_id, regimen_start_date),

temp_6 as (
    SELECT DISTINCT
             cohort_definition_id,
             person_id,
             Line_of_therapy,
             regimen,
             regimen_start_date,
             regimen_end_date
    FROM temp_5
),

temp_7 as (
          SELECT cohort_definition_id,
             person_id,
             Line_of_therapy,
             regimen,
             regimen_start_date,
             regimen_end_date,

      	   case WHEN lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
            	        person_id order by person_id) - regimen_end_date < 1 then NULL
      	              else lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
            	        person_id order by person_id) - regimen_end_date end
      			          as Treatment_free_Interval,


      	CASE WHEN lead(regimen_start_date, 1) over (PARTITION BY
      	               cohort_definition_id,	person_id
      	               order by cohort_definition_id,
      							   person_id) - regimen_start_date >= @gapBetweenTreatment
      							   OR lead(regimen_start_date, 1) over (PARTITION BY
      							   cohort_definition_id,person_id
      							   order by cohort_definition_id,person_id) IS NULL
      							   then abs(regimen_start_date - regimen_end_date)
      							   end as Time_to_Treatment_Discontinuation,

      	CASE WHEN Line_of_therapy = 1 AND
                    	lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
                    	order by cohort_definition_id, person_id) IS NOT NULL AND
                    	lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
                    	order by cohort_definition_id, person_id) - regimen_start_date > 0
                    	then lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
                    	order by cohort_definition_id, person_id) - regimen_start_date
                      end
                    	as Time_to_Next_Treatment
      FROM temp_6
      order by  cohort_definition_id, person_id, regimen_start_date, Line_of_therapy)

SELECT *
FROM temp_7
order by 1,2,3,5

