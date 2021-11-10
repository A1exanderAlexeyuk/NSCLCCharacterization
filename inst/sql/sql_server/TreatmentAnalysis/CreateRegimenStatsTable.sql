DROP TABLE IF EXISTS @cohortDatabaseSchema.@regimenStatsTable;
-- create subject regimen_stats_table

--here i joined requeried tables (cohort, death, observation, person)
-- to collect data for future analysis
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
        	select cohort_definition_id, person_id, cohort_start_date, regimen_start_date,
          coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
          death_date ) as  regimen_end_date,
          regimen, observation_period_end_date, death_date
        	FROM temp_ ORDER BY 1,2,3,4
),

temp as (
          select  cohort_definition_id, person_id, min(regimen_start_date) over
          (PARTITION BY person_id, regimen, cohort_definition_id) as regimen_start_date,
          max(regimen_end_date) over  (PARTITION BY person_id, regimen,cohort_definition_id) as regimen_end_date,
                   regimen, observation_period_end_date, death_date, cohort_start_date
          FROM temp_0 ORDER BY 1, 2, 3
),

temp_1 as (
          select cohort_definition_id, person_id, regimen_start_date, regimen_end_date,
          regimen, row_number() over (PARTITION BY  person_id
          ORDER BY cohort_definition_id, person_id, regimen_start_date) as Line_of_therapy,
          observation_period_end_date, death_date, cohort_start_date
          FROM temp ORDER BY 1,2,3,6
),

-- creation a table for analysis


temp_2 as (
SELECT cohort_definition_id,
       person_id,
       Line_of_therapy,
       regimen,
  /*Time from discontinuation of one LoT to initiation of the subsequent LoT,
	or date of death if death occurs prior to start of the subsequent LoT.
	Patients will be censored at their last activity within the database
	or end of follow-up. and test it
	*/
      	abs(lag(regimen_end_date, 1) over (PARTITION BY cohort_definition_id,
      	person_id order by person_id) - regimen_start_date)
			            as Treatment_free_Interval,

	/*Length of time from the initiation of each LoT to the date the patient discontinues
	the treatment (i.e., the last administration or noncancelled order of
	a drug contained in the same regimen). TTD will be described for the
	first two LoTs. Discontinuation will be defined as having a subsequent
	systemic anti-neoplastic therapy regimen after the first LoT; having a
	gap of more than 120 days with no systemic anti-neoplastic therapy following
	the last administration; or having a date of death while on the regimen.
	Patients will be censored at their last known usage within the database
	or end of follow-up
	*/
	CASE when abs(lead(regimen_start_date, 1) over (PARTITION BY
	               cohort_definition_id,	person_id
	               order by cohort_definition_id,
							   person_id) - regimen_start_date) >= 120
							   OR lead(regimen_start_date, 1) over (PARTITION BY
							   cohort_definition_id,person_id
							   order by cohort_definition_id,person_id) IS NULL
							   then abs(regimen_start_date - regimen_end_date)
							   end as Time_to_Treatment_Discontinuation,

	/*Time from the index date to the date the patient received their next systemic
anti-neoplastic treatment regimen or to their date of death if death occurs prior
to having another systemic anti-neoplastic treatment regimen. Patients will be censored
at their last activity within the database or end of follow-up*/

	CASE when Line_of_therapy = 1 AND
	  lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id, person_id
							   order by cohort_definition_id, person_id) IS NOT NULL
							   then abs(lead(regimen_start_date, 1) over (PARTITION BY cohort_definition_id,
							   person_id
							   order by cohort_definition_id, person_id) - cohort_start_date)
		when regimen_end_date = observation_period_end_date
		OR regimen_end_date = death_date
		then abs(lead(regimen_start_date, 1) over
		(PARTITION BY cohort_definition_id,
							   person_id
							   order by cohort_definition_id,
							   person_id) - regimen_end_date)
							   end
							   as Time_to_Next_Treatment

from temp_1 group by cohort_definition_id,
       person_id, Line_of_therapy,
       regimen_start_date,
       regimen_end_date, regimen,
	   cohort_start_date,
	   observation_period_end_date,
	   death_date
       order by 1,2,3,4
)

SELECT *
INTO @cohortDatabaseSchema.@regimenStatsTable
FROM temp_2
