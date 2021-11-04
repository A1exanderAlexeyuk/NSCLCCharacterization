-- create subject regimen_stats_table

--here i joined requeried tables (cohort, death, observation, person)
-- to collect data for future analysis
with temp as (select DISTINCT c.cohort_definition_id, c.subject_id as person_id,
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


with temp_0 as(
        	select cohort_definition_id, person_id, cohort_start_date, regimen_start_date,
          coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
          death_date ) as  regimen_end_date,
          regimen, observation_period_end_date, death_date
        	from temp order by 1,2,3,4
),

temp_1 as (select r1.cohort_definition_id,
          r1.person_id, r1.cohort_start_date, r1.regimen_start_date,
          r1.regimen_end_date,  r1.regimen,
  			  min(r2.regimen_start_date) as regimen_start_date_new,
  			  max(r2.regimen_end_date) as regimen_end_date_new,
  		    r1.observation_period_end_date, r1.death_date
  			  from temp_0 r1
  			  left join temp_0 r2 on r1.person_id = r2.person_id
  		 		AND r2.regimen_start_date <= r1.regimen_start_date
    			AND r2.regimen_end_date <=	 r1.regimen_end_date
  			  group by 1,2,3,4,5,6,9,10
  			  order by 1,2,3,4),

temp_2 as (select cte.cohort_definition_id, cte.person_id, cte.cohort_start_date,
          cte.regimen_start_date, cte.regimen_end_date, cte.regimen,
          coalesce(lag(cte.regimen, 1) over (PARTITION BY cte.cohort_definition_id,
          cte.person_id order by cte.person_id) != cte.regimen, TRUE)
          as New_regimen,
          temp_1.regimen_start_date_new, temp_1.regimen_end_date_new,
          cte.observation_period_end_date, cte.death_date
          from temp_1 cte
          join temp_1
          ON cte.person_id = temp_1.person_id
          AND cte.regimen = temp_1.regimen
          AND cte.regimen_end_date <= temp_1.regimen_end_date_new
          order by 1,3,2,4),

temp_3 as (select cohort_definition_id, person_id, regimen,
            case when regimen_start_date > regimen_start_date_new
            then regimen_start_date
            else regimen_start_date_new end  as regimen_start_date,
            regimen_end_date_new as regimen_end_date,
            row_number() over (PARTITION BY  New_regimen,person_id
            Order by person_id, regimen_start_date) as Line_of_therapy,
            death_date, observation_period_end_date
            from temp_2 where New_regimen != false
            order by cohort_definition_id, regimen_start_date, Line_of_therapy)



-- creation a table for analysis

DROP TABLE IF EXISTS @cohortDatabaseSchema.@regimenStatsTable;
CREATE TABLE @cohortDatabaseSchema.@regimenStatsTable AS

SELECT temp_3.cohort_definition_id,
       temp_3.person_id,
       temp_3.Line_of_therapy,
       temp_3.regimen,
  /*Time from discontinuation of one LoT to initiation of the subsequent LoT,
	or date of death if death occurs prior to start of the subsequent LoT.
	Patients will be censored at their last activity within the database
	or end of follow-up. and test it
	*/
	abs(lag(temp_3.regimen_end_date, 1) over (PARTITION BY temp_3.person_id
			            order by temp_3.person_id) - temp_3.regimen_start_date)
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
	CASE when abs(lead(temp_3.regimen_start_date, 1) over (PARTITION BY
	               temp_3.cohort_definition_id,	temp_3.person_id,
	               order by temp_3.cohort_definition_id,
							   temp_3.person_id) - temp_3.regimen_start_date) >= @gapBetweenTreatment
							   OR lead(temp_3.regimen_start_date, 1) over (PARTITION BY
							   temp_3.cohort_definition_id,temp_3.person_id
							   order by temp_3.cohort_definition_id,temp_3.person_id) IS NULL
							   then abs(temp_3.regimen_start_date - temp_3.regimen_end_date)
							   end as Time_to_Treatment_Discontinuation,

	/*Time from the index date to the date the patient received their next systemic
anti-neoplastic treatment regimen or to their date of death if death occurs prior
to having another systemic anti-neoplastic treatment regimen. Patients will be censored
at their last activity within the database or end of follow-up*/

	CASE when Line_of_therapy = 1 AND
	  lead(regimen_start_date, 1) over (PARTITION BY temp_3.cohort_definition_id,person_id
							   order by temp_3.cohort_definition_id, person_id) IS NOT NULL
							   then abs(lead(regimen_start_date, 1) over (PARTITION BY temp_3.cohort_definition_id,
							   person_id
							   order by temp_3.cohort_definition_id, temp_3.person_id) - temp_3.cohort_start_date)
		when temp_3.regimen_end_date = temp_3.observation_period_end_date
		OR temp_3.regimen_end_date = temp_3.death_date
		then abs(lead(regimen_start_date, 1) over
		(PARTITION BY temp_3.cohort_definition_id,
							   temp_3.person_id
							   order by temp_3.cohort_definition_id,
							   temp_3.person_id) - temp_3.regimen_end_date)
							   end
							   as Time_to_Next_Treatment

from temp_3 group by temp_3.cohort_definition_id,
       temp_3.person_id, temp_3.Line_of_therapy,
       temp_3.regimen_start_date,
       temp_3.regimen_end_date
       order by 1,2,3,4
;
