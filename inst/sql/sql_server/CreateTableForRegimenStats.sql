--here i joined requeried tables (cohort, death, observation, person)
-- to collect data for future analysis
with cte as (select DISTINCT c.cohort_definition_id, c.subject_id as person_id, 
            c.cohort_start_date, c.cohort_end_date,
            op.observation_period_end_date, 
            d.death_date, concept.concept_name as gender, p.year_of_birth,
            regimen_start_date, regimen_end_date, regimen 
			  FROM @cohort_database_schema.@regimeningredienttable r
        LEFT JOIN @cohortDatabaseSchema.@regimenIngredientsTable r 
          on r.person_id = c.subject_id 
          and r.regimen_start_date >= DATEADD(day, -14, c.cohort_start_date)
          and r.regimen_end_date >= c.cohort_start_date
          and r.regimen_start_date <= c.cohort_end_date
        LEFT JOIN @cdmDatabaseSchema.observation_period op
          on op.person_id = c.subject_id
          and op.observation_period_start_date <= c.cohort_start_date
          and op.observation_period_end_date >= c.cohort_end_date
        LEFT JOIN @cdmDatabaseSchema.@deathTable d on d.person_id = c.subject_id
        LEFT JOIN @cdmDatabaseSchema.person p on c.subject_id = p.person_id
        LEFT JOIN @cdmDatabaseSchema.concept on concept.concept_id = p.gender_concept_id
        ORDER BY c.cohort_definition_id, c.subject_id, r.regimen_start_date),

-- I need to be sure that I will have an ancor "regimen_end_date"
--because I will not be able to make correct joins

/*with temp_0 as (
                	select person_id, regimen_start_date, 
                        coalesce(regimen_end_date,observation_end_date,
                        death_date) as  regimen_end_date,
                        regimen, observation_end_date, death_date
                	from @cohort_database_schema.@regimenTable*/
			
)
temp_0 as (select person_id, regimen_start_date, 
        coalesce(regimen_end_date, regimen_start_date) as  --should come up with the place!!!
       	 regimen_end_date, regimen, cohort_definition_id, cohort_start_date, 
        cohort_end_date, death_date, observation_period_end_date, gender,
        year_of_birth FROM cte),

/*ctt as (select r1.person_id, r1.regimen_start_date, r1.regimen_end_date,  r1.regimen,
      			  min(r2.regimen_start_date) as regimen_start_date_new,
      			  max(r2.regimen_end_date) as regimen_end_date_new
      			  from temp_0 r1
      			  left join temp_0 r2 on r1.person_id = r2.person_id 
      		 		AND r2.regimen_start_date <= r1.regimen_start_date
      			 AND r2.regimen_end_date <=	r1.regimen_end_date
			  group by r1.person_id, r1.regimen_start_date, r1.regimen_end_date, r1.regimen
		
		--,r1.observation_end_date, r1.death_date
			  order by 1,2,3,4), */
			  
temp_1 as 	(select   r1.person_id, r1.regimen_start_date, 
         		  r1.regimen_end_date, r1.regimen, 
  	  		  min(r2.regimen_start_date) as regimen_start_date_new,
  			  max(r2.regimen_end_date) as regimen_end_date_new, 
  			  r1.cohort_definition_id, r1.death_date,  r1.observation_period_end_date, 
  			  r1.gender, r1.year_of_birth, r1.cohort_start_date
  			 FROM cte r1
  			  left join cte r2 on r1.person_id = r2.person_id 
  			 AND r2.regimen_start_date <= r1.regimen_start_date
  			 AND r2.regimen_end_date <= r1.regimen_end_date
  			  group by r1.person_id, r1.regimen_start_date, r1.regimen_end_date, 
  			  r1.regimen,  r1.cohort_definition_id, r1.death_date,   
  			  r1.observation_period_end_date, r1.gender, r1.year_of_birth, r1.cohort_start_date
  			  order by 5,1,2,3,7),
			  

/* sss as (select cte.person_id, cte.regimen_start_date, cte.regimen_end_date, cte.regimen, 
            coalesce(lag(cte.regimen, 1) over (PARTITION BY cte.person_id order by cte.person_id) != cte.regimen, TRUE) as New_regimen,
            ctt.regimen_start_date_new  ,ctt.regimen_end_date_new 
            from ctt cte join ctt
            ON cte.person_id = ctt.person_id 
            AND cte.regimen = ctt.regimen 
            AND cte.regimen_end_date <= ctt.regimen_end_date_new 
        order by 1,2,3) */
	
temp_2 as (select temp_1.cohort_definition_id, temp_1.person_id, 
           temp_1.regimen_start_date, temp_1.regimen_end_date,
           temp_1.regimen,  temp_1.death_date,temp_1.observation_period_end_date, 
           coalesce(lag(temp_1.regimen, 1) over 
                    (order by temp_1.person_id) != temp_1.regimen, TRUE) as New_regimen,
           temp_1.regimen_start_date_new  ,temp_1.regimen_end_date_new,
           temp_1.gender, temp_1.year_of_birth, temp_1.cohort_start_date
           FROM temp_1   join temp_1 t
           ON temp_1.person_id = t.person_id 
           AND temp_1.regimen = t.regimen 
           AND temp_1.regimen_end_date <= t.regimen_end_date_new 
           order by 1,2,3),

/*select person_id, regimen, case when regimen_start_date > regimen_start_date_new
    then regimen_start_date
    else regimen_start_date_new end  as regimen_start_date,
    regimen_end_date_new as regimen_end_date,
    row_number() over (PARTITION BY  New_regimen,person_id
    Order by person_id, regimen_start_date) as Line_of_therapy
from sss where 
    New_regimen != false */
    
temp_3 as (
      select cohort_definition_id, person_id, regimen, 
      case when regimen_start_date > regimen_start_date_new
    	then regimen_start_date
    	else regimen_start_date_new end  as regimen_start_date,
      regimen_end_date_new as regimen_end_date,
      row_number() over (PARTITION BY  person_id
      Order by person_id, regimen_start_date) as Line_of_therapy,
       gender,year_of_birth,	death_date, observation_period_end_date,
	cohort_start_date
      FROM temp_2 where New_regimen != false
      order by cohort_definition_id, 2, 4, 6)


DROP TABLE IF  EXISTS @cohort_database_schema.@regimenandlines
CREATE TABLE  @cohort_database_schema.@regimenandlines

( 
    cohort_definition_id int,
    cohort_start_date date,
    person_id bigint,
    regimen text,
    regimen_start_date date,
    regimen_end_date date,
    Line_of_therapy int,
    gender VARCHAR(50), 
    year_of_birth int,
    observation_period_end_date date,
    death_date date

)
SELECT * FROM temp_4 INTO @cohort_database_schema.@regimenAndLines;
