DROP TABLE IF
EXISTS @cohortDatabaseSchema.@lab_values_table

CREATE table @cohortDatabaseSchema.@lab_values_table (
             cohort_definition_id int,
             person_id bigint,
             measurement_id bigint,
             measurement_date date,
             cohort_start_date date,
             value_as_number int,
             unit_concept_id int

);

INSERT INTO @cohortDatabaseSchema.@lab_values_table (
             cohort_definition_id ,
             person_id ,
             measurement_id ,
             measurement_date ,
             cohort_start_date ,
             value_as_number ,
             unit_concept_id

)
                  SELECT cohort.cohort_definition_id,
                  cohort.subject_id person_id,
                  m.measurement_id,
                  value_as_number,
                  cohort_start_date,
                  m.measurement_date,
                  m.unit_concept_id
                  FROM @cohortDatabaseSchema.@cohortTable cohort
                  JOIN @cdmDatabaseSchema.measurement m
                      ON cohort.subject_id = m.person_id
                  WHERE cohort_definition_id IN (@target_ids)
                    AND abs(datediff(day, cohort.cohort_start_date,
                    m.measurement_date)) <= 10
