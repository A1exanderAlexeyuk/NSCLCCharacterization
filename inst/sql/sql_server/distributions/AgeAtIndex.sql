WITH init_data AS (
                  SELECT cohort_definition_id, age AS value
                  FROM @cohortDatabaseSchema.subject_age
                  ),
