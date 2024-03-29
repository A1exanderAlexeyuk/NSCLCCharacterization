WITH init_data AS (
                      SELECT cohort_definition_id,
                             subject_id AS person_id,
                             cohort_start_date,
                             SUM(weight) AS value
                      FROM @cohortDatabaseSchema.charlson_map
                      GROUP BY cohort_definition_id,
                               subject_id,
                               cohort_start_date
                      ),
