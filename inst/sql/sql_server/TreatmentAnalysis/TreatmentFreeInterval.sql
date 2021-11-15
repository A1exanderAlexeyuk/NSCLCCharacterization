WITH init_data AS (
                  SELECT cohort_definition_id,
                  line_of_therapy,
                  case when Treatment_free_Interval IS NULL then 0
                  else 1 end as event,
                  Treatment_free_Interval as time_to_event
                  FROM @cohortDatabaseSchema.@regimenStatsTable
                  WHERE cohort_definition_id IN (@targetId)
                  AND Treatment_free_Interval IS NOT NULL
                  )

                  SELECT ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id,line_of_therapy ORDER BY
                         time_to_event) AS row_number,
                         cohort_definition_id,
                         line_of_therapy,
                         event,
                         time_to_event

                  FROM init_data;

