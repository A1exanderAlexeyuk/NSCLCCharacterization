WITH init_data AS (
                  SELECT cohort_definition_id,
                  line_of_therapy,
                  case when Treatment_free_Interval IN NULL then 0
                  else 1 end as ,
                  Treatment_free_Interval as time_to_event
                  FROM @cohortDatabaseSchema.@regimenStatsTable
                  WHERE cohort_definition_id IN (@targetId)
                  )

                  SELECT ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id,line_of_therapy ORDER BY
                         time_to_event) AS row_number,
                         cohort_definition_id,
                         line_of_therapy,
                         event,
                         time_to_event

                  FROM init_data;

