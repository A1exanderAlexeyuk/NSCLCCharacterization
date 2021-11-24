WITH init_data AS (
                  SELECT cohort_definition_id,  line_of_therapy,
                  case when Time_to_Treatment_Discontinuation
                  IS NOT NULL then 1
                  else 0 end  as event,
                  Time_to_Treatment_Discontinuation as time_to_event
                  FROM @cohortDatabaseSchema.@regimenStatsTable
                  WHERE line_of_therapy < 3
                  AND cohort_definition_id IN (@targetId)
                  )


                  SELECT
                  ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id,
                         line_of_therapy ORDER BY time_to_event) AS row_number,
                         cohort_definition_id,
                         line_of_therapy,
                         event,
                         time_to_event
                  FROM init_data;
