WITH init_data AS (
                  SELECT cohort_definition_id,
                  line_of_therapy,
                  case when
                  Treatment_free_Interval IS NOT NULL AS value then 1 else 0 end
                  as event,
                  Treatment_free_Interval as time_to_event
                  FROM @cohort_database_schema.@regimenStatsTable
                  AND cohort_definition_id IN (@targetId)
                  )


                  SELECT ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id,line_of_therapy ORDER BY
                         time_to_event) AS row_number,
                         cohort_definition_id,
                         line_of_therapy,
                         event,
                         time_to_event,
                         SUM(1) OVER (PARTITION BY cohort_definition_id,
                         line_of_therapy) AS total
                  FROM init_data
                  WHERE total > 100; -- to make sure that analysis will be valid
