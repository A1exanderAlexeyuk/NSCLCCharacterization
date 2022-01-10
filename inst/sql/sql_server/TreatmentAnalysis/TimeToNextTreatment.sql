WITH init_data AS (
                  SELECT cohort_definition_id,
                  CASE WHEN
                  Time_to_Next_Treatment IS NOT NULL
                  then 1 else 0 end
                  as event,
                  Time_to_Next_Treatment as time_to_event,
                  Line_of_therapy
                  FROM @cohortDatabaseSchema.@regimenStatsTable
                  WHERE cohort_definition_id IN (@targetId)
                  AND Line_of_therapy = 1
                  )


                  SELECT ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id ORDER BY time_to_event) AS row_number,
                         cohort_definition_id,
                         event,
                         time_to_event
                  FROM init_data
                  ;
