WITH init_data AS (
                  SELECT cohort_definition_id,
                  line_of_therapy,
                  Treatment_free_Interval as value
                  FROM @cohortDatabaseSchema.@regimenStatsTable
                  WHERE cohort_definition_id IN (@targetId)
                  AND Treatment_free_Interval IS NOT NULL
                  ),

details as       (
                  SELECT ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id, line_of_therapy) AS row_number,
                         cohort_definition_id,
                         SUM(1) OVER (PARTITION BY cohort_definition_id,
                         line_of_therapy) AS total,
                         line_of_therapy,
                         value
                  FROM init_data
                  ),

quartiles AS (
                  SELECT cohort_definition_id,
                         value,
                         line_of_therapy,
                         AVG(CASE
                                 WHEN row_number >= (FLOOR(total / 2.0) / 2.0)
                                     AND row_number <= (FLOOR(total / 2.0) / 2.0) + 1
                                     THEN value / 1.0
                             END
                             ) OVER (PARTITION BY cohort_definition_id, line_of_therapy) AS q1,
                         AVG(CASE
                                 WHEN row_number >= (total / 2.0)
                                     AND row_number <= (total / 2.0) + 1
                                     THEN value / 1.0
                             END
                             ) OVER (PARTITION BY cohort_definition_id, line_of_therapy) AS median,
                         AVG(CASE
                                 WHEN row_number >= (CEIL(total / 2.0) + (FLOOR(total / 2.0) / 2.0))
                                     AND row_number <= (CEIL(total / 2.0) + (FLOOR(total / 2.0) / 2.0) + 1)
                                     THEN value / 1.0
                             END
                             ) OVER (PARTITION BY cohort_definition_id, line_of_therapy) AS q3

                  FROM details

                  )


SELECT
       cohort_definition_id,
       line_of_therapy,
       ROUND (AVG(q3) - AVG(q1),1) AS IQR,
       ROUND (MIN(value),1) AS minimum,
       ROUND (AVG(q1),1) AS q1,
       ROUND (AVG(median),1) AS median,
       ROUND (AVG(q3),1) AS q3,
       ROUND (MAX(value),1) AS maximum,
     'Treatment Free Interval' AS analysis_name,
       '@databaseId' AS databaseId

FROM quartiles
GROUP BY 1, 2;
