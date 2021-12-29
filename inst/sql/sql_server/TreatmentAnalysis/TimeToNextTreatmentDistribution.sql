WITH init_data AS (
                  SELECT cohort_definition_id,
                  Time_to_Next_Treatment
                  FROM @cohortDatabaseSchema.@regimenStatsTable
                  WHERE cohort_definition_id IN (@targetId)
                  AND Line_of_therapy = 1
                  ),

details as(
                  SELECT ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id ) AS row_number,
                         SUM(1) OVER (PARTITION BY cohort_definition_id
                         ) AS total,
                         cohort_definition_id,
                         Time_to_Next_Treatment as value
                  FROM init_data
                  ),

quartiles AS (
                  SELECT cohort_definition_id,
                         value,
                         AVG(CASE
                                 WHEN row_number >= (FLOOR(total / 2.0) / 2.0)
                                     AND row_number <= (FLOOR(total / 2.0) / 2.0) + 1
                                     THEN value / 1.0
                             END
                             ) OVER (PARTITION BY cohort_definition_id) AS q1,
                         AVG(CASE
                                 WHEN row_number >= (total / 2.0)
                                     AND row_number <= (total / 2.0) + 1
                                     THEN value / 1.0
                             END
                             ) OVER (PARTITION BY cohort_definition_id) AS median,
                         AVG(CASE
                                 WHEN row_number >= (CEIL(total / 2.0) + (FLOOR(total / 2.0) / 2.0))
                                     AND row_number <= (CEIL(total / 2.0) + (FLOOR(total / 2.0) / 2.0) + 1)
                                     THEN value / 1.0
                             END
                             ) OVER (PARTITION BY cohort_definition_id) AS q3

                  FROM details

                  )


SELECT
       cohort_definition_id,
       ROUND (AVG(q3) - AVG(q1),1) AS IQR,
       ROUND (MIN(value),1) AS minimum,
       ROUND (AVG(q1),1) AS q1,
       ROUND (AVG(median),1) AS median,
       ROUND (AVG(q3),1) AS q3,
       ROUND (MAX(value),1) AS maximum,
     'Time to Next Treatment' AS analysis_name,
       '@databaseId' AS databaseId

FROM quartiles
GROUP BY 1;

