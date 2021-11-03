with init_data AS  (SELECT t.subject_id,
                      	 t.cohort_start_date,
                      	 coalesce(min(o.cohort_start_date), max(t.cohort_end_date)) AS event_date,
                      	 CASE WHEN min(o.cohort_start_date) IS NULL THEN 0 ELSE 1 END AS event
                      FROM @cohort_database_schema.@cohort_table t
                      LEFT JOIN @cohort_database_schema.@cohort_table o
                        ON t.subject_id = o.subject_id
                      	  AND o.cohort_start_date >= t.cohort_start_date
                      	  AND o.cohort_start_date <= t.cohort_end_date
                      	  AND o.cohort_definition_id = @outcome_id
                      WHERE t.cohort_definition_id = @target_id
                      GROUP BY t.subject_id, t.cohort_start_date),

     details   AS (
                  SELECT cohort_definition_id,
                         value,
                         ROW_NUMBER() OVER (PARTITION BY
                         cohort_definition_id ORDER BY value) AS row_number,
                         SUM(1) OVER (PARTITION BY cohort_definition_id) AS total
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
       ROUND (MAX(value),1) AS maximum
       'TimeToTreatmentInitiation' AS analysis_name

FROM quartiles
GROUP BY 1;
