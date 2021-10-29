/*with cte as (select *, 
            --here we check is regimen unique 
                coalesce(lag(regimen, 1) over (order by regimen) != regimen, TRUE) 
                as New_regimen
                from (select  rt.person_id, regimen_start_date, 
                regimen_end_date,  regimen, d.death_date 
                from @cohort_database_schema.@regimeningredienttable rt
                left join @cdm_database_schema.death d
                on rt.person_id = d.person_id
                left join @cohort_database_schema.@cohortTable coht
                on  coht.person_id = rt.person_id
                --consider about date of start cohort and regimen...
                AND coht.cohort_definition_id IN (@targetIds)) as ct),
            
-- if regimen is unique we assign as LoT
cte_lines as (select *, row_number() over (PARTITION BY person_id
                Order by person_id, regimen_start_date) as Line_of_therapy
                from cte where New_regimen != FALSE),

                         --if LoT not the first we can compute interval between lines 
regimen_lines as (select *, case when Line_of_therapy != 1 then 
---check it! if no previous regimen_end_date - use death_date
 -- DATEDIFF(day, coalesce(lag(regimen_end_date, 1) 
 -- over (order by regimen), death_date), regimen_start_date)
 -- !!! didn't think about case when patient died and won't be new start date...
	abs(coalesce(lag(regimen_end_date, 1) over (order by regimen), death_date, 
	             cohort_end_day) - regimen_start_date)
	                                                      
	else 0 end as interval_beetwen_lines
from cte_lines),*/

                         --if LoT not the first we can compute interval between lines 
regimen_lines as (select *, case when Line_of_therapy != 1 then 
---check it! if no previous regimen_end_date - use death_date
 -- DATEDIFF(day, coalesce(lag(regimen_end_date, 1) 
 -- over (order by regimen), death_date), regimen_start_date)
 -- !!! didn't think about case when patient died and won't be new start date...
	abs(coalesce(lag(regimen_end_date, 1) over (order by regimen), death_date, 
	             cohort_end_day) - regimen_start_date)
	                                                      
	else 0 end as interval_beetwen_lines
from cte_lines)



with   details   AS (
                  SELECT cohort_definition_id,
                         person_id,
                         Line_of_therapy,
                         interval_beetwen_lines,
                         ROW_NUMBER() OVER (PARTITION BY 
                         Line_of_therapy ORDER BY interval_beetwen_lines) AS row_number,
                         SUM(1) OVER (PARTITION BY Line_of_therapy) AS total
                  FROM regimen_lines
                  WHERE Line_of_therapy IN (@LinesOfTreatment)
                  ),
     quartiles AS (
                  SELECT Line_of_therapy,
                         interval_beetwen_lines,
                         AVG(CASE
                                 WHEN row_number >= (FLOOR(total / 2.0) / 2.0)
                                     AND row_number <= (FLOOR(total / 2.0) / 2.0) + 1
                                     THEN interval_beetwen_lines / 1.0
                             END
                             ) OVER (PARTITION BY Line_of_therapy) AS q1,
                         AVG(CASE
                                 WHEN row_number >= (total / 2.0)
                                     AND row_number <= (total / 2.0) + 1
                                     THEN interval_beetwen_lines / 1.0
                             END
                             ) OVER (PARTITION BY Line_of_therapy) AS median,
                         AVG(CASE
                                 WHEN row_number >= (CEIL(total / 2.0) + (FLOOR(total / 2.0) / 2.0))
                                     AND row_number <= (CEIL(total / 2.0) + (FLOOR(total / 2.0) / 2.0) + 1)
                                     THEN interval_beetwen_lines / 1.0
                             END
                             ) OVER (PARTITION BY Line_of_therapy) AS q3
                  FROM details
                  )
SELECT cohort_definition_id,
       Line_of_therapy,
       AVG(q3) - AVG(q1) AS IQR,
       MIN(interval_beetwen_lines) AS minimum,
       AVG(q1) AS q1,
       AVG(median) AS median,
       AVG(q3) AS q3,
       MAX(interval_beetwen_lines) AS maximum,
       'days_analysis' AS analysis_name
FROM quartiles
GROUP BY 1, 2
ORDER by 1, 2;
