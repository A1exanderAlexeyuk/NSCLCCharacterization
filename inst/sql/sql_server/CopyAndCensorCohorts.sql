IF OBJECT_ID('@cohort_database_schema.@cohort_table', 'U') IS NOT NULL
	DROP TABLE @cohort_database_schema.@cohort_table;

CREATE TABLE @cohort_database_schema.@cohort_table (
	cohort_definition_id INT,
	subject_id BIGINT,
	cohort_start_date DATE,
	cohort_end_date DATE
);

--summarize counts of cohorts so we can filter to those that are feasible
select cohort_definition_id, count(distinct subject_id) as num_persons
into #cohort_summary
from @cohort_database_schema.@cohort_staging_table
group by cohort_definition_id
;
