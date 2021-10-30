-- create subject age table
DROP TABLE IF EXISTS @cohort_database_schema.subject_age;
CREATE TABLE @cohort_database_schema.subject_age AS
SELECT tab.cohort_definition_id,
       tab.person_id,
       tab.cohort_start_date,
       DATEDIFF(year, DATEFROMPARTS(tab.year_of_birth, 
       tab.month_of_birth, tab.day_of_birth),
                tab.cohort_start_date) AS age
FROM (
     SELECT c.cohort_definition_id, 
     p.person_id, c.cohort_start_date, 
     p.year_of_birth, p.month_of_birth, 
     p.day_of_birth
     FROM @cohort_database_schema.@cohort_table c
     JOIN @cdm_database_schema.person p
         ON p.person_id = c.subject_id
     WHERE c.cohort_definition_id IN (@target_ids)
     ) tab
;

-- create subject regimen_stats_table

--here i joined requeried tables (cohort, death, observation, person)
-- to collect data for future analysis
with temp as (select DISTINCT c.cohort_definition_id, c.subject_id as person_id, 
            c.cohort_start_date, c.cohort_end_date,
            op.observation_period_end_date, 
            d.death_date, regimen_start_date, regimen_end_date, regimen 
			  FROM @cohortDatabaseSchema.@cohortTable c
        LEFT JOIN @cohortDatabaseSchema.@regimenIngredientsTable r 
          on r.person_id = c.subject_id 
          and r.regimen_start_date >= DATEADD(day, -14, c.cohort_start_date)
          and r.regimen_end_date >= c.cohort_start_date
          and r.regimen_start_date <= c.cohort_end_date
        LEFT JOIN @cdmDatabaseSchema.observation_period op
          on op.person_id = c.subject_id
          and op.observation_period_start_date <= c.cohort_start_date
          and op.observation_period_end_date >= c.cohort_end_date
        LEFT JOIN @cdmDatabaseSchema.@deathTable d on d.person_id = c.subject_id
        LEFT JOIN @cdmDatabaseSchema.person p on c.subject_id = p.person_id
        ORDER BY c.cohort_definition_id, c.subject_id, r.regimen_start_date),
        
        
with temp_0 as(
	select cohort_definition_id, person_id, cohort_start_date, regimen_start_date, 
        coalesce(regimen_end_date, cohort_end_date,observation_period_end_date,
        death_date ) as  regimen_end_date,
        regimen, observation_period_end_date, death_date
	from temp order by 1,2,3,4
),

temp_1 as (select r1.cohort_definition_id, 
          r1.person_id, r1.cohort_start_date, r1.regimen_start_date, 
          r1.regimen_end_date,  r1.regimen,
  			  min(r2.regimen_start_date) as regimen_start_date_new,
  			  max(r2.regimen_end_date) as regimen_end_date_new,
  		    r1.observation_period_end_date, r1.death_date
  			  from temp_0 r1
  			  left join temp_0 r2 on r1.person_id = r2.person_id 
  		 		AND r2.regimen_start_date <= r1.regimen_start_date
    			AND r2.regimen_end_date <=	 r1.regimen_end_date
  			  group by 1,2,3,4,5,6,9,10
  			  order by 1,2,3,4),

temp_2 as (select cte.cohort_definition_id, cte.person_id, cte.cohort_start_date,
          cte.regimen_start_date, cte.regimen_end_date, cte.regimen, 
          coalesce(lag(cte.regimen, 1) over (PARTITION BY cte.cohort_definition_id, 
          cte.person_id order by cte.person_id) != cte.regimen, TRUE) as New_regimen,
          temp_1.regimen_start_date_new, temp_1.regimen_end_date_new, 
          cte.observation_period_end_date, cte.death_date
          from temp_1 cte 
          join temp_1
          ON cte.person_id = temp_1.person_id 
          AND cte.regimen = temp_1.regimen 
          AND cte.regimen_end_date <= temp_1.regimen_end_date_new 
          order by 1,3,2,4),

temp_3 as (select cohort_definition_id, person_id, regimen, 
            case when regimen_start_date > regimen_start_date_new
            then regimen_start_date
            else regimen_start_date_new end  as regimen_start_date,
            regimen_end_date_new as regimen_end_date,
            row_number() over (PARTITION BY  New_regimen,person_id
            Order by person_id, regimen_start_date) as Line_of_therapy, 
            death_date, observation_period_end_date
            from temp_2 where New_regimen != false
            order by cohort_definition_id, regimen_start_date, Line_of_therapy)
            
DROP TABLE IF EXISTS @cohort_database_schema.regimen_stats;
CREATE TABLE @cohort_database_schema.regimen_stats AS

SELECT temp_3.cohort_definition_id,
       temp_3.person_id,
  /*Time from discontinuation of one LoT to initiation of the subsequent LoT, 
	or date of death if death occurs prior to start of the subsequent LoT. 
	Patients will be censored at their last activity within the database 
	or end of follow-up. and test it
	*/
	abs(lag(temp_3.regimen_end_date, 1) over (PARTITION BY temp_3.person_id 
			            order by temp_3.person_id) - temp_3.regimen_start_date) 
			            as Treatment_free_Interval,
							   
	/*Length of time from the initiation of each LoT to the date the patient discontinues 
	the treatment (i.e., the last administration or noncancelled order of 
	a drug contained in the same regimen). TTD will be described for the 
	first two LoTs. Discontinuation will be defined as having a subsequent 
	systemic anti-neoplastic therapy regimen after the first LoT; having a 
	gap of more than 120 days with no systemic anti-neoplastic therapy following 
	the last administration; or having a date of death while on the regimen. 
	Patients will be censored at their last known usage within the database
	or end of follow-up
	*/						   
	CASE when abs(lead(temp_3.regimen_start_date, 1) over (PARTITION BY 
	               temp_3.cohort_definition_id,	temp_3.person_id,
	               order by temp_3.cohort_definition_id,
							   temp_3.person_id) - temp_3.regimen_start_date) >= 120
							   OR lead(temp_3.regimen_start_date, 1) over (PARTITION BY 
							   temp_3.cohort_definition_id,temp_3.person_id 
							   order by temp_3.cohort_definition_id,temp_3.person_id) IS NULL
							   then abs(temp_3.regimen_start_date - temp_3.regimen_end_date)
							   end as Time_to_Treatment_Discontinuation,
							   
	/*Time from the index date to the date the patient received their next systemic
anti-neoplastic treatment regimen or to their date of death if death occurs prior 
to having another systemic anti-neoplastic treatment regimen. Patients will be censored 
at their last activity within the database or end of follow-up*/

	CASE when Line_of_therapy = 1 AND 
	  lead(regimen_start_date, 1) over (PARTITION BY temp_3.cohort_definition_id,person_id 
							   order by temp_3.cohort_definition_id, person_id) IS NOT NULL 
							   then abs(lead(regimen_start_date, 1) over (PARTITION BY temp_3.cohort_definition_id,
							   person_id 
							   order by temp_3.cohort_definition_id, temp_3.person_id) - temp_3.cohort_start_date)
		when temp_3.regimen_end_date = temp_3.observation_period_end_date 
		OR temp_3.regimen_end_date = temp_3.death_date		
		then abs(lead(regimen_start_date, 1) over 
		(PARTITION BY temp_3.cohort_definition_id,
							   temp_3.person_id 
							   order by temp_3.cohort_definition_id,
							   temp_3.person_id) - temp_3.regimen_end_date)
							   end 
							   as Time_to_Next_Treatment

from temp_3 group by temp_3.cohort_definition_id,
       temp_3.person_id, temp_3.regimen_start_date,
       temp_3.regimen_end_date 
       order by 1,2,3
;



-- Charlson analysis
DROP TABLE IF EXISTS @cohort_database_schema.charlson_concepts;
CREATE TABLE @cohort_database_schema.charlson_concepts
(
    diag_category_id INT,
    concept_id       INT
);

DROP TABLE IF EXISTS @cohort_database_schema.charlson_scoring;
CREATE TABLE @cohort_database_schema.charlson_scoring
(
    diag_category_id   INT,
    diag_category_name VARCHAR(255),
    weight             INT
);


--acute myocardial infarction
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (1, 'Myocardial infarction', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 1, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (4329847);


--Congestive heart failure
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (2, 'Congestive heart failure', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 2, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (316139);


--Peripheral vascular disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (3, 'Peripheral vascular disease', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 3, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (321052);


--Cerebrovascular disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (4, 'Cerebrovascular disease', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 4, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);


--Dementia
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (5, 'Dementia', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 5, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (4182210);


--Chronic pulmonary disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (6, 'Chronic pulmonary disease', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 6, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (4063381);


--Rheumatologic disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (7, 'Rheumatologic disease', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 7, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348);


--Peptic ulcer disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (8, 'Peptic ulcer disease', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 8, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (4247120);


--Mild liver disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (9, 'Mild liver disease', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 9, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (4064161, 4212540);


--Diabetes (mild to moderate)
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (10, 'Diabetes (mild to moderate)', 1);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 10, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (201820);


--Diabetes with chronic complications
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (11, 'Diabetes with chronic complications', 2);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 11, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (443767, 442793);


--Hemoplegia or paralegia
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (12, 'Hemoplegia or paralegia', 2);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 12, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (192606, 374022);


--Renal disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (13, 'Renal disease', 2);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 13, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (4030518);

--Any malignancy
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (14, 'Any malignancy', 2);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 14, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (443392);


--Leukemia
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (15, 'Leukemia', 2);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 15, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (317510);


--Lymphoma
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (16, 'Lymphoma', 2);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 16, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (432571);


--Moderate to severe liver disease
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (17, 'Moderate to severe liver disease', 3);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 17, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (4245975, 4029488, 192680, 24966);


--Metastatic solid tumor
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (18, 'Metastatic solid tumor', 6);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 18, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (432851);


--AIDS
INSERT INTO @cohort_database_schema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (19, 'AIDS', 6);

INSERT INTO @cohort_database_schema.charlson_concepts (diag_category_id, concept_id)
SELECT 19, descendant_concept_id
FROM @cdm_database_schema.concept_ancestor
WHERE ancestor_concept_id IN (439727);



DROP TABLE IF EXISTS @cohort_database_schema.charlson_map;
CREATE TABLE @cohort_database_schema.charlson_map AS
SELECT DISTINCT @cohort_database_schema.charlson_scoring.diag_category_id,
                @cohort_database_schema.charlson_scoring.weight,
                cohort_definition_id,
                cohort.subject_id,
                cohort.cohort_start_date
FROM @cohort_database_schema.@cohort_table cohort
INNER JOIN @cdm_database_schema.condition_era condition_era
    ON cohort.subject_id = condition_era.person_id
INNER JOIN @cohort_database_schema.charlson_concepts
    ON condition_era.condition_concept_id = charlson_concepts.concept_id
INNER JOIN @cohort_database_schema.charlson_scoring
    ON @cohort_database_schema.charlson_concepts.diag_category_id = @cohort_database_schema.charlson_scoring.diag_category_id
WHERE condition_era_start_date <= cohort.cohort_start_date;


-- Update weights to avoid double counts of mild/severe course of the disease
-- Diabetes
UPDATE @cohort_database_schema.charlson_map t1
SET weight = 0
FROM @cohort_database_schema.charlson_map t2
WHERE t1.subject_id = t2.subject_id
  AND t1.cohort_definition_id = t2.cohort_definition_id
  AND t1.diag_category_id = 10
  AND t2.diag_category_id = 11;

-- Liver disease
UPDATE @cohort_database_schema.charlson_map t1
SET weight = 0
FROM @cohort_database_schema.charlson_map t2
WHERE t1.subject_id = t2.subject_id
  AND t1.cohort_definition_id = t2.cohort_definition_id
  AND t1.diag_category_id = 9
  AND t2.diag_category_id = 15;

-- Malignancy
UPDATE @cohort_database_schema.charlson_map t1
SET weight = 0
FROM @cohort_database_schema.charlson_map t2
WHERE t1.subject_id = t2.subject_id
  AND t1.cohort_definition_id = t2.cohort_definition_id
  AND t1.diag_category_id = 14
  AND t2.diag_category_id = 16;

-- Add age criteria
INSERT INTO @cohort_database_schema.charlson_map
SELECT 0 AS diag_category_id,
       CASE
           WHEN age < 50
               THEN 0
           WHEN age >= 50 AND age < 60
               THEN 1
           WHEN age >= 60 AND age < 70
               THEN 2
           WHEN age >= 70 AND age < 80
               THEN 3
           WHEN age >= 80
               THEN 4
       END AS weight,
       cohort_definition_id, person_id AS subject_id, cohort_start_date
FROM @cohort_database_schema.subject_age
WHERE cohort_definition_id IN (
                              SELECT DISTINCT cohort_definition_id
                              FROM @cohort_database_schema.charlson_map
                              );

