-- create subject age table
DROP TABLE IF EXISTS @cohortDatabaseSchema.subject_age;
CREATE TABLE @cohortDatabaseSchema.subject_age AS
SELECT tab.cohort_definition_id,
       tab.person_id,
       tab.cohort_start_date,
       DATEDIFF(year, DATEFROMPARTS(tab.year_of_birth, tab.month_of_birth, tab.day_of_birth),
                tab.cohort_start_date) AS age
FROM (
     SELECT c.cohort_definition_id, p.person_id, c.cohort_start_date, p.year_of_birth, p.month_of_birth, p.day_of_birth
     FROM @cohortDatabaseSchema.@cohortTable c
     JOIN @cdmDatabaseSchema.person p
         ON p.person_id = c.subject_id
     WHERE c.cohort_definition_id IN (@targetIds)
     ) tab
;

-- Charlson analysis
DROP TABLE IF EXISTS @cohortDatabaseSchema.charlson_concepts;
CREATE TABLE @cohortDatabaseSchema.charlson_concepts
(
    diag_category_id INT,
    concept_id       INT
);

DROP TABLE IF EXISTS @cohortDatabaseSchema.charlson_scoring;
CREATE TABLE @cohortDatabaseSchema.charlson_scoring
(
    diag_category_id   INT,
    diag_category_name VARCHAR(255),
    weight             INT
);


--acute myocardial infarction
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (1, 'Myocardial infarction', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 1, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (4329847);


--Congestive heart failure
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (2, 'Congestive heart failure', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 2, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (316139);


--Peripheral vascular disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (3, 'Peripheral vascular disease', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 3, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (321052);


--Cerebrovascular disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (4, 'Cerebrovascular disease', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 4, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);


--Dementia
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (5, 'Dementia', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 5, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (4182210);


--Chronic pulmonary disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (6, 'Chronic pulmonary disease', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 6, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (4063381);


--Rheumatologic disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (7, 'Rheumatologic disease', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 7, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348);


--Peptic ulcer disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (8, 'Peptic ulcer disease', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 8, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (4247120);


--Mild liver disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (9, 'Mild liver disease', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 9, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (4064161, 4212540);


--Diabetes (mild to moderate)
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (10, 'Diabetes (mild to moderate)', 1);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 10, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (201820);


--Diabetes with chronic complications
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (11, 'Diabetes with chronic complications', 2);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 11, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (443767, 442793);


--Hemoplegia or paralegia
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (12, 'Hemoplegia or paralegia', 2);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 12, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (192606, 374022);


--Renal disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (13, 'Renal disease', 2);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 13, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (4030518);

--Any malignancy
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (14, 'Any malignancy', 2);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 14, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (443392);


--Leukemia
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (15, 'Leukemia', 2);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 15, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (317510);


--Lymphoma
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (16, 'Lymphoma', 2);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 16, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (432571);


--Moderate to severe liver disease
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (17, 'Moderate to severe liver disease', 3);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 17, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (4245975, 4029488, 192680, 24966);


--Metastatic solid tumor
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (18, 'Metastatic solid tumor', 6);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 18, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (432851);


--AIDS
INSERT INTO @cohortDatabaseSchema.charlson_scoring (diag_category_id, diag_category_name, weight)
VALUES (19, 'AIDS', 6);

INSERT INTO @cohortDatabaseSchema.charlson_concepts (diag_category_id, concept_id)
SELECT 19, descendant_concept_id
FROM @cdmDatabaseSchema.concept_ancestor
WHERE ancestor_concept_id IN (439727);



DROP TABLE IF EXISTS @cohortDatabaseSchema.charlson_map;
CREATE TABLE @cohortDatabaseSchema.charlson_map AS
SELECT DISTINCT @cohortDatabaseSchema.charlson_scoring.diag_category_id,
                @cohortDatabaseSchema.charlson_scoring.weight,
                cohort_definition_id,
                cohort.subject_id,
                cohort.cohort_start_date
FROM @cohortDatabaseSchema.@cohortTable cohort
INNER JOIN @cdmDatabaseSchema.condition_era condition_era
    ON cohort.subject_id = condition_era.person_id
INNER JOIN @cohortDatabaseSchema.charlson_concepts
    ON condition_era.condition_concept_id = charlson_concepts.concept_id
INNER JOIN @cohortDatabaseSchema.charlson_scoring
    ON @cohortDatabaseSchema.charlson_concepts.diag_category_id = @cohortDatabaseSchema.charlson_scoring.diag_category_id
WHERE condition_era_start_date <= cohort.cohort_start_date;


-- Update weights to avoid double counts of mild/severe course of the disease
-- Diabetes
UPDATE @cohortDatabaseSchema.charlson_map t1
SET weight = 0
FROM @cohortDatabaseSchema.charlson_map t2
WHERE t1.subject_id = t2.subject_id
  AND t1.cohort_definition_id = t2.cohort_definition_id
  AND t1.diag_category_id = 10
  AND t2.diag_category_id = 11;

-- Liver disease
UPDATE @cohortDatabaseSchema.charlson_map t1
SET weight = 0
FROM @cohortDatabaseSchema.charlson_map t2
WHERE t1.subject_id = t2.subject_id
  AND t1.cohort_definition_id = t2.cohort_definition_id
  AND t1.diag_category_id = 9
  AND t2.diag_category_id = 15;

-- Malignancy
UPDATE @cohortDatabaseSchema.charlson_map t1
SET weight = 0
FROM @cohortDatabaseSchema.charlson_map t2
WHERE t1.subject_id = t2.subject_id
  AND t1.cohort_definition_id = t2.cohort_definition_id
  AND t1.diag_category_id = 14
  AND t2.diag_category_id = 16;

-- Add age criteria
INSERT INTO @cohortDatabaseSchema.charlson_map
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
FROM @cohortDatabaseSchema.subject_age
WHERE cohort_definition_id IN (
                              SELECT DISTINCT cohort_definition_id
                              FROM @cohortDatabaseSchema.charlson_map
                              );



-- Add age criteria
INSERT INTO @cohortDatabaseSchema.charlson_map
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
FROM @cohortDatabaseSchema.subject_age
WHERE cohort_definition_id IN (
                              SELECT DISTINCT cohort_definition_id
                              FROM @cohortDatabaseSchema.charlson_map
                              );
