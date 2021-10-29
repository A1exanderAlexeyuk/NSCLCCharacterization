source('file_4_test.R')
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "testnode.arachnenetwork.com/synpuf_110k",
                                             user = "ohdsi",
                                             password = 'ohdsi',
                                             port = "5441",
                                             pathToDriver = 'c:/jdbcDrivers')


conn <- connect(connectionDetails)
resultsDatabaseSchema <- 'alex_alexeyuk_results'
cdmDatabaseSchema <- 'cdm_531'
cohortTable <- "testLC"
targetIds <- 1
outcomeIds <- 3
df_test <- as.data.frame(renderTranslateQuerySql(connection = conn, 
                                      sql = "select * from alex_alexeyuk_results.testLC"))




library(testthat)
test_that("Survival test", {
  expect_s3_class(generateSurvival(connection = conn, 
                                cohortDatabaseSchema = resultsDatabaseSchema, 
                                cohortTable = cohortTable, 
                                targetIds = targetIds, 
                                outcomeIds = outcomeIds 
  ), "data.frame")
  expect_error(generateSurvival(connection = conn, 
                                cohortDatabaseSchema = resultsDatabaseSchema, 
                                cohortTable = NULL, 
                                targetIds = targetIds, 
                                outcomeIds = outcomeIds))
})
start <- lubridate::as_date("2008-06-19")
event <- lubridate::as_date("2010-01-01")

"COHORT_DEFINITION_ID SUBJECT_ID COHORT_START_DATE COHORT_END_DATE
1                     1        305        2008-06-19      2010-01-01
11                    3        305        2010-01-01      2010-01-01
"
test_that("Time To Outcome test", {
  expect_s3_class(generateTimeToEvent(connection = conn, 
                                   cohortDatabaseSchema = resultsDatabaseSchema, 
                                   cohortTable = cohortTable, 
                                   targetId = targetIds, 
                                   outcomeId = outcomeIds
  ), "data.frame")
  expect_error(generateSurvival(connection = conn, 
                                cohortDatabaseSchema = resultsDatabaseSchema, 
                                cohortTable = NULL, 
                                targetId = targetIds, 
                                outcomeId = outcomeId))
  expect_equal(generateTimeToEvent(connection = conn, 
                                   cohortDatabaseSchema = resultsDatabaseSchema, 
                                   cohortTable = cohortTable, 
                                   targetId = targetIds, 
                                   outcomeId = outcomeIds 
  )$value[1], start - event)
})

