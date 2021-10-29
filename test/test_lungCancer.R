source('file_4_test.R')
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
library(testthat)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "testnode.arachnenetwork.com/synpuf_110k",
                                             user = "ohdsi",
                                             password = 'ohdsi',
                                             port = "5441",
                                             pathToDriver = 'c:/jdbcDrivers')


conn <- connect(connectionDetails)
# df_test <- as.data.frame(renderTranslateQuerySql(connection = conn, 
                                       # sql = "select * from alex_alexeyuk_results.testLC"))
#==========================Survival test===================================
test_that("Survival test", {
  conn <- connect(connectionDetails)
  resultsDatabaseSchema <- 'alex_alexeyuk_results'
  cdmDatabaseSchema <- 'cdm_531'
  cohortTable <- "testLC"
  targetIds <- 1
  outcomeIds <- 3
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


#===============================================Outcome test===============================================
test_that("Time To Outcome test", {

  conn <- connect(connectionDetails)
  resultsDatabaseSchema <- 'alex_alexeyuk_results'
  cdmDatabaseSchema <- 'cdm_531'
  cohortTable <- "testLC"
  targetIds <- 1
  outcomeIds <- 3
  output <- "         COHORT_DEFINITION_ID SUBJECT_ID COHORT_START_DATE COHORT_END_DATE
  -- target    1                     1        305        2008-06-19      2010-01-01
  -- outcome   11                    3        305        2010-01-01      2010-01-01
               2                     1        442        2008-07-10      2010-03-01
               12                    3        442        2010-03-01      2010-03-01
              "
  start_1 <- lubridate::as_date("2008-06-19")
  event_1 <- lubridate::as_date("2010-01-01")
  start_11 <- lubridate::as_date("2008-07-10")
  event_11 <- lubridate::as_date("2010-03-01")
  conn <- connect(connectionDetails)
  expect_s3_class(generateTimeToEvent(connection = conn, 
                                   cohortDatabaseSchema = resultsDatabaseSchema, 
                                   cohortTable = cohortTable, 
                                   targetId = targetIds, 
                                   outcomeId = outcomeIds
  ), "data.frame")
   expect_equal(generateTimeToEvent(connection = conn, 
                                   cohortDatabaseSchema = resultsDatabaseSchema, 
                                   cohortTable = cohortTable, 
                                   targetId = targetIds, 
                                   outcomeId = outcomeIds 
  )$value[1], as.integer(event_1 - start_1))
  
  expect_equal(generateTimeToEvent(connection = conn, 
                                   cohortDatabaseSchema = resultsDatabaseSchema, 
                                   cohortTable = cohortTable, 
                                   targetId = targetIds, 
                                   outcomeId = outcomeIds 
  )$value[2], as.integer(event_11 - start_11))
})


#===============================================Lot===============================================
test_that("Lines of Therapy test", {
  conn <- connect(connectionDetails)
  expect_s3_class(generateLinesOfTreatment(connection = conn, 
                                          cohortDatabaseSchema = 'alex_alexeyuk_results', 
                                          regimenTable =  "test_regimens"
  ), "data.frame")
  
  test_df <- generateLinesOfTreatment(connection = conn, 
                          cohortDatabaseSchema = 'alex_alexeyuk_results', 
                          regimenTable =  "test_regimens")
  expect_equal(length(test_df$lineOfTherapy), 8)
  expect_equal(lubridate::as_date(test_df$regimenEndDate[1]), 
               lubridate::as_date("2000-04-05"))
  expect_equal(max(lubridate::as_date(test_df$regimenEndDate)), 
               lubridate::as_date("2003-03-10"))
  expect_equal(dim(test_df)[1], 
               c(8))
  expect_false(is.na(test_df$regimenEndDate[8]))
  expect_lt(max(test_df$lineOfTherapy) , 4)
  expect_gt(max(test_df$lineOfTherapy) , 2)
})

#==================================Treatment-free Interval (TFI)===============================================
test_that("Treatment-free Interval (TFI)", {
  conn <- connect(connectionDetails)
  expect_s3_class(generateLinesOfTreatment(connection = conn, 
                                           cohortDatabaseSchema = 'alex_alexeyuk_results', 
                                           regimenTable =  "test_regimens"
  ), "data.frame")
  
  test_df <- generateLinesOfTreatment(connection = conn, 
                                      cohortDatabaseSchema = 'alex_alexeyuk_results', 
                                      regimenTable =  "test_regimens")
  expect_equal(test_df$treatmentFreeInterval[2], 10)
  expect_false(!is.na(test_df$treatmentFreeInterval[1]))
  expect_equal(test_df$treatmentFreeInterval[8], 0)
  
})


#==================================Time to Treatment Discontinuation (TTD)===============================================
test_that("Time to Treatment Discontinuation (TTD)", {
  conn <- connect(connectionDetails)
  expect_s3_class(generateLinesOfTreatment(connection = conn, 
                                           cohortDatabaseSchema = 'alex_alexeyuk_results', 
                                           regimenTable =  "test_regimens"
  ), "data.frame")
  
  test_df <- generateLinesOfTreatment(connection = conn, 
                                      cohortDatabaseSchema = 'alex_alexeyuk_results', 
                                      regimenTable =  "test_regimens")
  expect_equal(test_df$timeToTreatmentDiscontinuation[2], 626)
  expect_false(!is.na(test_df$timeToTreatmentDiscontinuation[5]))
  expect_equal(test_df$timeToTreatmentDiscontinuation[8], 
               as.integer(abs(as_date("2003-02-01") - as_date("2003-03-10"))))
  
})
