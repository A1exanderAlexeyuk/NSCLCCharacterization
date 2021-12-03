library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
resultsDatabaseSchema <- "alex_alexeyuk_results"
cdmDatabaseSchema <- "cdm_531"
cohortDatabaseSchema <- "alex_alexeyuk_results"
cohortTable <- "test_cohort_table"
databaseId <- "testDatabaseId"
packageName <- "NSCLCCharacterization"
outputFolder <- getwd()
###########################Test passed ############################
test_that("Cohort Diagnostics", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_110k",
    user = Sys.getenv("ohdsi_password"),
    password = Sys.getenv("ohdsi_password"),
    port = "5441"
  )
  conn <- connect(connectionDetails = connectionDetails)

  expect_error(NSCLCCharacterization::runCohortDiagnostics(
    connection = conn,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    createCohorts = TRUE,
    outputFolder = file.path(outputFolder, "diagnosticsExport"),
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = "cohortd_test",
     tempEmulationSchema = NULL
    # cohortStagingTable = 'cohort_stg_test'
  ), NA)

  list_of_files <- list.files(path = file.path(outputFolder, "diagnosticsExport"),
                              recursive = TRUE,
                              pattern = "\\.csv",
                              full.names = TRUE)

  expect_true(length(list_of_files) > 0)

})
# ORACLE



resultsDatabaseSchema <- "alex_alexeyuk_results"

cohortDatabaseSchema <- "alex_alexeyuk_results"
cohortTable <- "test_cohort_table"
databaseId <- "testDatabaseId"
packageName <- "NSCLCCharacterization"
outputFolder <- getwd()
###########################Test passed ############################
test_that("Cohort Diagnostics", {
  tempEmulationSchema <- "alex_a"
  cdmDatabaseSchema <- "CDMV5"
  connectionDetails <- createConnectionDetails(dbms = "oracle",
                                               server = Sys.getenv("CDM5_ORACLE_SERVER"),
                                               user = "OHDSI",
                                               password = Sys.getenv("CDM5_ORACLE_PASSWORD"),
                                               port = "1521"

  )
  conn <- connect(connectionDetails)

  expect_error(NSCLCCharacterization::runCohortDiagnostics(
    tempEmulationSchema <- "OHDSI",
    connection = conn,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    createCohorts = TRUE,
    outputFolder = file.path(outputFolder, "diagnosticsExport"),
    cohortDatabaseSchema = "OHDSI",
    cohortTable = "oracle_test"
  ), NA)

  list_of_files <- list.files(path = file.path(outputFolder, "diagnosticsExport"),
                              recursive = TRUE,
                              pattern = "\\.csv",
                              full.names = TRUE)

  expect_true(length(list_of_files) > 0)

})
