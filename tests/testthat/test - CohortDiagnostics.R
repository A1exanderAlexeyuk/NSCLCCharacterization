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
###########################Test passed ############################
test_that("Cohort Diagnostics", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_110k",
    user = "ohdsi",
    password = "ohdsi",
    port = "5441"
  )
  conn <- connect(connectionDetails = connectionDetails)
  outputFolder <- getwd()
  expect_error(NSCLCCharacterization::runCohortDiagnostics(
    connection = conn,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    createCohorts = FALSE,
    cohortIds = c(101,102,103),
    exportFolder = file.path(outputFolder, "diagnosticsExport"),
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = "union_table",
    tempEmulationSchema = NULL,
    outputFolder = outputFolder
  ), NA)
  list_of_files <- list.files(path = file.path(outputFolder, "diagnosticsExport"),
                              recursive = TRUE,
                              pattern = "\\.csv",
                              full.names = TRUE)

  expect_true(length(list_of_files) > 0)
})




