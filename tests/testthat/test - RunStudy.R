library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
resultsDatabaseSchema <- "alex_alexeyuk_results"
cdmDatabaseSchema <- "cdm_531"
cohortDatabaseSchema <- "alex_alexeyuk_results"
cohortTable <- "union_table2"
databaseId <- "testDatabaseId"
packageName <- "NSCLCCharacterization"
outputFolder <- getwd()
###########################Test passed ############################
test_that("Run Study!!!", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_110k",
    user = Sys.getenv("ohdsi_password"),
    password = Sys.getenv("ohdsi_password"),
    port = "5441"
  )
  conn <- connect(connectionDetails = connectionDetails)
  expect_error(NSCLCCharacterization::runStudy(connectionDetails = connectionDetails,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               writeDatabaseSchema = cohortDatabaseSchema,
                                               tempEmulationSchema = NULL,
                                               cohortDatabaseSchema = cohortDatabaseSchema,
                                               cohortTable = cohortTable,
                                               regimenIngredientsTable = "regimeningredienttable_voc",
                                               createRegimenStats = F,
                                               gapBetweenTreatment=120,
                                               createCategorizedRegimensTable = F,
                                               regimenStatsTable = 'test_rst',
                                               exportFolder = outputFolder,
                                               databaseId=databaseId,
                                               databaseName=databaseName,
                                               dropRegimenStatsTable = FALSE, # optional - drop created table
                                               databaseDescription = ""
  ), NA)

})

