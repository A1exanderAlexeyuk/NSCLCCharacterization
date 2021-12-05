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
test_that("FULL 100K!!!", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_110k",
    user = Sys.getenv("ohdsi_password"),
    password = Sys.getenv("ohdsi_password"),
    port = "5441"
  )
  conn <- connect(connectionDetails = connectionDetails)

  expect_error(NSCLCCharacterization::runCohortDiagnostics(
    connectionDetails = connectionDetails,
    connection = conn,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    createCohorts = F,
    cohortTable = 'union_table2',
    tempEmulationSchema = NULL,
    outputFolder = file.path(outputFolder, "diagnosticsExport"),
    databaseId  = 'databaseId',
    databaseName = 'databaseName',
    databaseDescription = "Unknown"
  ), NA)

  list_of_files <- list.files(path = file.path(outputFolder, "diagnosticsExport"),
                              recursive = TRUE,
                              pattern = "\\.csv",
                              full.names = TRUE)

  expect_true(length(list_of_files) > 0)

  expect_error(NSCLCCharacterization::runStudy(connectionDetails = connectionDetails,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               writeDatabaseSchema = cohortDatabaseSchema,
                                               tempEmulationSchema = NULL,
                                               cohortDatabaseSchema = cohortDatabaseSchema,
                                               cohortTable = cohortTable,
                                               regimenIngredientsTable = "regimeningredienttable_voc",
                                               createRegimenStats = F,
                                               gapBetweenTreatment=120,
                                               createCategorizedRegimensTable = T,
                                               regimenStatsTable = 'test_rst',
                                               exportFolder = file.path(outputFolder, "diagnosticsExport"),
                                               databaseId=databaseId,
                                               databaseName=databaseName,
                                               dropRegimenStatsTable = FALSE, # optional - drop created table
                                               databaseDescription = ""
  ), NA)

})

test_that("FULL 2m !!!", {

  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_2m",
    user = Sys.getenv("ohdsi_password"),
    password = Sys.getenv("ohdsi_password"),
    port = "5441"
  )
  conn <- connect(connectionDetails = connectionDetails)
#   expect_error(NSCLCCharacterization::runCohortDiagnostics(
#   connectionDetails = connectionDetails,
#   connection = conn,
#   cdmDatabaseSchema = cdmDatabaseSchema,
#   cohortDatabaseSchema = cohortDatabaseSchema,
#   createCohorts = T,
#   cohortTable = 'union_table2m',
#   tempEmulationSchema = NULL,
#   outputFolder = file.path(outputFolder, "diagnosticsExport"),
#   databaseId  = 'databaseId',
#   databaseName = 'databaseName',
#   databaseDescription = "Unknown"
# ), NA)


# # To view the results:
# # Optional: if there are results zip files from multiple sites in a folder, this merges them, which will speed up starting the viewer:
# CohortDiagnostics::preMergeDiagnosticsFiles(file.path(outputFolder, "diagnosticsExport"))
#
# # Use this to view the results. Multiple zip files can be in the same folder. If the files were pre-merged, this is automatically detected:
# CohortDiagnostics::launchDiagnosticsExplorer(file.path(outputFolder, "diagnosticsExport"))
#
#
# # To explore a specific cohort in the local database, viewing patient profiles:
# CohortDiagnostics::launchCohortExplorer(connectionDetails,
#                                         cdmDatabaseSchema,
#                                         cohortDatabaseSchema,
#                                         cohortTable,
#                                         cohortId)

# When finished with reviewing the diagnostics, use the next command
# to upload the diagnostic results
# uploadDiagnosticsResults(outputFolder, keyFileName, userName)

# devtools::install_github("A1exanderAlexeyuk/OncologyRegimenFinder")
# library(OncologyRegimenFinder)
# # writeDatabaseSchema <- "your_schema_to_write" # should be the same as cohortDatabaseSchema
# # cdmDatabaseSchema <- "cdm_schema"
#   writeDatabaseSchema <- "alex_alexeyuk_results"
#   cdmDatabaseSchema <- "cdm_531"
#   vocabularyTable <- "vt"
#   ccohortTable <- "ct_regimens"
#   regimenTable <- "regimenTable"
#   regimenIngredientTable <- "regimenIngredientTable_voc"
#   dateLagInput <- 30
#   expect_error(OncologyRegimenFinder::createRegimens(connectionDetails = connectionDetails,
#                                         cdmDatabaseSchema = cdmDatabaseSchema,
#                                         writeDatabaseSchema = writeDatabaseSchema,
#                                         cohortTable = ccohortTable,
#                                         rawEventTable = rawEventTable,
#                                         regimenTable = regimenTable,
#                                         regimenIngredientTable = regimenIngredientTable,
#                                         vocabularyTable = vocabularyTable,
#                                         cancerConceptId = 4115276,
#                                         dateLagInput = 30,
#                                         generateVocabTable = F,
#                                         generateRawEvents = F
#   ), NA)

# Use this to run the study. The results will be stored in a zip file called
# 'Results_<databaseId>.zip in the outputFolder.
  #regimenStatsTable <- "regimen_stats_table"

expect_error( NSCLCCharacterization::runStudy(connectionDetails = connectionDetails,
            connection = conn,
           cdmDatabaseSchema = cdmDatabaseSchema,
           tempEmulationSchema = NULL,
           cohortDatabaseSchema = cohortDatabaseSchema,
           writeDatabaseSchema = writeDatabaseSchema,
           cohortTable = 'union_table2m',
           regimenIngredientsTable = 'regimenIngredientTable_voc',
           createRegimenStats = T,
           createCategorizedRegimensTable = F,
           regimenStatsTable = 'regimen_stats_table',
           dropRegimenStatsTable = F,
           exportFolder = file.path(outputFolder, "diagnosticsExport"),
           databaseId = 't',
           databaseName = 't',
           databaseDescription = 't',
           gapBetweenTreatment = 120), NA)

})
