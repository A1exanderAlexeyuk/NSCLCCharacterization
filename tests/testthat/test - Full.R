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
test_that("FULL 2m !!!", {

connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "testnode.arachnenetwork.com/synpuf_2m",
  user = Sys.getenv("ohdsi_password"),
  password = Sys.getenv("ohdsi_password"),
  port = "5441"
)
conn <- connect(connectionDetails = connectionDetails)
NSCLCCharacterization::runCohortDiagnostics(
connectionDetails = connectionDetails,
connection = conn,
cdmDatabaseSchema = cdmDatabaseSchema,
cohortDatabaseSchema = cohortDatabaseSchema,
createCohorts = F,
cohortTable = 'union_table2m',
tempEmulationSchema = NULL,
outputFolder = file.path(outputFolder, "diagnosticsExport"),
databaseId  = 'databaseId',
databaseName = 'databaseName',
databaseDescription = "Unknown"
)
writeDatabaseSchema <- "alex_alexeyuk_results"
cdmDatabaseSchema <- "cdm_531"
vocabularyTable <- "vt"
ccohortTable <- "ct_regimens"
regimenTable <- "regimenTable"
regimenIngredientTable <- "regimenIngredientTable_voc"
dateLagInput <- 30
OncologyRegimenFinder::createRegimens(connectionDetails = connectionDetails,
                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                      writeDatabaseSchema = writeDatabaseSchema,
                                      cohortTable = ccohortTable,
                                      rawEventTable = rawEventTable,
                                      regimenTable = regimenTable,
                                      regimenIngredientTable = regimenIngredientTable,
                                      vocabularyTable = vocabularyTable,
                                      cancerConceptId = 4115276,
                                      dateLagInput = 30,
                                      generateVocabTable = F,
                                      generateRawEvents = F
)

NSCLCCharacterization::runStudy(connectionDetails = connectionDetails,
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
         gapBetweenTreatment = 120)


  list_of_files <- list.files(path = file.path(outputFolder, "diagnosticsExport"),
                              recursive = TRUE,
                              pattern = "\\.csv",
                              full.names = TRUE)

  setOfFiles <- c("metrics_distribution__info.csv",
   "Survuval_info.csv"             ,
   "TFI_info.csv"                  ,
   "timeToNT_info.csv"             ,
   "timeToTI_info.csv"             ,
   "TTD_info.csv" )

  spitter <- function(x){
    unlist(stringi::stri_split(x, regex="/"))[9]
  }
  list <- sapply(list_of_files, splitter)
  expect_equal(setOfFiles,  intersect(setOfFiles, list))

})


test_that("FULL 110k !!!", {

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
    createCohorts = T,
    cohortTable = 'union_table110k',
    tempEmulationSchema = NULL,
    outputFolder = file.path(outputFolder, "diagnosticsExport"),
    databaseId  = 'databaseId',
    databaseName = 'databaseName',
    databaseDescription = "Unknown"
  ), NA)
  writeDatabaseSchema <- "alex_alexeyuk_results"
  cdmDatabaseSchema <- "cdm_531"
  vocabularyTable <- "vt"
  ccohortTable <- "ct_regimens"
  regimenTable <- "regimenTable"
  regimenIngredientTable <- "regimenIngredientTable_voc"
  dateLagInput <- 30
  expect_error(OncologyRegimenFinder::createRegimens(connectionDetails = connectionDetails,
                                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                                     writeDatabaseSchema = writeDatabaseSchema,
                                                     cohortTable = ccohortTable,
                                                     rawEventTable = rawEventTable,
                                                     regimenTable = regimenTable,
                                                     regimenIngredientTable = regimenIngredientTable,
                                                     vocabularyTable = vocabularyTable,
                                                     cancerConceptId = 4115276,
                                                     dateLagInput = 30,
                                                     generateVocabTable = F,
                                                     generateRawEvents = F
  ), NA)



  expect_error( NSCLCCharacterization::runStudy(connectionDetails = connectionDetails,
                                                connection = conn,
                                                cdmDatabaseSchema = cdmDatabaseSchema,
                                                tempEmulationSchema = NULL,
                                                cohortDatabaseSchema = cohortDatabaseSchema,
                                                writeDatabaseSchema = writeDatabaseSchema,
                                                cohortTable = 'union_table110k',
                                                regimenIngredientsTable = regimenIngredientTable,
                                                createRegimenStats = T,
                                                createCategorizedRegimensTable = F,
                                                regimenStatsTable = 'regimen_stats_table',
                                                dropRegimenStatsTable = F,
                                                exportFolder = file.path(outputFolder, "diagnosticsExport"),
                                                databaseId = 't',
                                                databaseName = 't',
                                                databaseDescription = 't',
                                                gapBetweenTreatment = 120), NA)

  list_of_files <- list.files(path = file.path(outputFolder, "diagnosticsExport"),
                              recursive = TRUE,
                              pattern = "\\.csv",
                              full.names = TRUE)

  setOfFiles <- c("metrics_distribution__info.csv",
                  "Survuval_info.csv"             ,
                  "TFI_info.csv"                  ,
                  "timeToNT_info.csv"             ,
                  "timeToTI_info.csv"             ,
                  "TTD_info.csv" )

  splitter <- function(x){
    unlist(stringi::stri_split(x, regex="/"))[9]
  }
  list <- sapply(list_of_files, splitter)
  expect_equal(setOfFiles,  intersect(setOfFiles, list))

})

tt <- dbGetQuery(conn = conn, "select * from alex_alexeyuk_results.regimen_stats_table")

connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "testnode.arachnenetwork.com/synpuf_110k",
  user = Sys.getenv("ohdsi_password"),
  password = Sys.getenv("ohdsi_password"),
  port = "5441"
)
conn <- connect(connectionDetails = connectionDetails)
NSCLCCharacterization::runStudy(connectionDetails = connectionDetails,
                                connection = conn,
                                cdmDatabaseSchema = cdmDatabaseSchema,
                                tempEmulationSchema = NULL,
                                cohortDatabaseSchema = cohortDatabaseSchema,
                                writeDatabaseSchema = writeDatabaseSchema,
                                cohortTable = 'union_table110k',
                                regimenIngredientsTable = regimenIngredientTable,
                                createRegimenStats = T,
                                createCategorizedRegimensTable = F,
                                regimenStatsTable = 'regimen_stats_table',
                                dropRegimenStatsTable = F,
                                exportFolder = file.path(outputFolder, "diagnosticsExport"),
                                databaseId = 't',
                                databaseName = 't',
                                databaseDescription = 't',
                                gapBetweenTreatment = 120)


connectionDetails <- createConnectionDetails(
  dbms = "postgresql",
  server = "testnode.arachnenetwork.com/synpuf_2m",
  user = Sys.getenv("ohdsi_password"),
  password = Sys.getenv("ohdsi_password"),
  port = "5441"
)
conn <- connect(connectionDetails = connectionDetails)
# NSCLCCharacterization::runStudy(connectionDetails = connectionDetails,
#                                 connection = conn,
#                                 cdmDatabaseSchema = cdmDatabaseSchema,
#                                 tempEmulationSchema = NULL,
#                                 cohortDatabaseSchema = cohortDatabaseSchema,
#                                 writeDatabaseSchema = writeDatabaseSchema,
#                                 cohortTable = 'union_table2m',
#                                 regimenIngredientsTable = 'regimenIngredientTable_voc',
#                                 createRegimenStats = F,
#                                 createCategorizedRegimensTable = F,
#                                 regimenStatsTable = 'regimen_stats_table',
#                                 dropRegimenStatsTable = F,
#                                 exportFolder = file.path(outputFolder, "diagnosticsExport"),
#                                 databaseId = 't',
#                                 databaseName = 't',
#                                 databaseDescription = 't',
#                                 gapBetweenTreatment = 120)

ll <- dbGetQuery(conn, "select subject_id from alex_alexeyuk_results.union_table2m limit 10")
dbGetQuery(conn, "select measurement_concept_id from cdm_531.measurement
where measurement_concept_id IN (

                          2212389)
            limit 100")
