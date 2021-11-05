library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
resultsDatabaseSchema <- Sys.getenv("testresultsDatabaseSchema")
cdmDatabaseSchema <- Sys.getenv("testcdmDatabaseSchema")
cohortDatabaseSchema <- resultsDatabaseSchema
cohortTable <- Sys.getenv("testcohortTable")
databaseId <- "testDatabaseId"
packageName = "NSCLCCharacterization"
regimenStatsTable = "regimenStatsTable"
regimenIngredientsTable = "cancer_regimen_ingredients_table"
test_that("Create Regimen Statistics", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                                                  server = Sys.getenv("testserver"),
                                                                  user = Sys.getenv("testuser"),
                                                                  password = Sys.getenv("testuser"),
                                                                  port = Sys.getenv("testport"))
  conn <- connect(connectionDetails=connectionDetails)
  expect_error(NSCLCCharacterization::createRegimenStats(connection = conn,
                                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                                         cohortTable = cohortTable,
                                                         regimenStatsTable = regimenStatsTable,
                                                         regimenIngredientsTable = regimenIngredientsTable,
                                                         gapBetweenTreatment = 120), NA)

  expect_true(isTRUE(unlist(DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                         sql = "SELECT EXISTS (
                                     SELECT FROM information_schema.tables
                                     WHERE  table_schema = @cohortDatabaseSchema
                                     AND    table_name   = @regimenStatsTable
                                     );"))))


})

