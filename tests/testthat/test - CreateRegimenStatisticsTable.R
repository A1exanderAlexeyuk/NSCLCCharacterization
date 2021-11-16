library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
resultsDatabaseSchema <- 'alex_alexeyuk_results1'
cdmDatabaseSchema <- Sys.getenv("testcdmDatabaseSchema")
cohortDatabaseSchema <- 'alex_alexeyuk_results1'
writeDatabaseSchema <- cohortDatabaseSchema
cohortTable <- Sys.getenv("testcohortTable")
databaseId <- "testDatabaseId"
packageName <- "NSCLCCharacterization"
regimenStatsTable <- "regimenStatsTable"
regimenIngredientsTable <- "cancer_regimen_ingredients_table"
test_that("Create Regimen Statistics", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_2m",
    user = Sys.getenv("testuser"),
    password = Sys.getenv("testuser"),
    port = Sys.getenv("testport")
  )
  conn <- connect(connectionDetails = connectionDetails)
  expect_error(NSCLCCharacterization::createRegimenStats(
    connection = conn,
    cdmDatabaseSchema = cdmDatabaseSchema,
    writeDatabaseSchema = writeDatabaseSchema,
    cohortTable = cohortTable,
    regimenStatsTable = regimenStatsTable,
    regimenIngredientsTable = regimenIngredientsTable,
    gapBetweenTreatment = 120
  ), NA)

  expect_error(DatabaseConnector::renderTranslateQuerySql(
    connection = conn,
    sql = "DROP TABLE  @cohortDatabaseSchema.@regimenStatsTable;",
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable
  ), NA)

})

test_that("Create Regimen Statistics", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = "postgres/localhost",
    port = "5432",
    connectionString = "jdbc:postgresql://localhost:5432/postgres",
    user = "postgres",
    password = "sql",
    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  )
  conn <- connect(connectionDetails = connectionDetails)
  NSCLCCharacterization::createCategorizedRegimensTable(
    connection = conn,
    cohortDatabaseSchema = 'regimen_stats_schema',
    regimenStatsTable = "rstF2",
    targetIds = 1
  )


  expect_s3_class(t, 'data.frame')
})

