library(testthat)
library(DatabaseConnector)
library(SqlRender)

test_that("Create Regimen Statistics", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("postgres_local_server"),
    port = "5432",
    connectionString = Sys.getenv("postgres_local_conn_string"),
    user = "postgres",
    password = Sys.getenv("postgres_local_password")
    )
  conn <- connect(connectionDetails = connectionDetails)
  cdmDatabaseSchema = "regimen_stats_schema"
  writeDatabaseSchema = "regimen_stats_schema"
  regimenIngredientsTable <- "regimeningredienttable"
  regimenStatsTable <- "regimenStatsTable1"
  cohortTable = "ct"
  expect_error(NSCLCCharacterization::createRegimenStats(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    writeDatabaseSchema = writeDatabaseSchema,
    cohortTable = cohortTable,
    regimenStatsTable = regimenStatsTable,
    regimenIngredientsTable = regimenIngredientsTable,
    gapBetweenTreatment = 120
  ), NA)

  test <- DatabaseConnector::renderTranslateQuerySql(connection = conn,
                                                  sql = "SELECT * FROM @writeDatabaseSchema.@regimenStatsTable",
                                                  writeDatabaseSchema = writeDatabaseSchema,
                                                  regimenStatsTable = regimenStatsTable,
                                                  snakeCaseToCamelCase = TRUE)
  expect_true(nrow(test) > 0)

})





test_that("Create Regimen Categories", {
  cdmDatabaseSchema = "regimen_stats_schema"
  writeDatabaseSchema = "regimen_stats_schema"
  cohortDatabaseSchema = writeDatabaseSchema
  regimenIngredientsTable <- "regimeningredienttable"
  regimenStatsTable <- "regimenStatsTable"
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("postgres_local_server"),
    port = "5432",
    connectionString = Sys.getenv("postgres_local_conn_string"),
    user = "postgres",
    password = Sys.getenv("postgres_local_password")
  )
  conn <- connect(connectionDetails = connectionDetails)
  t <- NSCLCCharacterization::createCategorizedRegimensTable(
    connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = 1
  )
  expect_s3_class(t, "data.frame")

  expect_true(dim(t)[[1]] > 0)


})



