library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
cohortDatabaseSchema <- 'regimen_stats_schema'
cohortTable <- 'construct_test'
databaseId <- "testDatabaseId"
packageName <- "NSCLCCharacterization"
test_that("Create Cohort Table", {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("postgres_local_server"),
    port = "5432",
    connectionString = Sys.getenv("postgres_local_conn_string"),
    user = "postgres",
    password = Sys.getenv("postgres_local_password")
  )
  conn <- connect(connectionDetails = connectionDetails)

  expect_error(renderTranslateExecuteSql(
    connection = conn, sql = "INSERT INTO
                                       @cohortDatabaseSchema.@cohortTable
                                       VALUES (1, 1, '2000-01-01', '2000-01-01')",
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  ))

  NSCLCCharacterization::createCohortTable(
    connectionDetails = connectionDetails,
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    resultsDatabaseSchema = cohortDatabaseSchema,
  )

  expect_error(renderTranslateExecuteSql(
    connection = conn, sql = "INSERT INTO
                                       @cohortDatabaseSchema.@cohortTable
                                       VALUES (1, 1, '2000-01-01', '2000-01-01')",
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  ), NA)

  expect_type(renderTranslateQuerySql(
    connection = conn, sql = "SELECT *
                                      FROM @cohortDatabaseSchema.@cohortTable",
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  ), "list")

  expect_error(renderTranslateExecuteSql(
    connection = conn, sql = "DROP TABLE
                                       @cohortDatabaseSchema.@cohortTable
                                       ",
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  ), NA)
})
