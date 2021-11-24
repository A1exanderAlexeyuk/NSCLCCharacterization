library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)

test_that("Create Cohort Table", {
  cohortDatabaseSchema <- 'alex_alexeyuk_results'
  cohortTable <- 'construct_test'
  databaseId <- "testDatabaseId"
  packageName <- "NSCLCCharacterization"
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_110k",
    user = "ohdsi",
    password = "ohdsi",
    port = "5441"
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
    resultsDatabaseSchema = cohortDatabaseSchema
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
