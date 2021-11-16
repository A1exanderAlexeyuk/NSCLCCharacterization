library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
resultsDatabaseSchema <- Sys.getenv("testresultsDatabaseSchema")
cdmDatabaseSchema <- Sys.getenv("testcdmDatabaseSchema")
cohortDatabaseSchema <- resultsDatabaseSchema
cohortTable <- Sys.getenv("testcohortTable")
cohortTable <- "test2"
databaseId <- "testDatabaseId"
packageName <- "NSCLCCharacterization"
# connectionDetails <- createConnectionDetails(dbms = "postgresql",
#                                              server = Sys.getenv("testserver"),
#                                              user = Sys.getenv("testuser"),
#                                              password = Sys.getenv("testuser"),
#                                              port = Sys.getenv("testport"))
# conn <- connect(connectionDetails=connectionDetails)
#Test passed
test_that("Create Cohort Table", {

  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("testserver"),
    user = Sys.getenv("testuser"),
    password = Sys.getenv("testuser"),
    port = Sys.getenv("testport")
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



