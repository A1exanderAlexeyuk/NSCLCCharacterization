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
# connectionDetails <- createConnectionDetails(dbms = "postgresql",
#                                              server = Sys.getenv("testserver"),
#                                              user = Sys.getenv("testuser"),
#                                              password = Sys.getenv("testuser"),
#                                              port = Sys.getenv("testport"))
# conn <- connect(connectionDetails=connectionDetails)
test_that("Create Cohort Table", {
  connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                               server = Sys.getenv("testserver"),
                                               user = Sys.getenv("testuser"),
                                               password = Sys.getenv("testuser"),
                                               port = Sys.getenv("testport"))
  conn <- connect(connectionDetails=connectionDetails)
  expect_error(renderTranslateQuerySql(connection = conn, sql = "INSERT INTO
                                       @cohortDatabaseSchema.@cohortTable
                                       VALUES (1, 1, 1, '2000-01-01', '2000-01-01'",
                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                       cohortTable = cohortTable))

  NSCLCCharacterization::createCohortTable(connectionDetails = connectionDetails,
                                          connection = conn,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortTable = cohortTable,
                                          createInclusionStatsTables = FALSE,
                                          resultsDatabaseSchema = cohortDatabaseSchema,
                                          cohortInclusionTable = paste0(cohortTable, "_inclusion"),
                                          cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
                                          cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"),
                                          cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"))

  expect_error(renderTranslateQuerySql(connection = conn, sql = "INSERT INTO
                                       @cohortDatabaseSchema.@cohortTable
                                       VALUES (1, 1, 1, '2000-01-01', '2000-01-01'",
                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                       cohortTable = cohortTable), NA)

  expect_type(renderTranslateQuerySql(connection = conn, sql = "SELECT *
                                      FROM @cohortDatabaseSchema.@cohortTable",
                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                      cohortTable = cohortTable), "list")

  renderTranslateQuerySql(connection = conn, sql = "DROP TABLE
                                       @cohortDatabaseSchema.@cohortTable",
                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                       cohortTable = cohortTable)

  expect_error(renderTranslateQuerySql(connection = conn, sql = "INSERT INTO
                                       @cohortDatabaseSchema.@cohortTable
                                       VALUES (1, 1, 1, '2000-01-01', '2000-01-01'",
                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                       cohortTable = cohortTable))

})
