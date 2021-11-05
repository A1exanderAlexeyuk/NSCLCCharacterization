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
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = Sys.getenv("testserver"),
                                             user = Sys.getenv("testuser"),
                                             password = Sys.getenv("testuser"),
                                             port = Sys.getenv("testport"))
conn <- connect(connectionDetails=connectionDetails)
test_that("Survival test", {
  connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                               server = Sys.getenv("testserver"),
                                               user = Sys.getenv("testuser"),
                                               password = Sys.getenv("testuser"),
                                               port = Sys.getenv("testport"))
  conn <- connect(connectionDetails=connectionDetails)
  targetIds <- 1
  outcomeId <- 3

  expect_s3_class(NSCLCCharacterization::generateSurvival(connection = conn,
                                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                                         cohortTable = cohortTable,
                                                         targetIds = targetIds,
                                                         outcomeId = outcomeId,
                                                         packageName = "NSCLCCharacterization",
                                                         databaseId = databaseId
  ), "data.frame")
  expect_error(generateSurvival(connection = conn,
                                cohortDatabaseSchema = cohortDatabaseSchema,
                                cohortTable = NULL,
                                targetIds = targetIds,
                                outcomeId = outcomeId,
                                packageName = "NSCLCCharacterization",
                                databaseId = databaseId))
})

test_that("generateKaplanMeierDescriptionTFITTD", {
  connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                               server = Sys.getenv("testserver"),
                                               user = Sys.getenv("testuser"),
                                               password = Sys.getenv("testuser"),
                                               port = Sys.getenv("testport"))
  conn <- connect(connectionDetails=connectionDetails)
  targetIds <- 1
  regimenStatsTable = "stats_test_lines"
  expect_s3_class(NSCLCCharacterization::generateKaplanMeierDescriptionTFITTD(connection = conn,
                                                                              cohortDatabaseSchema = cohortDatabaseSchema,
                                                                              regimenStatsTable,
                                                                              targetIds = targetIds,
                                                                              packageName = "NSCLCCharacterization",
                                                                              databaseId = databaseId
  ), "data.frame")
})

