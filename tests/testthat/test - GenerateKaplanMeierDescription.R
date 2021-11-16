library(testthat)
library(DatabaseConnector)
library(SqlRender)
library(lubridate)
resultsDatabaseSchema <- Sys.getenv("testresultsDatabaseSchema")
cdmDatabaseSchema <- Sys.getenv("testcdmDatabaseSchema")
cohortDatabaseSchema <- resultsDatabaseSchema
cohortTable <- Sys.getenv("testcohortTable")
databaseId <- "testDatabaseId"
packageName <- "NSCLCCharacterization"
test_that("Survival test", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("testserver"),
    user = Sys.getenv("testuser"),
    password = Sys.getenv("testuser"),
    port = Sys.getenv("testport")
  )
  conn <- connect(connectionDetails = connectionDetails)
  targetIds <- 1
  outcomeId <- 3

  expect_s3_class(NSCLCCharacterization::generateSurvival(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    targetIds = targetIds,
    outcomeId = outcomeId,
    packageName = "NSCLCCharacterization",
    databaseId = databaseId
  ), "data.frame")
  expect_error(generateSurvival(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = NULL,
    targetIds = targetIds,
    outcomeId = outcomeId,
    packageName = "NSCLCCharacterization",
    databaseId = databaseId
  ))
})



# !!Test passed
test_that("generateKaplanMeierDescriptionTFITTD", {
  # locally
  cohortDatabaseSchema <- "regimen_stats_schema"
  regimenStatsTable <- "rstF2"
  targetIds <- 1
  databaseId <- "test"
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

  expect_s3_class(NSCLCCharacterization::generateKaplanMeierDescriptionTFITTD(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable,
    targetIds = targetIds,
    databaseId = databaseId
  ), "data.frame")
})





# !!!Test passed
test_that("generateKaplanMeierDescriptionTNT", {
  cohortDatabaseSchema <- "regimen_stats_schema"
  regimenStatsTable <- "rstF2"
  targetIds <- 1
  databaseId <- "test"
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

  testthat::expect_s3_class(NSCLCCharacterization::generateKaplanMeierDescriptionTNT(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIds,
    databaseId = databaseId
  ), "data.frame")
})
