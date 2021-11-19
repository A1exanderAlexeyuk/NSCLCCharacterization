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

})



# !!Test passed
test_that("generateKaplanMeierDescriptionTFI", {
  # locally
  cohortDatabaseSchema <- "regimen_stats_schema"
  regimenStatsTable <- "rstF3"
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
  t <-  NSCLCCharacterization::generateKaplanMeierDescriptionTFI(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable,
    targetIds = targetIds,
    databaseId = databaseId
  )
  expect_s3_class(t, "data.frame")
  expect_true(dim(t)[[1]] > 0)
})


# !!Test passed
test_that("generateKaplanMeierDescriptionTFI", {
  # locally
  cohortDatabaseSchema <- "regimen_stats_schema"
  regimenStatsTable <- "rstF3"
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
  t <- NSCLCCharacterization::generateKaplanMeierDescriptionTTD(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable,
    targetIds = targetIds,
    databaseId = databaseId)
  expect_s3_class(
  t, "data.frame")

  expect_true(dim(t)[[1]] > 0)
})



# !!!Test passed
test_that("generateKaplanMeierDescriptionTNT", {
  cohortDatabaseSchema <- "regimen_stats_schema"
  regimenStatsTable <- "rstF3"
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
  t <- NSCLCCharacterization::generateKaplanMeierDescriptionTNT(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIds,
    databaseId = databaseId
  )
  testthat::expect_s3_class(t, "data.frame")
  expect_true(dim(t)[[1]]>0)
})
