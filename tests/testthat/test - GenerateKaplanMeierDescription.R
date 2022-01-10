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
  connectionDetails <- createConnectionDetails(    dbms = "postgresql",
                                                   server = "postgres/localhost",
                                                   port = "5432",
                                                   connectionString = "jdbc:postgresql://localhost:5432/postgres",
                                                   user = "postgres",
                                                   password = Sys.getenv("postgres_local_password")
  )
  conn <- connect(connectionDetails = connectionDetails)
  targetIds <- c(101,102)
  outcomeId <- 103
  cohortDatabaseSchema <- "regimen_stats_schema"
  regimenStatsTable <- "rstF3"
  cohortTable = "ct_4test"
  test <- NSCLCCharacterization::generateSurvival(
    connection = conn,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    targetIds = targetIds,
    outcomeId = outcomeId,
    packageName = "NSCLCCharacterization",
    databaseId = databaseId
  )
  expect_s3_class(test, "data.frame")

  expect_true(nrow(test) > 0)


})



# !!Test passed
test_that("generateKaplanMeierDescriptionTFI", {
  # locally
  cohortDatabaseSchema <- "bigquery"
  regimenStatsTable <- "rst_test"
  targetIds <- 1
  databaseId <- "test"
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = "postgres/localhost",
    port = "5432",
    connectionString = "jdbc:postgresql://localhost:5432/postgres",
    user = "postgres",
    password = Sys.getenv("postgres_local_password"),
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
test_that("generateKaplanMeierDescriptionTTD", {
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
    password = Sys.getenv("postgres_local_password"),
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
    password = Sys.getenv("postgres_local_password"),
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



test_that("generateTimeToTreatmenInitiationStatistics", {
  cohortDatabaseSchema <- "regimen_stats_schema"
  targetIds <- c(101,102)
  outcomeId <- 103
  regimenStatsTable <- "rstF3"
  cohortTable = "ct_4test"
  databaseId <- "test"
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = "postgres/localhost",
    port = "5432",
    connectionString = "jdbc:postgresql://localhost:5432/postgres",
    user = "postgres",
    password = Sys.getenv("postgres_local_password"),
    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  )
  conn <- connect(connectionDetails = connectionDetails)
  t <- NSCLCCharacterization::generateTimeToTreatmenInitiationStatistics(connection = conn,
                                                                         cohortDatabaseSchema = cohortDatabaseSchema,
                                                                          targetIds = targetIds,
                                                                          outcomeId = outcomeId,
                                                                         cohortTable = cohortTable,# treatment initiation
                                                                          databaseId = databaseId)
  testthat::expect_s3_class(t, "data.frame")
  expect_true(dim(t)[[1]] > 0)
})

testthat::test_that("generateTreatmentStatistics", {
  cohortDatabaseSchema <- "regimen_stats_schema"
  targetIds <- c(1)
  regimenStatsTable <- "rstF3"
  databaseId <- "test"
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    server = "postgres/localhost",
    port = "5432",
    connectionString = "jdbc:postgresql://localhost:5432/postgres",
    user = "postgres",
    password = Sys.getenv("postgres_local_password"),
    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  )
  conn <- connect(connectionDetails = connectionDetails)
  t <- NSCLCCharacterization::generateTreatmentStatistics(connection = conn,
                                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                                            targetIds = targetIds,
                                                          regimenStatsTable = "rstF3",
                                                            databaseId = databaseId)
  testthat::expect_s3_class(t, "data.frame")
  testthat::expect_true(dim(t)[[1]] > 0)
})
