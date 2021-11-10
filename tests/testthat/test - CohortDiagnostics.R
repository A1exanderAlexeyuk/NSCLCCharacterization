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
# connectionDetails <- createConnectionDetails(dbms = "postgresql",
#                                              server = Sys.getenv("testserver"),
#                                              user = Sys.getenv("testuser"),
#                                              password = Sys.getenv("testuser"),
#                                              port = Sys.getenv("testport"))
# conn <- connect(connectionDetails=connectionDetails)

test_that("Cohort Diagnostics", {

  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("testserver"),
    user = Sys.getenv("testuser"),
    password = Sys.getenv("testuser"),
    port = Sys.getenv("testport")
  )
  conn <- connect(connectionDetails = connectionDetails)

  expect_error(NSCLCCharacterization::runCohortDiagnostics(connection = conn,
                                                          connectionDetails = connectionDetails,
                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                           cohortDatabaseSchema = cdmDatabaseSchema,
                                                           cohortTable = cohortTable,
                                                           tempEmulationSchema = NULL,
                                                           outputFolder = getwd()), NA)
})




