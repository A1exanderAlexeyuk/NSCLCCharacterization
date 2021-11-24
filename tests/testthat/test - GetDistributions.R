library(testthat)
library(DatabaseConnector)
library(SqlRender)
resultsDatabaseSchema <- Sys.getenv("testresultsDatabaseSchema")
cdmDatabaseSchema <- Sys.getenv("testcdmDatabaseSchema")
cohortDatabaseSchema <- resultsDatabaseSchema
cohortTable <- Sys.getenv("testcohortTable")
databaseId <- "testDatabaseId"

test_that("Get Distributions", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = Sys.getenv("testserver"),
    user = Sys.getenv("testuser"),
    password = Sys.getenv("testuser"),
    port = Sys.getenv("testport")
  )
  connection <- connect(connectionDetails = connectionDetails)


  # prepare necessary tables
  targetIdsFormatted <- c(101, 102, 103)
  pathToSql <- system.file("sql", "sql_server",
    "distributions", "IQRComplementaryTables.sql",
    package = getThisPackageName()
  )

  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    targetIds = targetIdsFormatted
  )


  metricsDistribution <- data.frame()
  DistribAnalyses <- c(
    "AgeAtIndex",
    "CharlsonAtIndex",
    "NeutrophilToLymphocyteRatioAtIndex",
    "PDLAtIndex",
    "PlateletToLymphocyteRatioAtIndex"
  )
  for (analysis in DistribAnalyses) {
    result <- getAtEventDistribution(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = cohortTable,
      targetIds = targetIdsFormatted,
      databaseId = databaseId,
      packageName = getThisPackageName(),
      analysisName = analysis
    )
    metricsDistribution <- rbind(metricsDistribution, result)
  }

  expect_s3_class(metricsDistribution, "data.frame")

  pathToSql <- system.file("sql",
    "sql_server",
    "distributions",
    "RemoveComplementaryTables.sql",
    package = getThisPackageName()
  )
  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  expect_error(DatabaseConnector::renderTranslateExecuteSql(connection,
    sql = sql,
    cohortDatabaseSchema = cohortDatabaseSchema
  ), NA)
})
