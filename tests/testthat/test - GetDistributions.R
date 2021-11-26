library(testthat)
library(DatabaseConnector)
library(SqlRender)

test_that("Get Distributions", {
  connectionDetails <- createConnectionDetails(
    dbms = "postgresql",
    server = "testnode.arachnenetwork.com/synpuf_110k",
    user = "ohdsi",
    password = "ohdsi",
    port = "5441"
  )
  conn <- connect(connectionDetails = connectionDetails)
  resultsDatabaseSchema <- "alex_alexeyuk_results"
  cdmDatabaseSchema <- "cdm_531"
  cohortDatabaseSchema <- "alex_alexeyuk_results"
  databaseId <- "testDatabaseId"
  packageName <- "NSCLCCharacterization"
  # prepare necessary tables
  targetIdsFormatted <- c(101, 102, 103)
  # prepare necessary tables
  pathToSql <- system.file("sql", "sql_server",
                           "distributions", "IQRComplementaryTables.sql",
                           package = packageName
  )

  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  DatabaseConnector::renderTranslateExecuteSql(conn,
                                               sql = sql,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               cohortDatabaseSchema = cohortDatabaseSchema,
                                               cohortTable = "union_table",
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
    result <- NSCLCCharacterization::getAtEventDistribution(connection = conn,
                                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                                            cdmDatabaseSchema,
                                                            cohortTable = "union_table",
                                                            targetIds = targetIdsFormatted,
                                                            databaseId = databaseId,
                                                            packageName = packageName,
                                                            analysisName = analysis)

    metricsDistribution <- rbind(metricsDistribution, result)
  }

 expect_s3_class(metricsDistribution, "data.frame")

 expect_true(nrow(metricsDistribution)>0)


    pathToSql <- system.file("sql",
                             "sql_server",
                             "distributions",
                             "RemoveComplementaryTables.sql",
                             package = packageName
    )
    sql <- readChar(pathToSql, file.info(pathToSql)$size)
    expect_error(

    DatabaseConnector::renderTranslateExecuteSql(connection = conn,
                                                 sql = sql,
                                                 cohortDatabaseSchema = cohortDatabaseSchema
    ),
    NA)
})
