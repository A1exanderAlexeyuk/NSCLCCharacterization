#' @export
getAtEventDistribution <- function(connection,
                                   cohortDatabaseSchema,
                                   cdmDatabaseSchema,
                                   cohortTable,
                                   targetIds,
                                   databaseId,
                                   packageName,
                                   analysisName) {
  targetIds <- paste(targetIds, collapse = ", ")

  sqlFileName <- paste0(analysisName, ".sql"#, sep = "."
                        )

  analysisName <- substring(SqlRender::camelCaseToTitleCase(analysisName), 2)

  pathToSql <- system.file("sql",
    "sql_server",
    "distributions",
    sqlFileName,
    package = packageName
  )

  pathToAggregSql <- system.file("sql",
    "sql_server",
    "distributions",
    "DistributiveStatistics.sql",
    package = packageName
  )

  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  sqlAggreg <- readChar(pathToAggregSql, file.info(pathToAggregSql)$size)
  sql <- paste0(sql, sqlAggreg)
  sql <- SqlRender::render(sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortTable,
    target_ids = targetIds,
    analysis_name = analysisName,
    warnOnMissingParameters = FALSE
  )
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)

  data <- as.data.frame(DatabaseConnector::querySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = T
  ))


  if (nrow(data) == 0) {
    ParallelLogger::logWarn("There is NO data for atEventDistribution")
    df <- data.frame(matrix(nrow = 0, ncol = 10))
    colnames(df) <- c(
      "cohortDefinitionId",
      "iqr",
      "minimum",
      "q1",
      "median",
      "q3",
      "maximum",
      "mean",
      "std",
      "analysisName"
    )
    return(df)
  }

  return(
    data.frame(
      cohortDefinitionId = data$cohortDefinitionId,
      iqr = data$iqr,
      minimum = data$minimum,
      q1 = data$q1,
      median = data$median,
      q3 = data$q3,
      maximum = data$maximum,
      mean = data$mean,
      std = data$std,
      analysisName = data$analysisName,
      databaseId = databaseId
    )
  )
}
