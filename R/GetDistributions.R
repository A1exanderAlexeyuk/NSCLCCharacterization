#' @export
getAtEventDistribution <- function(connection,
                                   cohortDatabaseSchema,
                                   cdmDatabaseSchema,
                                   cohortTable,
                                   targetIds,
                                   outcomeId,
                                   databaseId,
                                   packageName,
                                   analysisName){
  targetIds <- paste(targetIds, collapse = ', ')

  sqlFileName <- paste(analysisName, 'sql', sep='.')

  analysisName <- substring(SqlRender::camelCaseToTitleCase(analysisName), 2)

  pathToSql <- system.file("sql",
                           "sql_server",
                           "distributions",
                           sqlFileName,
                           package = packageName)

  pathToAggregSql <- system.file("sql",
                                 "sql_server",
                                 "distributions",
                                 'DistributiveStatistics.sql',
                                 package = packageName)

  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  sqlAggreg <- readChar(pathToAggregSql, file.info(pathToAggregSql)$size)
  sql <- paste0(sql, sqlAggreg)
  sql <- SqlRender::render(sql, cohort_database_schema = cohortDatabaseSchema,
                           cdm_database_schema = cdmDatabaseSchema,
                           cohort_table = cohortTable,
                           target_ids = targetIds,
                           analysis_name = analysisName,
                           warnOnMissingParameters = FALSE)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms)

  data <- DatabaseConnector::querySql(connection = connection,
                                      sql = sql,
                                      snakeCaseToCamelCase = T)

  if (nrow(data) == 0) {
    ParallelLogger::logWarn("There is NO data for atEventDistribution")
    df <- data.frame(matrix(nrow = 0, ncol = 10))
    colnames(df) <- c("cohortDefinitionId",
                      "iqr",
                      "minimum",
                      "q1",
                      "median",
                      "q3",
                      "maximum",
                      "mean",
                      "std",
                      "analysisName")
    return(df)
  }

  return(
    data.frame(cohortDefinitionId = data$cohortDefinitionId,
               iqr = data$iqr,
               minimum = data$minimum,
               q1 = data$q1,
               median = data$median,
               q3 = data$q3,
               maximum = data$maximum,
               Mean = data$mean,
               StD = data$StD,
               analysisName = data$analysisName,
               databaseId = databaseId
               )
  )
}
