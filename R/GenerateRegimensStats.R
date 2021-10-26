#' export

generateRegimensStats <- function(connection, 
                             cohortDatabaseSchema, 
                             regimenIngredientTable, 
                             cohortTable,
                             targetIds, 
                             linesOfTreatment, 
                             #databaseId, 
                             #packageName
                             ){
  sqlFileName <- "Lines_of_therapy_stats.sql"
  pathToSql <- system.file("sql", "sql_server", sqlFileName, package = packageName)
  sql <- readChar(pathToSql, file.info(pathToSql)$size)
      sql_tmp <- SqlRender::render(sql, 
                                   cohort_database_schema = cohortDatabaseSchema,
                                   cohortTable = cohortTable, 
                                   linesOfTreatment = linesOfTreatment, 
                                   targetIds = targetIds)
      sql_tmp <- SqlRender::translate(sql = sql_tmp, 
                                      targetDialect = connection@dbms)
      
      stats_raw <- DatabaseConnector::querySql(connection = connection, 
                                              sql = sql_tmp, 
                                              snakeCaseToCamelCase = T)
