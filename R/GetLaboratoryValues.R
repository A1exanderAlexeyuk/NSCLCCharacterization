#' @export
getLaboratoryValues <- function(
  connection,
  targetIds,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  labValuesTable,
  cohortTable,
  packageName,
  databaseId
){

    sqlFileName <- "PullAllLabValues.sql"
    pathToSql <- system.file("sql", "sql_server", sqlFileName, package = packageName)

    sql <- readChar(pathToSql, file.info(pathToSql)$size)

      sqlTmp <- SqlRender::render(
        sql = sql,
        cohortDatabaseSchema = cohortDatabaseSchema,
        lab_values_table = labValuesTable,
        targetIds  = target_id,
        cohortTable = cohortTable
      )
      sqlTmp <- SqlRender::translate(
        sql = sqlTmp,
        targetDialect = connection@dbms
      )

      labValues <- DatabaseConnector::querySql(
        connection = connection,
        sql = sqlTmp,
        snakeCaseToCamelCase = T
      )
  return(data.table::data.table(labValues))

}
