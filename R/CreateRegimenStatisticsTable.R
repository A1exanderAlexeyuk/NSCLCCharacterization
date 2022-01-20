#' Create regimen stats table
#' @return
#' The function returns nothing. As side effect it creates regimen stats table in write database schema
#'
#' @export

createRegimenStats <- function(connectionDetails,
                               cdmDatabaseSchema,
                               writeDatabaseSchema,
                               cohortTable,
                               regimenStatsTable,
                               regimenIngredientsTable,
                               gapBetweenTreatment = 120) {
  cohortDatabaseSchema <- writeDatabaseSchema
  packageName <- getThisPackageName()
  sqlFileName <- "CreateRegimenStatsTable.sql"
  pathToSql <- system.file("sql", "sql_server", "TreatmentAnalysis",
                           sqlFileName, package = packageName)

  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  sqlRendered <- SqlRender::render(
    sql = sql,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    regimenStatsTable = regimenStatsTable,
    regimenIngredientsTable = regimenIngredientsTable,
    gapBetweenTreatment = gapBetweenTreatment
  )

  sqlTranslated <- SqlRender::translate(
    sql = sqlRendered,
    targetDialect = connectionDetails$dbms
  )
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sqlTranslated
  )
}


#' @returns dataframe with categories of treatment
#' @export
createCategorizedRegimensTable <- function(connectionDetails,
                                           cohortDatabaseSchema,
                                           regimenStatsTable,
                                           targetIds,
                                           databaseId) {
  packageName <- getThisPackageName()
  sqlFileName <- "RegimenCategories.sql"
  pathToSql <- system.file("sql", "sql_server",
    "TreatmentAnalysis", sqlFileName,
    package = packageName
  )
  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  sqlTmp <- SqlRender::render(
    sql = sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIds
  )

  sqlTmp <- SqlRender::translate(
    sql = sqlTmp,
    targetDialect = connectionDetails$dbms
  )
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  categories <- DatabaseConnector::querySql(
    connection = connection,
    sql = sqlTmp,
    snakeCaseToCamelCase = T
  )

  data.frame(
    targetId = categories$cohortDefinitionId,
    personId = categories$personId,
    lineOfTherapy = categories$lineOfTherapy,
    regimen = categories$regimen,
    databaseId = databaseId
  )
}
