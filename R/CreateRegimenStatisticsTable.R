#' Create regimen stats table
#'
#' @description
#' Computes features using all drugs, conditions, procedures, etc. observed on or prior to the cohort
#' index date.
#'
#'
#' @param regimenStatsTable  A table what will be created in cohortDatabaseSchema
#'
#' @param gapBetweenTreatment  To calculate time to treatment discontinuation
#'
#' @return
#' A data frame with cohort characteristics.
#'
#' @export
#'
createRegimenStats <- function(connectionDetails,
                               cdmDatabaseSchema,
                               writeDatabaseSchema,
                               cohortTable,
                               regimenStatsTable,
                               regimenIngredientsTable,
                               gapBetweenTreatment = 120) {
  cohortDatabaseSchema <-  writeDatabaseSchema
  packageName <- getThisPackageName()
  sqlFileName <- "CreateRegimenStatsTable.sql"
  pathToSql <- system.file("sql", "sql_server", "TreatmentAnalysis", sqlFileName, package = packageName)

  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  # sql <- SqlRender::readSql(file.path(
  #   getPathToTreatmentStats(),
  #   "CreateRegimenStatsTable.sql"
  # ))

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

#' @export
createCategorizedRegimensTable <- function(connectionDetails,
                                           cohortDatabaseSchema,
                                           regimenStatsTable,
                                           targetIds) {

  packageName <- getThisPackageName()
  sqlFileName <- "RegimenCategories.sql"
  pathToSql <- system.file("sql", "sql_server",
                           "TreatmentAnalysis", sqlFileName,
                           package = packageName)
  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  # sql <- readChar(pathToSql, file.info(pathToSql)$size)
  # sql <- SqlRender::readSql(file.path(
  #   getPathToTreatmentStats(),
  #   "RegimenCategories.sql"
  # ))


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
    as.data.frame(DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    ))

}
