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
createRegimenStats <- function(connection,
                               cdmDatabaseSchema,
                               cohortDatabaseSchema,
                               cohortTable,
                               regimenStatsTable,
                               regimenIngredientsTable,
                               gapBetweenTreatment = 120) {
  sql <- SqlRender::readSql(file.path(
    getPathToTreatmentStats(),
    "CreateRegimenStatsTable.sql"
  ))

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
    targetDialect = connection@dbms
  )

  DatabaseConnector::executeSql(
    connection = connection,
    sql = sqlTranslated
  )
}

#' @export
createCategorizedRegimensTable <- function(connection,
                                           cohortDatabaseSchema,
                                           cohortTable,
                                           regimenStatsTable,
                                           targetIds) {
  sql <- SqlRender::readSql(file.path(
    getPathToTreatmentStats(),
    "RegimenCategories.sql"
  ))

  categorizedRegimensOutput <- purrr::map_df(targetIds, function(targetId) {
    sqlTmp <- SqlRender::render(
      sql = sql,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      targetId = targetId
    )

    sqlTmp <- SqlRender::translate(
      sql = sqlTmp,
      targetDialect = connection@dbms
    )

    as.data.frame(DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    ))
  })
}