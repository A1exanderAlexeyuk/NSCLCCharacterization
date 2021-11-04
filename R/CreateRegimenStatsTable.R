#' Create regimen stats table
#'
#' @description
#' Computes features using all drugs, conditions, procedures, etc. observed on or prior to the cohort
#' index date.
#'
#' @template Connection
#'
#' @template CdmDatabaseSchema
#'
#' @template cohortDatabaseSchema
#'
#' @template cohortTable
#'
#' @template regimenIngredientsTable  An OncoRegimenFinder output table
#'
#' @param regimenStatsTable  A table what will be created in cohortDatabaseSchema
#'
#' @param gapBetweenTreatment  To calculate time to treatment discontinuation
#'
#' @return
#' A data frame with cohort characteristics.
#'
#' @export
createRegimenStats <- function( connection,
                                cdmDatabaseSchema,
                                cohortDatabaseSchema,
                                cohortTable,
                                regimenStatsTable,
                                regimenIngredientsTable,
                                gapBetweenTreatment = 120){

  sql <- SqlRender::readSql(file.path(getPathToTreatmentStats(),
                            "CreateRegimenStatsTable.sql"))

  sqlRendered <- SqlRender::render(sql = sql,
                                   cdmDatabaseSchema = cdmDatabaseSchema,
                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                   cohortTable = cohortTable,
                                   regimenStatsTable = regimenStatsTable,
                                   regimenIngredientsTable = regimenIngredientsTable,
                                   gapBetweenTreatment = gapBetweenTreatment)

  sqlTranslated <- SqlRender::translate(sql = sqlRendered,
                                        targetDialect = connection@dbms)

  DatabaseConnector::executeSql(connection = connection,
                                sql = sqlTranslated)
}
