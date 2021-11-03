#' @importFrom magrittr %>%
#' @export
generateSurvival <- function(connection,
                             cohortDatabaseSchema,
                             cohortTable,
                             targetIds,
                             outcomeId,
                             databaseId,
                             packageName){

  sqlFileName <- "TimeToEvent.sql"
  pathToSql <- system.file("sql", "sql_server", sqlFileName, package = packageName)

  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  surv_outputs <- purrr::map_df(targetIds, function(targetId){

      sql_tmp <- SqlRender::render(sql,
                                   cohort_database_schema = cohortDatabaseSchema,
                                   cohort_table = cohortTable,
                                   outcome_id = outcomeId,
                                   target_id = targetId)
      sql_tmp <- SqlRender::translate(sql = sql_tmp,
                                      targetDialect = connection@dbms)

      km_raw <- DatabaseConnector::querySql(connection = connection,
                                            sql = sql_tmp,
                                            snakeCaseToCamelCase = T)

      ## edit
      if(nrow(km_raw) < 100 | length(km_raw$event[km_raw$event == 1]) < 1){return(NULL)}

      km_proc <- km_raw %>%
        dplyr::mutate(timeToEvent = as.integer(as.Date(eventDate) - as.Date(cohortStartDate)),
                      id = dplyr::row_number()) %>%
        dplyr::select(id, timeToEvent, event)

      surv_info <- survival::survfit(survival::Surv(timeToEvent, event) ~ 1, data = km_proc)

      surv_info <- survminer::surv_summary(surv_info)

      data.frame(targetId = targetId, outcomeId = outcomeId, time = surv_info$time, surv = surv_info$surv,
                 n.censor = surv_info$n.censor, n.event = surv_info$n.event, n.risk = surv_info$n.risk,
                 lower = surv_info$lower, upper = surv_info$upper, databaseId = databaseId)



  })
}

#' @export
generateTimeToTreatmenInitiationStatistics <- function(connection,
                                                      cohortDatabaseSchema,
                                                      cohortTable,
                                                      targetIds,
                                                      outcomeId
                                                      ){

  sql <- SqlRender::readSql(file.path(getPathToTreatmentStats(),
                                      "TimeToTreatmentInitiation.sql"))

  surv_outputs <- purrr::map_df(targetIds, function(targetId){

      sql_tmp <- SqlRender::render(sql = sql,
                                   cohort_database_schema = cohortDatabaseSchema,
                                   cohort_table = cohortTable,
                                   outcome_id = outcomeId,
                                   target_id = targetId)

      sql_tmp <- SqlRender::translate(sql = sql_tmp,
                                      targetDialect = connection@dbms)

      timeToTreatmentInitiation <- data.frame(DatabaseConnector::querySql(connection = connection,
                                                   sql = sql_tmp,
                                                   snakeCaseToCamelCase = T))

  })
}
