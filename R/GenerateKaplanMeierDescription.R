#' @importFrom magrittr %>%
#' @export
generateSurvival <- function(connection,
                             cohortDatabaseSchema,
                             cohortTable,
                             targetIds,
                             outcomeId,
                             packageName,
                             databaseId){

  sqlFileName <- "TimeToEvent.sql"
  pathToSql <- system.file("sql", "sql_server", sqlFileName, package = packageName)

  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  surv_outputs <- purrr::map_dfr(targetIds, function(targetId){

      sqlTmp <- SqlRender::render(sql,
                                   cohort_database_schema = cohortDatabaseSchema,
                                   cohort_table = cohortTable,
                                   outcome_id = outcomeId,
                                   target_id = targetId)
      sqlTmp <- SqlRender::translate(sql = sqlTmp,
                                      targetDialect = connection@dbms)

      kmRaw <- DatabaseConnector::querySql(connection = connection,
                                            sql = sqlTmp,
                                            snakeCaseToCamelCase = T)

      ## edit
      if(nrow(kmRaw) < 100 | length(kmRaw$event[kmRaw$event == 1]) < 1){return(NULL)}

      km_proc <- kmRaw %>%
        dplyr::mutate(timeToEvent = as.integer(as.Date(eventDate) - as.Date(cohortStartDate)),
                      id = dplyr::row_number()) %>%
        dplyr::select(id, timeToEvent, event)

      survInfo <- survival::survfit(survival::Surv(timeToEvent, event) ~ 1, data = km_proc)

      survInfo <- survminer::surv_summary(survInfo)

      data.frame(targetId = targetId,
                 outcomeId = outcomeId,
                 time = survInfo$time,
                 surv = survInfo$surv,
                 n.censor = survInfo$n.censor,
                 n.event = survInfo$n.event,
                 n.risk = survInfo$n.risk,
                 lower = survInfo$lower,
                 upper = survInfo$upper,
                 databaseId = databaseId)

  })
}

# K-M info for TimeToNextTreatment

#' @export
generateKaplanMeierDescriptionTNT <-   function(connection,
                                               cohortDatabaseSchema,
                                               regimenStatsTable,
                                               cohortTable,
                                               targetIds,
                                               databaseId){


  sqlFilesName <- c("TimeToNextTreatment.sql")

  sql <- SqlRender::readSql(file.path(getPathToTreatmentStats(),
                                        sqlFilesName))

  linesTreatmentOutput <- purrr::map_dfr(targetIds, function(targetId){

      sqlTmp <-render(sql = sql,
                       cohort_database_schema = cohortDatabaseSchema,
                       targetId = targetId,
                       regimenStatsTable = regimenStatsTable)

      sqlTmp <- SqlRender::translate(sql, targetDialect = conn@dbms)

      km_proc <- as.data.frame(DatabaseConnector::querySql(connection=conn,
                                                           sql = sqlTmp,
                                                           snakeCaseToCamelCase = T))

      survInfo <- survival::survfit(survival::Surv(timeToEvent, event) ~ 1, data = km_proc)


      survInfo <- survminer::surv_summary(survInfo)

      data.frame(targetId = targetId,
                 outcomeId = "TimeToNextTreatment",
                 time = survInfo$time,
                 surv = survInfo$surv,
                 n.censor = survInfo$n.censor,
                 n.event = survInfo$n.event,
                 n.risk = survInfo$n.risk,
                 lower = survInfo$lower,
                 upper = survInfo$upper,
                 databaseId = databaseId)


  })
}

# K-M info for TreatmentFreeInterval and TimeToTreatmenDiscontinuation
#' @export
generateKaplanMeierDescriptionTFITTD <- function(connection,
                                                 cohortDatabaseSchema,
                                                 regimenStatsTable,
                                                 targetIds,
                                                 databaseId){



  sqlFilesName <- c("TreatmentFreeInterval.sql",
                    "TimeToTreatmenDiscontinuation.sql")

  outcome <- 'Time_to_Treatment_Discontinuation'

  getPathToTreatmentStats <- function() {
    return("inst/sql/sql_server/TreatmentAnalysis")
  }

  sql_s <- lapply(file.path(getPathToTreatmentStats(), sqlFilesName),
                  SqlRender::readSql)

  linesTreatmentOutput <- purrr::map_dfr(sql_s, function(sql){
    if(is.na(stringr::str_locate(sql,
                                 outcome)[1])){
      outcomeId = "TreatmentFreeInterval"
    } else {outcomeId = "TimeToTreatmenDiscontinuation"}

    sqlTmp <-render(sql = sql,
                     cohort_database_schema = cohortDatabaseSchema,
                     targetId = targetId,
                     regimenStatsTable = regimenStatsTable)

    sqlTmp <- SqlRender::translate(sql,
                                    targetDialect = conn@dbms)

    km_proc <- as.data.frame(DatabaseConnector::querySql(connection=conn,
                                                         sql = sqlTmp,
                                                         snakeCaseToCamelCase = T))

    nest_km <- km_proc %>%
      tidyr::nest(data = !lineOfTherapy) %>%
      dplyr::mutate(survInfo = purrr::map(data,
                                    ~survival::survfit(survival::Surv(timeToEvent,
                                                                      event)
                                                             ~ 1, data = .)))

    survivalSummary <- survInfo %>%
      dplyr::mutate(result = purrr::map(nest_km, broom::tidy)) %>%
      dplyr::select(targetId = targetId, outcomeId = outcomeId,
             lineOfTherapy, result, databaseId = databaseId) %>%
      tidyr::unnest(cols = c(result))

    data.frame(targetId = targetId,
               outcomeId = outcomeId,
               lineOfTherapy = lineOfTherapy,
               time = survivalSummary$time,
               surv = survivalSummary$estimate,
               n.censor = survivalSummary$n.censor,
               n.event = survivalSummary$n.event,
               n.risk = survivalSummary$n.risk,
               lower = survivalSummary$conf.low,
               upper = survivalSummary$conf.high,
               databaseId = databaseId)

  })
}


