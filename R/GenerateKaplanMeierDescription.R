#' @importFrom magrittr %>%
#' @export
generateSurvival <- function(connection,
                             cohortDatabaseSchema,
                             cohortTable,
                             targetIds,
                             outcomeId,
                             packageName,
                             databaseId) {
  sqlFileName <- "TimeToEvent.sql"
  pathToSql <- system.file("sql", "sql_server", sqlFileName, package = packageName)

  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  survOutputs <- purrr::map_dfr(targetIds, function(targetId) {
    sqlTmp <- SqlRender::render(sql,
                                cohort_database_schema = cohortDatabaseSchema,
                                cohort_table = cohortTable,
                                outcome_id = outcomeId,
                                target_id = targetId
    )
    sqlTmp <- SqlRender::translate(
      sql = sqlTmp,
      targetDialect = connection@dbms
    )

    kmRaw <- DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    )

    ## edit
    if (nrow(kmRaw) < 100 | length(kmRaw$event[kmRaw$event == 1]) < 1) {
      return(NULL)
    }

    km_proc <- kmRaw %>%
      dplyr::mutate(
        timeToEvent = as.integer(as.Date(eventDate) - as.Date(cohortStartDate)),
        id = dplyr::row_number()
      ) %>%
      dplyr::select(id, timeToEvent, event)

    survInfo <- survival::survfit(survival::Surv(timeToEvent, event) ~ 1, data = km_proc)

    survInfo <- survminer::surv_summary(survInfo)

    data.frame(
      targetId = targetId,
      outcomeId = outcomeId,
      time = survInfo$time,
      surv = survInfo$surv,
      n.censor = survInfo$n.censor,
      n.event = survInfo$n.event,
      n.risk = survInfo$n.risk,
      lower = survInfo$lower,
      upper = survInfo$upper,
      databaseId = databaseId
    )
  })
}

# K-M info for TimeToNextTreatment

#' @export
generateKaplanMeierDescriptionTNT <- function(connection,
                                              cohortDatabaseSchema,
                                              regimenStatsTable,
                                              targetIds,
                                              databaseId) {
  packageName <- getThisPackageName()
  sqlFileName <- "TimeToNextTreatment.sql"
  pathToSql <- system.file("sql", "sql_server", "TreatmentAnalysis",
                           sqlFileName,
                           package = packageName
  )

  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  linesTreatmentOutput <- purrr::map_dfr(targetIds, function(targetId) {
    sqlRendered <- SqlRender::render(
      sql = sql,
      cohortDatabaseSchema = cohortDatabaseSchema,
      targetId = targetId,
      regimenStatsTable = regimenStatsTable
    )

    sqlTmp <- SqlRender::translate(
      sql = sqlRendered,
      targetDialect = connection@dbms
    )

    km_proc <- as.data.frame(DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    ))

    if(nrow(km_proc) < 100 | length(km_proc$event[km_proc$event == 1]) < 1){
      return(NULL)
    }

        survInfo <- survival::survfit(survival::Surv(timeToEvent, event) ~ 1,
                                  data = km_proc
                                  )


    survInfo <- survminer::surv_summary(survInfo)

    data.frame(
      targetId = targetId,
      outcomeId = "TimeToNextTreatment",
      time = survInfo$time,
      surv = survInfo$surv,
      n.censor = survInfo$n.censor,
      n.event = survInfo$n.event,
      n.risk = survInfo$n.risk,
      lower = survInfo$lower,
      upper = survInfo$upper,
      databaseId = databaseId
    )
  })
}

#' @export
generateKaplanMeierDescriptionTFI <- function(connection,
                                              cohortDatabaseSchema,
                                              regimenStatsTable,
                                              targetIds,
                                              databaseId) {
  packageName <- getThisPackageName()
  sqlFileName <- "TreatmentFreeInterval.sql"
  pathToSql <- system.file("sql", "sql_server", "TreatmentAnalysis",
                           sqlFileName,
                           package = packageName
  )
  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  linesTreatmentOutput <- purrr::map_dfr(targetIds, function(targetId) {
    sqlRendered <- SqlRender::render(
      sql = sql,
      cohortDatabaseSchema = cohortDatabaseSchema,
      targetId = targetId,
      regimenStatsTable = regimenStatsTable
    )

    sqlTmp <- SqlRender::translate(
      sql = sqlRendered,
      targetDialect = connection@dbms
    )

    km_proc <- as.data.frame(DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    ))

    if(nrow(km_proc) < 100 | length(km_proc$event[km_proc$event == 1]) < 1){
      return(NULL)
    }

    km_proc_2 <- km_proc %>%
      tidyr::nest(data = !lineOfTherapy) %>%
      dplyr::mutate(survfit_output = purrr::map(
        data, ~ survival::survfit(
          survival::Surv(
            timeToEvent, event
          ) ~ 1,
          data = .
        )
      ))


    survivalSummary <- km_proc_2 %>%
      dplyr::mutate(result = purrr::map(survfit_output, broom::tidy)) %>%
      dplyr::select(lineOfTherapy, result) %>%
      tidyr::unnest(cols = c(result))


    data.frame(
      targetId = targetId,
      outcomeId = "TreatmentFreeInterval",
      lineOfTherapy = survivalSummary$lineOfTherapy,
      time = survivalSummary$time,
      surv = survivalSummary$estimate,
      n.censor = survivalSummary$n.censor,
      n.event = survivalSummary$n.event,
      n.risk = survivalSummary$n.risk,
      lower = survivalSummary$conf.low,
      upper = survivalSummary$conf.high,
      databaseId = databaseId
    )
  })
}



#' @export
generateKaplanMeierDescriptionTTD <- function(connection,
                                              cohortDatabaseSchema,
                                              regimenStatsTable,
                                              targetIds,
                                              databaseId) {
  packageName <- getThisPackageName()
  sqlFileName <- "TimeToTreatmentDiscontinuation.sql"
  pathToSql <- system.file("sql", "sql_server", "TreatmentAnalysis",
                           sqlFileName,
                           package = packageName
  )
  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  linesTreatmentOutput <- purrr::map_df(targetIds, function(targetId) {
    sqlRendered <- SqlRender::render(
      sql = sql,
      cohortDatabaseSchema = cohortDatabaseSchema,
      targetId = targetId,
      regimenStatsTable = regimenStatsTable
    )

    sqlTmp <- SqlRender::translate(
      sql = sqlRendered,
      targetDialect = connection@dbms
    )

    km_proc <- as.data.frame(DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    ))

    if(nrow(km_proc) < 100 | length(km_proc$event[km_proc$event == 1]) < 1){
      return(NULL)
    }

    km_proc_2 <- km_proc %>%
      tidyr::nest(data = !lineOfTherapy) %>%
      dplyr::mutate(survfit_output = purrr::map(
        data, ~ survival::survfit(
          survival::Surv(
            timeToEvent, event
          ) ~ 1,
          data = .
        )
      ))


    survivalSummary <- km_proc_2 %>%
      dplyr::mutate(result = purrr::map(survfit_output, broom::tidy)) %>%
      dplyr::select(lineOfTherapy, result) %>%
      tidyr::unnest(cols = c(result))


    data.frame(
      targetId = targetId,
      outcomeId = "TimeToTreatmentDiscontinuation",
      lineOfTherapy = survivalSummary$lineOfTherapy,
      time = survivalSummary$time,
      surv = survivalSummary$estimate,
      n.censor = survivalSummary$n.censor,
      n.event = survivalSummary$n.event,
      n.risk = survivalSummary$n.risk,
      lower = survivalSummary$conf.low,
      upper = survivalSummary$conf.high,
      databaseId = databaseId
    )
  })
}


#' @export
generateTimeToTreatmenInitiationStatistics <- function(connection,
                                                       cohortDatabaseSchema,
                                                       targetIds,
                                                       outcomeId, # treatment initiation
                                                       databaseId,
                                                       cohortTable) {

  packageName <- getThisPackageName()
  sqlFileName <- "TimeToTreatmentInitiation.sql"
  pathToSql <- system.file("sql", "sql_server", "TreatmentAnalysis",
                           sqlFileName,
                           package = packageName
  )
  sql <- readChar(pathToSql, file.info(pathToSql)$size)



  survOutputs <- purrr::map_df(targetIds, function(targetId) {
    sqlRendered <- SqlRender::render(
      sql = sql,
      cohortDatabaseSchema = cohortDatabaseSchema,
      outcomeId = outcomeId,
      targetId = targetId,
      databaseId = databaseId,
      cohortTable = cohortTable
    )

    sqlTmp <- SqlRender::translate(
      sql = sqlRendered,
      targetDialect = connection@dbms
    )

    timeToTreatmentInitiation <- data.frame(DatabaseConnector::querySql(
      connection = connection,
      sql = sqlTmp,
      snakeCaseToCamelCase = T
    ))
  })
}



#' @export
generateTreatmentStatistics <- function(connection,
                                        cohortDatabaseSchema,
                                        targetIds,
                                        regimenStatsTable,
                                        databaseId) {

  packageName <- getThisPackageName()
  sqlFileNames <- c(
    "TimeToNextTreatmentDistribution.sql",
    "TimeToTreatmentDiscontinuationDistribution.sql",
    "TreatmentFreeIntervalDistribution.sql"
                    )

  distributionOut <- purrr::map_dfr(targetIds, function(targetId) {
    purrr::map_dfr(sqlFileNames, function(sqlFileName){
      pathToSql <- system.file("sql", "sql_server", "TreatmentAnalysis",
                               sqlFileName,
                               package = packageName
      )

      sql <- readChar(pathToSql, file.info(pathToSql)$size)


      sqlRendered <- SqlRender::render(
        sql = sql,
        cohortDatabaseSchema = cohortDatabaseSchema,
        targetId = targetId,
        databaseId = databaseId,
        regimenStatsTable = regimenStatsTable
      )

      sqlTmp <- SqlRender::translate(
        sql = sqlRendered,
        targetDialect = connection@dbms
      )

      data.frame(DatabaseConnector::querySql(
        connection = connection,
        sql = sqlTmp,
        snakeCaseToCamelCase = T
      ))

      })
  })
  return(distributionOut)
}
