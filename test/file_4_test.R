library(magrittr)
generateSurvival <- function(connection, cohortDatabaseSchema, cohortTable, targetIds, outcomeIds){
  sql <- readSql("TimeToEvent.sql")
  surv_outputs <- purrr::map_df(targetIds, function(targetId){
    
    purrr::map_df(outcomeIds, function(outcomeId){
      
      sql_tmp <- SqlRender::render(sql, cohort_database_schema = cohortDatabaseSchema,
                                   cohort_table = cohortTable, outcome_id = outcomeId, target_id = targetId)
      sql_tmp <- SqlRender::translate(sql_tmp, targetDialect = connection@dbms)
      
      km_raw <- DatabaseConnector::querySql(connection, sql_tmp, snakeCaseToCamelCase = T)
      
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
  })
}


generateTimeToEvent <- function(connection, 
                                cohortDatabaseSchema, 
                                cohortTable, 
                                targetId, 
                                outcomeId){
  sql <- readSql("TimeToOutcome.sql")
  sql_tmp <- SqlRender::render(sql, cohort_database_schema = cohortDatabaseSchema,
                               cohort_table = cohortTable, 
                               outcome_id = outcomeId, target_ids = targetId)
  sql_tmp <- SqlRender::translate(sql_tmp, targetDialect = connection@dbms)
  
  as.data.frame(DatabaseConnector::querySql(connection, sql_tmp, snakeCaseToCamelCase = T))
                          
  
}



generateLinesOfTreatment <- function(connection, 
                                    cohortDatabaseSchema, 
                                    regimenTable 
                                    ){
  
  sql <- readSql("regimen_table_test.sql")
  sql_tmp <- SqlRender::render(sql, 
                               cohort_database_schema = cohortDatabaseSchema,
                               regimenTable = regimenTable 
                               )
  as.data.frame(DatabaseConnector::querySql(connection, sql_tmp, snakeCaseToCamelCase = T))
  
  
}