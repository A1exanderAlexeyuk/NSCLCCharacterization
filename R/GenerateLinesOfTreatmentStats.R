#' @importFrom magrittr %>%
#' @export
generateTreatmentStats <-   function(connection,
                                    cohortDatabaseSchema,
                                    regimenTable
                                    ){


  sqlFilesName <- c("TreatmentFreeInterval.sql",
                    "TimeToTreatmenDiscontinuation.sql",
                    "TimeToNextTreatment.sql")

  sql_s <- lapply(file.path(getPathToTreatmentStats(), sqlFilesName),
                  SqlRender::readSql)

  linesTreatmentOutput <- purrr::map_df(sql_s, function(sql){

    sql_tmp <-render(sql = sql,
                     cohort_database_schema = cohortDatabaseSchema,
                     regimen_stats = regimenTable)

    sql_tmp <- SqlRender::translate(sql, targetDialect = conn@dbms)

    data.frame(DatabaseConnector::querySql(connection=conn,
                                           sql = sql_tmp,
                                                    snakeCaseToCamelCase = T))


   })
  }

