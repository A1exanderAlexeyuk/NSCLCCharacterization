library(DatabaseConnector)
library(SqlRender)
library(lubridate)
library(testthat)
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "testnode.arachnenetwork.com/synpuf_110k",
                                             user = "ohdsi",
                                             password = 'ohdsi',
                                             port = "5441",
                                             pathToDriver = 'c:/jdbcDrivers')
conn <- connect(connectionDetails)
generateTreatmentStats <-   function(connection,
                                     cohortDatabaseSchema = 'alex_alexeyuk_results',
                                     regimenTable =  "stats_test_lines"
                                     #,
                                     #targetIds
                                     #,
                                     #outcomeIds,
                                     #databaseId,
                                     #packageName
                                     )
  {
  
  sqlFilesName <- c("TreatmentFreeInterval.sql",
                    "TimeToTreatmenDiscontinuation.sql"#,
  #                   #"TimeToNextTreatment.sql"
                 )
  sql_ <- lapply(sqlFilesName, SqlRender::readSql)
  
  output <- purrr::map_df(sql_, function(sql){
    
  
    sql_tmp <-render(sql, 
               cohort_database_schema='alex_alexeyuk_results',
                regimen_stats = "stats_test_lines")
  
  #sql_tmp <- SqlRender::translate(sql, targetDialect = conn@dbms)
  
  print(as.data.frame(DatabaseConnector::querySql(connection=conn, 
                                            sql_tmp, 
                                            snakeCaseToCamelCase = T)))
  
  }
  )
  
  
  }



generateTreatmentStats(conn)
