# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of NSCLCCharacterization
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Create cohort table(s)
#'
#' @description
#' This function creates an empty cohort table. Optionally, additional empty tables are created to
#' store statistics on the various inclusion criteria.
#'
#'
#' @param createInclusionStatsTables   Create the four additional tables for storing inclusion rule
#'                                     statistics?
#' @param resultsDatabaseSchema        Schema name where the statistics tables reside. Note that for
#'                                     SQL Server, this should include both the database and schema
#'                                     name, for example 'scratch.dbo'.
#' @param cohortInclusionTable         Name of the inclusion table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortInclusionResultTable   Name of the inclusion result table, one of the tables for
#'                                     storing inclusion rule statistics.
#' @param cohortInclusionStatsTable    Name of the inclusion stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#' @param cohortSummaryStatsTable      Name of the summary stats table, one of the tables for storing
#'                                     inclusion rule statistics.
#'
#' @export
#'
createCohortTable <- function(connectionDetails = NULL,
                              connection = NULL,
                              cohortDatabaseSchema,
                              cohortTable = "cohort",
                              createInclusionStatsTables = FALSE,
                              resultsDatabaseSchema = cohortDatabaseSchema) {
  start <- Sys.time()
  ParallelLogger::logInfo("Creating cohort table")
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql("CreateCohortTable.sql",
    packageName = getThisPackageName(),
    dbms = connection@dbms,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  ParallelLogger::logDebug("- Created table ", cohortDatabaseSchema, ".", cohortTable)

  delta <- Sys.time() - start
  writeLines(paste("Creating cohort table took", signif(delta, 3), attr(delta, "units")))
}

#' Instantiate a set of cohort
#'
#' @description
#' This function instantiates a set of cohort in the cohort table, using definitions that are fetched from a WebApi interface.
#' Optionally, the inclusion rule statistics are computed and stored in the \code{inclusionStatisticsFolder}.
#'
#' @param cohortIds                   Optionally, provide a subset of cohort IDs to restrict the
#'                                    construction to.
#' @param generateInclusionStats      Compute and store inclusion rule statistics?
#' @param inclusionStatisticsFolder   The folder where the inclusion rule statistics are stored. Can be
#'                                    left NULL if \code{generateInclusionStats = FALSE}.
#' @param createCohortTable           Create the cohort table? been executed.
#'
#'
#' @export
instantiateCohortSet <- function(connectionDetails = NULL,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 tempEmulationSchema = NULL,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 cohortIds = NULL,
                                 createCohortTable = FALSE) {
  start <- Sys.time()
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  if (createCohortTable) {
    needToCreate <- TRUE
  } else {
    needToCreate <- FALSE
  }

  if (needToCreate) {
    createCohortTable(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable
    )
  }


  cohorts <- loadCohortsFromPackage(cohortIds = cohortIds)

  instantiatedCohortIds <- c()
  for (i in 1:nrow(cohorts)) {
    ParallelLogger::logInfo(i, "/", nrow(cohorts), ": Instantiation cohort ", cohorts$cohortFullName[i], "  (", cohorts$cohortId[i], ".sql)")
    sql <- cohorts$sql[i]

    sql <- SqlRender::render(sql,
      cdm_database_schema = cdmDatabaseSchema,
      vocabulary_database_schema = cdmDatabaseSchema,
      target_database_schema = cohortDatabaseSchema,
      target_cohort_table = cohortTable,
      target_cohort_id = cohorts$cohortId[i],
      warnOnMissingParameters = FALSE,
      episodetable = FALSE
    )

    sql <- SqlRender::translate(sql,
      targetDialect = connectionDetails$dbms,
      tempEmulationSchema = tempEmulationSchema
    )
    DatabaseConnector::executeSql(connection, sql)
    instantiatedCohortIds <- c(instantiatedCohortIds, cohorts$cohortId[i])
  }
  delta <- Sys.time() - start
  writeLines(paste("Instantiating cohort set took", signif(delta, 3), attr(delta, "units")))
}



createTempInclusionStatsTables <- function(connection, tempEmulationSchema, cohorts) {
  ParallelLogger::logInfo("Creating temporary inclusion statistics tables")
  pathToSql <- system.file("inclusionStatsTables.sql", package = "ROhdsiWebApi", mustWork = TRUE)
  sql <- SqlRender::readSql(pathToSql)
  sql <- SqlRender::translate(sql, targetDialect = connection@dbms, tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection, sql)

  inclusionRules <- data.frame()
  for (i in 1:nrow(cohorts)) {
    cohortDefinition <- RJSONIO::fromJSON(cohorts$json[i])
    if (!is.null(cohortDefinition$InclusionRules)) {
      nrOfRules <- length(cohortDefinition$InclusionRules)
      if (nrOfRules > 0) {
        for (j in 1:nrOfRules) {
          inclusionRules <- rbind(inclusionRules, data.frame(
            cohortId = cohorts$cohortId[i],
            ruleSequence = j - 1,
            ruleName = cohortDefinition$InclusionRules[[j]]$name
          ))
        }
      }
    }
  }
  inclusionRules <- merge(inclusionRules, data.frame(
    cohortId = cohorts$cohortId,
    cohortName = cohorts$cohortFullName
  ))
  inclusionRules <- data.frame(
    cohort_definition_id = inclusionRules$cohortId,
    rule_sequence = inclusionRules$ruleSequence,
    name = inclusionRules$ruleName
  )
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#cohort_inclusion",
    data = inclusionRules,
    dropTableIfExists = FALSE,
    createTable = FALSE,
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
}

saveAndDropTempInclusionStatsTables <- function(connection,
                                                tempEmulationSchema,
                                                inclusionStatisticsFolder,
                                                cohortIds) {
  fetchStats <- function(table, fileName) {
    ParallelLogger::logDebug("- Fetching data from ", table)
    sql <- "SELECT * FROM @table"
    data <- DatabaseConnector::renderTranslateQuerySql(
      sql = sql,
      connection = connection,
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE,
      table = table
    )
    fullFileName <- file.path(inclusionStatisticsFolder, fileName)
    readr::write_csv(data, fullFileName)
  }
  fetchStats("#cohort_inclusion", "cohortInclusion.csv")
  fetchStats("#cohort_inc_result", "cohortIncResult.csv")
  fetchStats("#cohort_inc_stats", "cohortIncStats.csv")
  fetchStats("#cohort_summary_stats", "cohortSummaryStats.csv")

  sql <- "TRUNCATE TABLE #cohort_inclusion;
    DROP TABLE #cohort_inclusion;

    TRUNCATE TABLE #cohort_inc_result;
    DROP TABLE #cohort_inc_result;

    TRUNCATE TABLE #cohort_inc_stats;
    DROP TABLE #cohort_inc_stats;

    TRUNCATE TABLE #cohort_summary_stats;
    DROP TABLE #cohort_summary_stats;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE,
    tempEmulationSchema = tempEmulationSchema
  )
}

copyAndCensorCohorts <- function(connection,
                                 cohortDatabaseSchema,
                                 cohortStagingTable,
                                 cohortTable,
                                 targetIds,
                                 tempEmulationSchema) {
  packageName <- getThisPackageName()

  sql <- SqlRender::loadRenderTranslateSql(
    dbms = attr(connection, "dbms"),
    sqlFilename = "CopyAndCensorCohorts.sql",
    packageName = packageName,
    tempEmulationSchema = tempEmulationSchema,
    warnOnMissingParameters = TRUE,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_staging_table = cohortStagingTable,
    cohort_table = cohortTable
  )

  ParallelLogger::logInfo("Copy and censor cohorts to main analysis table")
  DatabaseConnector::executeSql(connection, sql)
}
