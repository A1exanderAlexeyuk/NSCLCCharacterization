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

#' Create characterization of a cohort
#'
#' @description
#' Computes features using all drugs, conditions, procedures, etc. observed on or prior to the cohort
#' index date.
#'
#' @param cohortId            The cohort definition ID used to reference the cohort in the cohort
#'                           = table.
#' @param covariateSettings   Either an object of type \code{covariateSettings} as created using one of
#'                            the createCovariate functions in the FeatureExtraction package, or a list
#'                            of such objects.
#'
#' @return
#' A data frame with cohort characteristics.
#'
#' @export
getCohortCharacteristics <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cdmDatabaseSchema,
                                     oracleTempSchema = NULL,
                                     cohortDatabaseSchema = cdmDatabaseSchema,
                                     cohortTable = "cohort",
                                     cohortId,
                                     covariateSettings = FeatureExtraction::createDefaultCovariateSettings(
                                       includedCovariateConceptIds = getCovariatesToInclude(),
                                       addDescendantsToInclude = TRUE,
                                       excludedCovariateConceptIds = 4162276, # melanoma
                                       addDescendantsToExclude = TRUE
                                     )) {
  if (!file.exists(getOption("fftempdir"))) {
    stop(
      "This function uses ff, but the fftempdir '",
      getOption("fftempdir"),
      "' does not exist. Either create it, or set fftempdir
         to another location using options(fftempdir = \"<path>\")"
    )
  }

  start <- Sys.time()

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!checkIfCohortInstantiated(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId
  )) {
    warning("Cohort with ID ", cohortId, " appears to be empty. Was it instantiated?")
    delta <- Sys.time() - start
    ParallelLogger::logInfo(paste(
      "Cohort characterization took",
      signif(delta, 3),
      attr(delta, "units")
    ))
    return(data.frame())
  }

  data <- FeatureExtraction::getDbCovariateData(
    connection = connection,
    oracleTempSchema = oracleTempSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId,
    covariateSettings = covariateSettings,
    aggregated = TRUE
  )
  result <- data.frame()
  if (!is.null(data$covariates)) {
    counts <- as.numeric(ff::as.ram(data$covariates$sumValue))
    n <- data$metaData$populationSize
    binaryCovs <- data.frame(
      covariateId = ff::as.ram(data$covariates$covariateId),
      mean = ff::as.ram(data$covariates$averageValue)
    )

    binaryCovs$sd <- sqrt((n * counts + counts) / (n^2))
    result <- rbind(result, binaryCovs)
  }
  if (!is.null(data$covariatesContinuous)) {
    continuousCovs <- data.frame(
      covariateId = ff::as.ram(data$covariatesContinuous$covariateId),
      mean = ff::as.ram(data$covariatesContinuous$averageValue),
      sd = ff::as.ram(data$covariatesContinuous$standardDeviation)
    )
    result <- rbind(result, continuousCovs)
  }
  if (nrow(result) > 0) {
    result <- merge(result, ff::as.ram(data$covariateRef))
    result$conceptId <- NULL
  }
  attr(result, "cohortSize") <- data$metaData$populationSize
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste(
    "Cohort characterization took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  return(result)
}

checkIfCohortInstantiated <- function(connection,
                                      cohortDatabaseSchema,
                                      cohortTable,
                                      cohortId) {
  sql <- "SELECT COUNT(*) FROM @cohortDatabaseSchema@cohortTable WHERE cohort_definition_id = @cohortId;"
  count <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortId = cohortId
  )
  return(count > 0)
}
