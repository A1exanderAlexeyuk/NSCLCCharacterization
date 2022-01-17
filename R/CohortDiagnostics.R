#' Execute the cohort diagnostics
#'
#' @details
#' This function executes the cohort diagnostics.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where intermediate data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the work database schema.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param tempEmulationSchema  Should be used in Oracle, BigQuery, impala to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @param databaseId           A short string for identifying the database (e.g.
#'                             'Synpuf').
#' @param databaseName         The full name of the database (e.g. 'Medicare Claims
#'                             Synthetic Public Use Files (SynPUFs)').
#' @param databaseDescription  A short description (several sentences) of the database.
#' @param createCohorts        Create the cohortTable table with the exposure and outcome cohorts?
#' @param runInclusionStatistics      Generate and export statistic on the cohort incusion rules?
#' @param runIncludedSourceConcepts   Generate and export the source concepts included in the cohorts?
#' @param runOrphanConcepts           Generate and export potential orphan concepts?
#' @param runTimeDistributions        Generate and export cohort time distributions?
#' @param runBreakdownIndexEvents     Generate and export the breakdown of index events?
#' @param runIncidenceRates      Generate and export the cohort incidence rates?
#' @param runCohortOverlap            Generate and export the cohort overlap?
#' @param runCohortCharacterization   Generate and export the cohort characterization?
#' @export
runCohortDiagnostics <- function(connectionDetails,
                                 connection = NULL,
                                 cdmDatabaseSchema,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 tempEmulationSchema = NULL,
                                 outputFolder,
                                 databaseId = "Unknown",
                                 databaseName = "Unknown",
                                 databaseDescription = "Unknown",
                                 createCohorts = TRUE,
                                 #cohortStagingTable = "cohort_stg",
                                 runInclusionStatistics = FALSE,
                                 runIncludedSourceConcepts = TRUE,
                                 runOrphanConcepts = TRUE,
                                 runTimeDistributions = TRUE,
                                 runBreakdownIndexEvents = TRUE,
                                 runIncidenceRates = TRUE,
                                 runCohortOverlap = TRUE,
                                 covariateSettings = FeatureExtraction::createDefaultCovariateSettings(
                                   includedCovariateConceptIds = getCovariatesToInclude(),
                                   addDescendantsToInclude = TRUE,
                                   excludedCovariateConceptIds = 4162276, # melanoma
                                   addDescendantsToExclude = TRUE
                                 ),
                                 runCohortCharacterization = TRUE,
                                 cohortIdsToExcludeFromExecution = NULL,
                                 cohortGroups = getUserSelectableCohortGroups()) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE)
  }

  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  #on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  #on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)

  if (createCohorts) {
    # Instantiate cohorts -----------------------------------------------------------------------
    cohorts <- getCohortsToCreate()

    # Remove any cohorts that are to be excluded
    targetCohortIds <- cohorts[cohorts$cohortType == 'target', "cohortId"][[1]] #
    outcomeCohortIds <- cohorts[cohorts$cohortType == "outcome", "cohortId"][[1]]
    # Start with the target cohorts
    ParallelLogger::logInfo("**********************************************************")
    ParallelLogger::logInfo("  ---- Creating target cohorts ---- ")
    ParallelLogger::logInfo("**********************************************************")
    instantiateCohortSet(connectionDetails = connectionDetails,
                         connection = connection,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         tempEmulationSchema = tempEmulationSchema,
                         cohortDatabaseSchema = cohortDatabaseSchema,
                         cohortTable = cohortTable,
                         cohortIds = targetCohortIds,
                         createCohortTable = TRUE
    )
    # Create the outcome cohorts
    ParallelLogger::logInfo("**********************************************************")
    ParallelLogger::logInfo(" ---- Creating outcome cohorts ---- ")
    ParallelLogger::logInfo("**********************************************************")
    instantiateCohortSet(
      connectionDetails = connectionDetails,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortIds = outcomeCohortIds
    )

  }
  cohortIds <- getCohortsToCreate()$cohortId
  cohortToCreateFile <- getCohortGroupsForDiagnostics()$fileName[1]
  ParallelLogger::logInfo("Running study diagnostics")

  CohortDiagnostics::runCohortDiagnostics(
    packageName = getThisPackageName(),
    cohortToCreateFile = cohortToCreateFile,
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohortIds,
    inclusionStatisticsFolder = outputFolder,
    exportFolder = file.path(outputFolder, "diagnosticsExport"),
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    runInclusionStatistics = TRUE,
    runIncludedSourceConcepts = TRUE,
    runOrphanConcepts = runOrphanConcepts,
    runTimeDistributions = runTimeDistributions,
    runBreakdownIndexEvents = runBreakdownIndexEvents,
    runIncidenceRate = runIncidenceRates,
    runCohortOverlap = runCohortOverlap,
    runCohortCharacterization = runCohortCharacterization,
    covariateSettings = covariateSettings
  )
}


#' @export
bundleDiagnosticsResults <- function(diagnosticOutputFolder, databaseId) {

  # Write metadata, log, and diagnostics results files to single ZIP file
  date <- format(Sys.time(), "%Y%m%dT%H%M%S")
  zipName <- file.path(diagnosticOutputFolder, paste0("Results_diagnostics_", databaseId, "_", date, ".zip"))
  files <- list.files(diagnosticOutputFolder, "^Results_.*.zip$|cohortDiagnosticsLog.txt", full.names = TRUE, recursive = TRUE)
  oldWd <- setwd(diagnosticOutputFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  return(zipName)
}

#' @export
exportResults <- function(exportFolder,
                          databaseId,
                          cohortIdsToExcludeFromResultsExport = NULL) {
  filesWithCohortIds <- c("covariate_value.csv", "cohort_count.csv")
  tempFolder <- NULL
  ParallelLogger::logInfo("Adding results to zip file")
  if (!is.null(cohortIdsToExcludeFromResultsExport)) {
    ParallelLogger::logInfo("Exclude cohort ids: ", paste(cohortIdsToExcludeFromResultsExport, collapse = ", "))
    # Copy files to temp location to remove the cohorts to remove
    tempFolder <- file.path(exportFolder, "temp")
    files <- list.files(exportFolder, pattern = ".*\\.csv$")
    if (!file.exists(tempFolder)) {
      dir.create(tempFolder)
    }
    file.copy(file.path(exportFolder, files), tempFolder)

    # Censor out the cohorts based on the IDs passed in
    for (i in 1:length(filesWithCohortIds)) {
      fileName <- file.path(tempFolder, filesWithCohortIds[i])
      fileContents <- readr::read_csv(fileName, col_types = readr::cols())
      fileContents <- fileContents[!(fileContents$cohort_id %in% cohortIdsToExcludeFromResultsExport), ]
      readr::write_csv(fileContents, fileName)
    }

    # Zip the results and copy to the main export folder
    zipName <- zipResults(tempFolder, databaseId)
    file.copy(zipName, exportFolder)
    unlink(tempFolder, recursive = TRUE)
    zipName <- file.path(exportFolder, basename(zipName))
  } else {
    zipName <- zipResults(exportFolder, databaseId)
  }
  ParallelLogger::logInfo("Results are ready for sharing at:", zipName)
}

zipResults <- function(exportFolder, databaseId) {
  date <- format(Sys.time(), "%Y%m%dT%H%M%S")
  zipName <- file.path(exportFolder, paste0("Results_", databaseId, "_", date, ".zip"))
  files <- list.files(exportFolder, ".*\\.csv$")
  oldWd <- setwd(exportFolder)
  on.exit(setwd(oldWd), add = TRUE)
  DatabaseConnector::createZipFile(zipFile = zipName, files = files)
  return(zipName)
}

# Per protocol, we will only characterize cohorts with
# >= 140 subjects to improve efficency
getMinimumSubjectCountForCharacterization <- function() {
  return(140)
}

getVocabularyInfo <- function(connection, cdmDatabaseSchema, tempEmulationSchema) {
  sql <- "SELECT vocabulary_version FROM @cdmDatabaseSchema.vocabulary WHERE vocabulary_id = 'None';"
  sql <- SqlRender::render(sql, cdmDatabaseSchema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"), tempEmulationSchema = tempEmulationSchema)
  vocabInfo <- DatabaseConnector::querySql(connection, sql)
  return(vocabInfo[[1]])
}

#' @export
getUserSelectableCohortGroups <- function() {
  cohortGroups <- getCohortGroups()
  return(unlist(cohortGroups[
    cohortGroups$userCanSelect == TRUE,
    c("cohortGroup")
  ], use.names = FALSE))
}

formatCovariates <- function(data) {
  # Drop covariates with mean = 0 after rounding to 4 digits:
  if (nrow(data) > 0) {
    data <- data[round(data$mean, 4) != 0, ]
    covariates <- unique(data.table::setDT(data[, c("covariateId", "covariateName", "analysisId")]))
    colnames(covariates)[[3]] <- "covariateAnalysisId"
  } else {
    covariates <- list("covariateId" = "", "covariateName" = "", "covariateAnalysisId" = "")
  }
  return(covariates)
}


loadCohortsFromPackage <- function(cohortIds) {
  packageName <- getThisPackageName()
  cohorts <- getCohortsToCreate()
  cohorts <- cohorts %>% dplyr::mutate(atlasId = NULL)
  if (!is.null(cohortIds)) {
    cohorts <- cohorts[cohorts$cohortId %in% cohortIds, ]
  }
  if ("atlasName" %in% colnames(cohorts)) {
    # Remove LungCancer cohort identifier (3.g. [LungCancer T1] )
    cohorts <- cohorts %>%
      dplyr::mutate(
        cohortName = trimws(gsub("(\\[.+?\\])", "", atlasName)),
        cohortFullName = atlasName
      ) %>%
      dplyr::select(-atlasName, -name)
  } else {
    cohorts <- cohorts %>% dplyr::rename(cohortName = name, cohortFullName = fullName)
  }

  getSql <- function(name) {
    pathToSql <- system.file("sql", "sql_server", paste0(name, ".sql"),
                             package = packageName, mustWork = TRUE
    )
    sql <- readChar(pathToSql, file.info(pathToSql)$size)
    return(sql)
  }
  cohorts$sql <- sapply(cohorts$cohortId, getSql)
  getJson <- function(name) {
    pathToJson <- system.file("cohorts", paste0(name, ".json"),
                              package = packageName,
                              mustWork = TRUE
    )
    json <- readChar(pathToJson, file.info(pathToJson)$size)
    return(json)
  }
  cohorts$json <- sapply(cohorts$cohortId, getJson)
  return(cohorts)
}

loadCohortsForExportFromPackage <- function(cohortIds) {
  packageName <- getThisPackageName()
  cohorts <- getCohortsToCreate()
  cohorts <- cohorts %>% dplyr::mutate(atlasId = NULL)
  if ("atlasName" %in% colnames(cohorts)) {
    # Remove LungCancer cohort identifier (3.g. [LungCancer O2])
    # Remove atlasName and name from object to prevent clashes when combining with stratXref
    cohorts <- cohorts %>%
      dplyr::mutate(
        cohortName = trimws(gsub("(\\[.+?\\])", "", atlasName)),
        cohortFullName = atlasName
      ) %>%
      dplyr::select(-atlasName, -name)
  } else {
    cohorts <- cohorts %>% dplyr::rename(
      cohortName = name,
      cohortFullName = fullName
    )
  }
}

loadCohortsForExportWithChecksumFromPackage <- function(cohortIds) {
  packageName <- getThisPackageName()
  cohorts <- loadCohortsForExportFromPackage(cohortIds)
}




enforceMinCellValue <- function(data, fieldName, minValues, silent = FALSE) {
  toCensor <- !is.na(data[, fieldName]) & data[, fieldName] < minValues & data[, fieldName] != 0
  if (!silent) {
    percent <- round(100 * sum(toCensor) / nrow(data), 1)
    ParallelLogger::logInfo(
      "   censoring ",
      sum(toCensor),
      " values (",
      percent,
      "%) from ",
      fieldName,
      " because value below minimum"
    )
  }
  if (length(minValues) == 1) {
    data[toCensor, fieldName] <- -minValues
  } else {
    data[toCensor, fieldName] <- -minValues[toCensor]
  }
  return(data)
}
