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
#'
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
                                 cohortStagingTable = "cohort_stg",
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
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)

  if (createCohorts) {
    # Instantiate cohorts -----------------------------------------------------------------------
    cohorts <- getCohortsToCreate()

    # Remove any cohorts that are to be excluded
    cohorts <- cohorts[!(cohorts$cohortId %in% cohortIdsToExcludeFromExecution), ]
    targetCohortIds <- cohorts[cohorts$cohortType %in% cohortGroups, "cohortId"][[1]] #
    outcomeCohortIds <- cohorts[cohorts$cohortType == "outcome", "cohortId"][[1]]
    # Start with the target cohorts
    ParallelLogger::logInfo("**********************************************************")
    ParallelLogger::logInfo("  ---- Creating target cohorts ---- ")
    ParallelLogger::logInfo("**********************************************************")
    instantiateCohortSet(
      connectionDetails = connectionDetails,
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = NULL,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortStagingTable,
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
      cohortTable = cohortStagingTable,
      cohortIds = outcomeCohortIds,
      createCohortTable = FALSE
    )


    # Copy and censor cohorts to the final table
    ParallelLogger::logInfo("**********************************************************")
    ParallelLogger::logInfo(" ---- Copy cohorts to main table ---- ")
    ParallelLogger::logInfo("**********************************************************")
    copyAndCensorCohorts(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortStagingTable = cohortStagingTable,
      cohortTable = cohortTable,
      targetIds = targetCohortIds,
      tempEmulationSchema = tempEmulationSchema
    )
  }
  cohortIds <- getCohortsToCreate()$cohortId[1:3]
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
    runInclusionStatistics = FALSE,
    runIncludedSourceConcepts = runIncludedSourceConcepts,
    runOrphanConcepts = runOrphanConcepts,
    runTimeDistributions = runTimeDistributions,
    runBreakdownIndexEvents = runBreakdownIndexEvents,
    runIncidenceRate = runIncidenceRates,
    runCohortOverlap = runCohortOverlap,
    runCohortCharacterization = runCohortCharacterization
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
