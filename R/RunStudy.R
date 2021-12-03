#' @export
runStudy <- function(connectionDetails,
                     connection  = NULL,
                     cdmDatabaseSchema,
                     tempEmulationSchema = NULL,
                     oracleDatabaseSchema = NULL,
                     cohortDatabaseSchema,
                     writeDatabaseSchema,
                     cohortTable = "cohort",
                     cohortIdsToExcludeFromExecution = c(),
                     cohortIdsToExcludeFromResultsExport = NULL,
                     regimenIngredientsTable = regimenIngredientsTable,
                     createRegimenStats = TRUE,
                     createCategorizedRegimensTable = F,
                     regimenStatsTable = 'default_rst',
                     dropRegimenStatsTable = FALSE,
                     exportFolder,
                     databaseId,
                     databaseName = databaseId,
                     databaseDescription = "",
                     gapBetweenTreatment = 120) {
  start <- Sys.time()

  if (!file.exists(exportFolder)) {
    dir.create(exportFolder, recursive = TRUE)
  }

  ParallelLogger::addDefaultFileLogger(file.path(
    exportFolder,
    "NSCLCCharacterization.txt"
  ))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT"))

  # Write out the system information
  ParallelLogger::logInfo(.systemInfo())

  useSubset <- Sys.getenv("USE_SUBSET")
  if (!is.na(as.logical(useSubset)) && as.logical(useSubset)) {
    ParallelLogger::logWarn("Running in subset mode for testing")
  }

  if (!is.null(getOption("fftempdir")) && !file.exists(getOption("fftempdir"))) {
    warning("fftempdir '", getOption("fftempdir"), "' not found. Attempting to create folder")
    dir.create(getOption("fftempdir"), recursive = TRUE)
  }

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  cohortIdsConditionIndex <- getCohortsToCreate()$cohortId[4:6]
  targetIdsTreatmentIndex <- getCohortsToCreate()$cohortId[1:3]

  # Generate  regimen stats table -----------------------------------------------------------------
  if (createRegimenStats) {
    if (!is.null(regimenIngredientsTable)) {
      createRegimenStats(
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = cdmDatabaseSchema,
        writeDatabaseSchema = writeDatabaseSchema,
        cohortTable = cohortTable,
        regimenStatsTable = regimenStatsTable,
        regimenIngredientsTable = regimenIngredientsTable,
        gapBetweenTreatment = gapBetweenTreatment
      )
    } else {
      ParallelLogger::logWarn("Specify regimen ingredients table")
    }
  }

    # Generate categorized regimens  info -----------------------------------------------------------------
    ParallelLogger::logInfo("Generating regimen categories")
  if(createCategorizedRegimensTable){
    categorizedRegimens <- createCategorizedRegimensTable(
      connectionDetails = connectionDetails,
      cohortDatabaseSchema = cohortDatabaseSchema,
      regimenStatsTable = regimenStatsTable,
      targetIds = targetIdsTreatmentIndex
    )

    writeToCsv(categorizedRegimens, file.path(
      exportFolder,
      "categorizedRegimens_info.csv"
    ))
  } else {
    ParallelLogger::logWarn("Regimen categories table is not created")
  }

  # Generate survival info -----------------------------------------------------------------

  ParallelLogger::logInfo("Generating survival info")

  KMOutcomes <- getFeatures()
  KMOutcomesIds <- KMOutcomes$cohortId[1]
  SurvivalInfo <- generateSurvival(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    targetIds = targetIdsTreatmentIndex,
    outcomeId = KMOutcomesIds,
    databaseId = databaseId,
    packageName = getThisPackageName()
  )
  KMOutcomesIds <- KMOutcomes$cohortId[KMOutcomes$name %in% c("Death")]
  writeToCsv(SurvivalInfo, file.path(
    exportFolder,
    "Survuval_info.csv"
  ))

  # Generate treatment outcomes info -----------------------------------------------------
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo(" ---- Treatment statistics  ---- ")
  ParallelLogger::logInfo("**********************************************************")
  # time to treatment initiation
  outcomesTI <- KMOutcomes$cohortId[KMOutcomes$name %in% c("Treatment initiation")]
  timeToTI <- generateTimeToTreatmenInitiationStatistics(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    targetIds = cohortIdsConditionIndex,
    outcomeId = outcomesTI,
    cohortTable = cohortTable,
    databaseId = databaseId
  )
  writeToCsv(timeToTI, file.path(
    exportFolder,
    "timeToTI_info.csv"
  ))

  # time to next treatment
  timeToNT <- generateKaplanMeierDescriptionTNT(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIdsTreatmentIndex,
    databaseId = databaseId
  )

  writeToCsv(timeToNT, file.path(
    exportFolder,
    "timeToNT_info.csv"
  ))

  # treatment free interval and time to treatment discontinuation
  TFI <- generateKaplanMeierDescriptionTFI(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIdsTreatmentIndex,
    databaseId = databaseId
  )

  writeToCsv(TFI, file.path(
    exportFolder,
    "TFI_info.csv"
  ))

  TTD <- generateKaplanMeierDescriptionTTD(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIdsTreatmentIndex,
    databaseId = databaseId
  )

  writeToCsv(TTD, file.path(
    exportFolder,
    "TTD_info.csv"
  ))
  ParallelLogger::logInfo("Dropping RegimenStatsTable")
  if(dropRegimenStatsTable){
    DatabaseConnector::renderTranslateExecuteSql(connection = connection,
                                                 sql = "DROP TABLE IF EXISTS @writeDatabaseSchema.@regimenStatsTable",
                                                 writeDatabaseSchema = writeDatabaseSchema,
                                                 regimenStatsTable = regimenStatsTable
    )
  }
  # Generate metricsDistribution info -----------------------------------------------------
  ParallelLogger::logInfo("Generating metrics distribution")


  # prepare necessary tables
  targetIdsFormatted <- targetIdsTreatmentIndex
  pathToSql <- system.file("sql", "sql_server",
                           "distributions", "IQRComplementaryTables.sql",
                           package = getThisPackageName()
  )

  sql <- readChar(pathToSql, file.info(pathToSql)$size)

  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               sql = sql,
                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                               cohortDatabaseSchema = cohortDatabaseSchema,
                                               cohortTable = cohortTable,
                                               targetIds = targetIdsFormatted
  )

  metricsDistribution <- data.frame()
  DistribAnalyses <- c(
    "AgeAtIndex",
    "CharlsonAtIndex",
    "NeutrophilToLymphocyteRatioAtIndex",
    "PDLAtIndex",
    "PlateletToLymphocyteRatioAtIndex"
  )
  for (analysis in DistribAnalyses) {
    result <- getAtEventDistribution(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cdmDatabaseSchema = cdmDatabaseSchema,
      cohortTable = cohortTable,
      targetIds = targetIdsTreatmentIndex,
      databaseId = databaseId,
      packageName = getThisPackageName(),
      analysisName = analysis
    )
    metricsDistribution <- rbind(metricsDistribution, result)
  }



  writeToCsv(metricsDistribution, file.path(
    exportFolder,
    "metrics_distribution__info.csv"
  ))
  # drom temp tables
  pathToSql <- system.file("sql",
                           "sql_server",
                           "distributions",
                           "RemoveComplementaryTables.sql",
                           package = getThisPackageName()
  )
  sql <- readChar(pathToSql, file.info(pathToSql)$size)
  DatabaseConnector::renderTranslateExecuteSql(connection,
                                               sql = sql,
                                               cohortDatabaseSchema = cohortDatabaseSchema
  )

}



writeToCsv <- function(data,
                       fileName,
                       ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  readr::write_csv(data, fileName)
}
