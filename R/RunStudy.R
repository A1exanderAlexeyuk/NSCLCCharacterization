#' @export
runStudy <- function(connectionDetails = NULL,
                     connection = NULL,
                     cdmDatabaseSchema,
                     tempEmulationSchema = NULL,
                     oracleDatabaseSchema = NULL,
                     cohortDatabaseSchema,
                     cohortStagingTable = "cohort_stg",
                     cohortTable = "cohort",
                     featureSummaryTable = "cohort_smry",
                     cohortIdsToExcludeFromExecution = c(),
                     cohortIdsToExcludeFromResultsExport = NULL,
                     cohortGroups = getUserSelectableCohortGroups(),
                     regimenIngredientsTable = regimenIngredientsTable,
                     createRegimenStats = TRUE,
                     exportFolder,
                     databaseId,
                     databaseName = databaseId,
                     databaseDescription = "",
                     minCellCount = 5) {
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
  #targetIdsTreatmentIndex <- list(101, 102, 103)
  # Instantiate cohorts -----------------------------------------------------------------------
  cohorts <- getCohortsToCreate()
  # Remove any cohorts that are to be excluded
  cohorts <- cohorts[!(cohorts$cohortId %in% cohortIdsToExcludeFromExecution), ]
  targetCohortIds <- cohorts[cohorts$cohortType %in% cohortGroups, "cohortId"][[1]] # ! should use first 3!
  outcomeCohortIds <- cohorts[cohorts$cohortType == "outcome", "cohortId"][[1]]

  # Start with the target cohorts
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo("  ---- Creating target cohorts ---- ")
  ParallelLogger::logInfo("**********************************************************")
  instantiateCohortSet(
    connectionDetails = connectionDetails,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    oracleTempSchema = oracleTempSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortStagingTable,
    cohortIds = targetCohortIds,
    minCellCount = minCellCount,
    createCohortTable = TRUE,
    generateInclusionStats = FALSE,
    inclusionStatisticsFolder = exportFolder
  )

  # Create the outcome cohorts
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo(" ---- Creating outcome cohorts ---- ")
  ParallelLogger::logInfo("**********************************************************")
  instantiateCohortSet(
    connectionDetails = connectionDetails,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    oracleTempSchema = oracleTempSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortStagingTable,
    cohortIds = outcomeCohortIds,
    minCellCount = minCellCount,
    createCohortTable = FALSE,
    generateInclusionStats = FALSE,
    inclusionStatisticsFolder = exportFolder
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
    minCellCount = minCellCount,
    targetIds = targetCohortIds,
    oracleTempSchema = oracleTempSchema
  )



  # Counting staging cohorts ---------------------------------------------------------------
  ParallelLogger::logInfo("Counting staging cohorts")
  counts <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortStagingTable
  )
  if (nrow(counts) > 0) {
    counts$databaseId <- databaseId
    counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
    counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
  }
  allStudyCohorts <- getAllStudyCohorts()
  counts <- dplyr::left_join(x = allStudyCohorts, y = counts, by = "cohortId")
  writeToCsv(counts, file.path(exportFolder, "cohort_staging_count.csv"),
    cohortId = counts$cohortId
  )




  # Generate  regimen stats table -----------------------------------------------------------------
if(createRegimenStats){
  if(!is.null(regimenIngredientsTable)){
  createRegimenStatsTable <- createcreateRegimenStats(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortStagingTable,
    regimenStatsTable = regimenStatsTable,
    regimenIngredientsTable = regimenIngredientsTable,
    gapBetweenTreatment = 120
  )
  }else{
    ParallelLogger::logWarn("Specify regimen ingredients table")
  }

  # Generate categorized regimens  info -----------------------------------------------------------------



  ParallelLogger::logInfo("Generating regimen categories")
  categorizedRegimens <- createCategorizedRegimensTable(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortStagingTable,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIdsTreatmentIndex
  )

  writeToCsv(categorizedRegimens, file.path(
    exportFolder,
    "categorizedRegimens_info.csv"
  ))
}

  # Generate survival info -----------------------------------------------------------------

  ParallelLogger::logInfo("Generating survival info")


  KMOutcomes <- getFeatures()
  KMOutcomesIds <- KMOutcomes$cohortId[1]
  SurvivalInfo <- generateSurvival(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortStagingTable,
    targetIds = targetIdsTreatmentIndex,
    outcomeIds = KMOutcomesIds,
    databaseId = databaseId,
    packageName = getThisPackageName()
  )
  KMOutcomesIds <- KMOutcomes$cohortId[KMOutcomes$name %in% c("Death")]
  writeToCsv(SurvivalInfo, file.path(
    exportFolder,
    "Survuval_info.csv"
  )
  )

  # Generate treatment outcomes info -----------------------------------------------------
  ParallelLogger::logInfo("**********************************************************")
  ParallelLogger::logInfo(" ---- Treatment statistics  ---- ")
  ParallelLogger::logInfo("**********************************************************")
  # time to treatment initiation
  outcomesTI <- KMOutcomes$cohortId[KMOutcomes$name %in% c("Treatment initiation")]
  timeToTI <- generateTimeToTreatmenInitiationStatistics(
    connection = connection,
    cohortDatabaseSchema,
    cohortTable = cohortStagingTable,
    targetIds = c(104, 105, 106), # condition index cohorts
    outcomeId = outcomesTI,
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
    cohortTable = cohortStagingTable,
    targetIds = targetIdsTreatmentIndex,
    databaseId = databaseId
  )

  writeToCsv(timeToNT, file.path(
    exportFolder,
    "timeToNT.csv"
   )
  )

  # treatment free interval and time to treatment discontinuation
  TFI_TTD <- generateKaplanMeierDescriptionTFI_TTD(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    regimenStatsTable = regimenStatsTable,
    targetIds = targetIdsTreatmentIndex,
    databaseId = databaseId
  )

  writeToCsv(TFI_TTD, file.path(
    exportFolder,
    "TFI_and_TTD.csv"
  ))

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
    cohortTable = cohortStagingTable,
    targetIds = targetIdsFormatted
  )


  DistribAnalyses <- c(
    "AgeAtIndex",
    "CharlsonAtIndex",
    "NeutrophilToLymphocyteRatioAtIndex",
    "PDLAtIndex",
    "PlateletToLymphocyteRatioAtIndex"
  )

  metricsDistribution <- data.frame()

  result <- getAtEventDistribution(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortTable = cohortStagingTable,
    targetIds = targetIds,
    databaseId = databaseId,
    packageName = getThisPackageName(),
    analysisName <- analysis
  )

  metricsDistribution <- rbind(metricsDistribution, result)




  writeToCsv(metricsDistribution, file.path(
    exportFolder,
    "metrics_distribution.csv"
    )
  )
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
    cohort_database_schema = cohortDatabaseSchema
  )


  # Counting cohorts -----------------------------------------------------------------------
  ParallelLogger::logInfo("Counting cohorts")
  counts <- getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable
  )
  if (nrow(counts) > 0) {
    counts$databaseId <- databaseId
    counts <- enforceMinCellValue(counts, "cohortEntries", minCellCount)
    counts <- enforceMinCellValue(counts, "cohortSubjects", minCellCount)
  }
  writeToCsv(counts, file.path(exportFolder, "cohort_count.csv"), cohortId = counts$cohortId)

  # Read in the cohort counts
  counts <- readr::read_csv(file.path(exportFolder, "cohort_count.csv"), col_types = readr::cols())
  colnames(counts) <- SqlRender::snakeCaseToCamelCase(colnames(counts))

  # Export the cohorts from the study
  cohortsForExport <- loadCohortsForExportFromPackage(cohortIds = counts$cohortId)
  writeToCsv(cohortsForExport, file.path(exportFolder, "cohort.csv"))

  # Cohort characterization ---------------------------------------------------------------
  runCohortCharacterization <- function() {
    data <- getCohortCharacteristics(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      oracleTempSchema = oracleTempSchema,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTable,
      cohortId = targetIdsTreatmentIndex,
      covariateSettings = FeatureExtraction::createDefaultCovariateSettings(
        includedCovariateConceptIds = getCovariatesToInclude(),
        addDescendantsToInclude = TRUE,
        excludedCovariateConceptIds = 4162276, # melanoma
        addDescendantsToExclude = TRUE
      )
    )
    if (nrow(data) > 0) {
      data$cohortId <- cohortId
    }

    return(data)
  }

  data <- runCohortCharacterization()
  covariates <- formatCovariates(data)
  writeToCsv(covariates, file.path(exportFolder, "covariate.csv"))
  data <- formatCovariateValues(data, counts, minCellCount, databaseId)
  writeToCsv(data, file.path(exportFolder, "covariate_value.csv"))


  # Format results -----------------------------------------------------------------------------------
  ParallelLogger::logInfo("********************************************************************************************")
  ParallelLogger::logInfo("Formatting Results")
  ParallelLogger::logInfo("********************************************************************************************")
  # Ensure that the covariate_value.csv is free of any duplicative values. This can happen after more than
  # one run of the package.
  cv <- data.table::fread_csv(file.path(exportFolder, "covariate_value.csv"),
    col_types = readr::cols()
  )
  cv <- unique(cv)
  writeToCsv(cv, file.path(exportFolder, "covariate_value.csv"))

  # Export to zip file -------------------------------------------------------------------------------
  exportResults(exportFolder, databaseId, cohortIdsToExcludeFromResultsExport)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste(
    "Running study took",
    signif(delta, 3),
    attr(delta, "units")
  ))
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

getVocabularyInfo <- function(connection, cdmDatabaseSchema, oracleTempSchema) {
  sql <- "SELECT vocabulary_version FROM @cdmDatabaseSchema.vocabulary WHERE vocabulary_id = 'None';"
  sql <- SqlRender::render(sql, cdmDatabaseSchema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"), oracleTempSchema = oracleTempSchema)
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

formatCovariateValues <- function(data, counts, minCellCount, databaseId) {
  data$covariateName <- NULL
  data$analysisId <- NULL
  if (nrow(data) > 0) {
    data$databaseId <- databaseId
    data <- merge(data, counts[, c("cohortId", "cohortEntries")])
    data <- enforceMinCellValue(data, "mean", minCellCount / data$cohortEntries)
    data$sd[data$mean < 0] <- NA
    data$cohortEntries <- NULL
    data$mean <- round(data$mean, 3)
    data$sd <- round(data$sd, 3)
  }
  return(data)
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


writeToCsv <- function(data,
                       fileName,
                       ...) {
  colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
  readr::write_csv(data, fileName)
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

getCohortCounts <- function(connectionDetails = NULL,
                            connection = NULL,
                            cohortDatabaseSchema,
                            cohortTable = "cohort",
                            cohortIds = c()) {
  start <- Sys.time()

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CohortCounts.sql",
    packageName = getThisPackageName(),
    dbms = connection@dbms,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds
  )
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  delta <- Sys.time() - start
  ParallelLogger::logInfo(paste(
    "Counting cohorts took",
    signif(delta, 3),
    attr(delta, "units")
  ))
  return(counts)
}

