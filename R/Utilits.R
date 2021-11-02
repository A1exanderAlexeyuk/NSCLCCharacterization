getThisPackageName <- function() {
  return("LungCancerCharacterization")
}

getFeatureTimeWindows <- function() {
  resourceFile <- file.path(getPathToResource(), "featureTimeWindows.csv")
  return(readCsv(resourceFile))
}

getPathToResource <- function(useSubset = Sys.getenv("USE_SUBSET")) {
  path <- "settings"
  useSubset <- as.logical(useSubset)
  if (is.na(useSubset)) {
    useSubset = FALSE
  }
  if (useSubset) {
    path <- file.path(path, "subset/")
  }
  return(path)
}

getCohortGroupsForDiagnostics <- function () {
  resourceFile <- file.path(getPathToResource(), "CohortGroupsDiagnostics.csv")
  return(readCsv(resourceFile))
}

getCohortsToCreate <- function(cohortGroups = getCohortGroups()) {
  packageName <- getThisPackageName()
  cohorts <- data.frame()
  for(i in 1:nrow(cohortGroups)) {
    c <- readr::read_csv(system.file(cohortGroups$fileName[i], 
                                     package = packageName, 
                                     mustWork = TRUE), 
                         col_types = readr::cols())
    c <- c[c('name', 'atlasName', 'atlasId', 'cohortId')]
    c$cohortType <- cohortGroups$cohortGroup[i]
    cohorts <- rbind(cohorts, c)
  }
  return(cohorts)  
}

getCohortGroups <- function () {
  resourceFile <- file.path(getPathToResource(), "CohortGroups.csv")
  return(readCsv(resourceFile))
}

getAllStudyCohorts <- function() {
  cohortsToCreate <- getCohortsToCreate()
  targetStrataXref <- getTargetStrataXref()
  colNames <- c("name", "cohortId")
  cohortsToCreate <- cohortsToCreate[, match(colNames, names(cohortsToCreate))]
  targetStrataXref <- targetStrataXref[, match(colNames, names(targetStrataXref))]
  allCohorts <- rbind(cohortsToCreate, targetStrataXref)
  return(allCohorts)
}

getFeatures <- function() {
  resourceFile <- file.path(getPathToResource(), "CohortsToCreateOutcome.csv")
  return(readCsv(resourceFile))
}