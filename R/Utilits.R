getThisPackageName <- function() {
  return("NSCLCCharacterization")
}


readCsv <- function(resourceFile) {
  packageName <- getThisPackageName()
  pathToCsv <- system.file(resourceFile, package = packageName, mustWork = TRUE)
  fileContents <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(fileContents)
}

getCohortGroupNamesForDiagnostics <- function() {
  return(getCohortGroupsForDiagnostics()$cohortGroup)
}

getPathToTreatmentStats <- function() {
  return("inst/sql/sql_server/TreatmentAnalysis")
}


getPathToQuantiles <- function() {
  return("inst/sql/sql_server/distributions")
}



getPathToResource <- function(useSubset = Sys.getenv("USE_SUBSET")) {
  path <- "settings"
  useSubset <- as.logical(useSubset)
  if (is.na(useSubset)) {
    useSubset <- FALSE
  }
  if (useSubset) {
    path <- file.path(path, "subset/")
  }
  return(path)
}

getCohortGroupsForDiagnostics <- function() {
  resourceFile <- file.path(getPathToResource(), "CohortGroupsDiagnostics.csv")
  return(readCsv(resourceFile))
}

getCohortsToCreate <- function(cohortGroups = getCohortGroups()) {
  packageName <- getThisPackageName()
  cohorts <- data.frame()
  for (i in 1:nrow(cohortGroups)) {
    c <- readr::read_csv(system.file(cohortGroups$fileName[i],
      package = packageName,
      mustWork = TRUE
    ),
    col_types = readr::cols()
    )
    c <- c[c("name", "atlasName", "atlasId", "cohortId")]
    c$cohortType <- cohortGroups$cohortGroup[i]
    cohorts <- rbind(cohorts, c)
  }
  return(cohorts)
}

getCohortGroups <- function() {
  resourceFile <- file.path(getPathToResource(), "CohortGroups.csv")
  return(readCsv(resourceFile))
}

getAllStudyCohorts <- function() {
  cohortsToCreate <- getCohortsToCreate()
  colNames <- c("name", "cohortId")
  allCohorts <- cohortsToCreate[, match(colNames, names(cohortsToCreate))]
  return(allCohorts)
}

getFeatures <- function() {
  resourceFile <- file.path(getPathToResource(), "CohortsToCreateOutcome.csv")
  return(readCsv(resourceFile))
}

getCovariatesToInclude <- function() {
  c(
    4182210,
    4063381,
    4064161,
    4212540,
    201820,
    443767,
    442793,
    4030518,
    4245975,
    4029488,
    192680,
    24966,
    257628,
    134442,
    80800,
    80809,
    256197,
    255348,
    381591,
    434056,
    40568109,
    4028741,
    4289933,
    438112
  )
}
