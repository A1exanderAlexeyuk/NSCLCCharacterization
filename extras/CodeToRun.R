# *******************************************************
# -----------------INSTRUCTIONS -------------------------
# *******************************************************
#
#-----------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------
# This CodeToRun.R is provided as an example of how to run this study package.
# Below you will find 2 sections: the 1st is for installing the dependencies
# required to run the study and the 2nd for running the package.
#
# The code below makes use of R environment variables (denoted by "Sys.getenv(<setting>)") to
# allow for protection of sensitive information. If you'd like to use R environment variables stored
# in an external file, this can be done by creating an .Renviron file in the root of the folder
# where you have cloned this code. For more information on setting environment variables please refer to:
# https://stat.ethz.ch/R-manual/R-devel/library/base/html/readRenviron.html
#
#
# Below is an example .Renviron file's contents: (please remove)
# the "#" below as these too are interprted as comments in the .Renviron file:
#
#    DBMS = "postgresql"
#    DB_SERVER = "database.server.com"
#    DB_PORT = 5432
#    DB_USER = "database_user_name_goes_here"
#    DB_PASSWORD = "your_secret_password"
#    FFTEMP_DIR = "E:/fftemp"
#    CDM_SCHEMA = "your_cdm_schema"
#    COHORT_SCHEMA = "public"  # or other schema to write intermediate results to
#    PATH_TO_DRIVER = "/path/to/jdbc_driver"
#
# The following describes the settings
#    DBMS, DB_SERVER, DB_PORT, DB_USER, DB_PASSWORD := These are the details used to connect
#    to your database server. For more information on how these are set, please refer to:
#    http://ohdsi.github.io/DatabaseConnector/
#
#    FFTEMP_DIR = A directory where temporary files used by the FF package are stored while running.
#
#
# Once you have established an .Renviron file, you must restart your R session for R to pick up these new
# variables.
#
# In section 2 below, you will also need to update the code to use your site specific values. Please scroll
# down for specific instructions.
#-----------------------------------------------------------------------------------------------
#
#
# *******************************************************
# SECTION 1: Install the package and its dependencies (not needed if already done) -------------
# *******************************************************
#
#
# First, it probably is best to make sure you are up-to-date on all existing packages.
# Important: This code is best run in R, not RStudio, as RStudio may have some libraries
# (like 'rlang') in use.
update.packages(ask = "graphics")

# When asked to update packages, select '1' ('update all') (could be multiple times)
# When asked whether to install from source, select 'No' (could be multiple times)
install.packages("devtools")
devtools::install_github("A1exanderAlexeyuk/NSCLCCharacterization")

# If this runs correctly, it should have installed the package and its dependencies, and you can proceed to section 2.

# *******************************************************
# SECTION 2: Running the package ---------------------------------------------------------------
# *******************************************************
library(NSCLCCharacterization)

# Optional: specify where the temporary files (used by the ff package) will be created:
fftempdir <- if (Sys.getenv("FFTEMP_DIR") == "") "~/fftemp" else Sys.getenv("FFTEMP_DIR")
options(fftempdir = fftempdir)

# Details for connecting to the server:
dbms = Sys.getenv("DBMS")
user <- if (Sys.getenv("DB_USER") == "") NULL else Sys.getenv("DB_USER")
password <- if (Sys.getenv("DB_PASSWORD") == "") NULL else Sys.getenv("DB_PASSWORD")
# password <- Sys.getenv("DB_PASSWORD")
server = Sys.getenv("DB_SERVER")
port = Sys.getenv("DB_PORT")
extraSettings <- if (Sys.getenv("DB_EXTRA_SETTINGS") == "") NULL else Sys.getenv("DB_EXTRA_SETTINGS")
pathToDriver <- if (Sys.getenv("PATH_TO_DRIVER") == "") NULL else Sys.getenv("PATH_TO_DRIVER")
connectionString <- if (Sys.getenv("CONNECTION_STRING") == "") NULL else Sys.getenv("CONNECTION_STRING")

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                user = user,
                                                                password = password,
                                                                server = server,
                                                                port = port,
                                                                connectionString = connectionString,
                                                                pathToDriver = pathToDriver)



# For Oracle: define a schema that can be used to emulate temp tables:
tempEmulationSchema <- NULL

# Details specific to the database:
databaseId <- "SP"
databaseName <- "Synpuf"
databaseDescription <- "Testing"
outputFolderPath <- getwd() # if needed, set up a different path for results

# Details for connecting to the CDM and storing the results
regimenIngredientsTable <- "name_of_your_regimen_stats_table" #sql db an output on OncologyRegimenFinder
gapBetweenTreatment <- 120 # specify gap between lines what will be used as a difinition on TTD
outputFolder <- normalizePath(file.path(outputFolderPath, databaseId))
cdmDatabaseSchema <- Sys.getenv("CDM_SCHEMA")
cohortDatabaseSchema <- Sys.getenv("COHORT_SCHEMA")
cohortTable <- paste0("NSCLC_", databaseId)
cohortStagingTable <- paste0(cohortTable, "_stg")
featureSummaryTable <- paste0(cohortTable, "_smry")
minCellCount <- 5
cohortIdsToExcludeFromExecution <- c()
cohortIdsToExcludeFromResultsExport <- NULL

# For uploading the results. You should have received the key file from the study coordinator, input the correct path here:
keyFileName <- "your-home-folder-here/.ssh/study-data-site-NSCLC"
userName <- "study-data-site-NSCLC"

# Run cohort diagnostics -----------------------------------
runCohortDiagnostics(connectionDetails = connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     cohortDatabaseSchema = cohortDatabaseSchema,
                     cohortStagingTable = cohortStagingTable,
                     tempEmulationSchema = tempEmulationSchema,
                     cohortIdsToExcludeFromExecution = cohortIdsToExcludeFromExecution,
                     exportFolder = outputFolder,
                     databaseId = databaseId,
                     databaseName = databaseName,
                     databaseDescription = databaseDescription,
                     minCellCount = minCellCount)


# CohortDiagnostics::launchDiagnosticsExplorer(file.path(outputFolder, "diagnostics", "target"))

# When finished with reviewing the diagnostics, use the next command
# to upload the diagnostic results
uploadDiagnosticsResults(outputFolder, keyFileName, userName)


# Use this to run the study. The results will be stored in a zip file called
# 'Results_<databaseId>.zip in the outputFolder.
runStudy(connectionDetails = connectionDetails,
         cdmDatabaseSchema = cdmDatabaseSchema,
         cohortDatabaseSchema = cohortDatabaseSchema,
         cohortStagingTable = cohortStagingTable,
         cohortTable = cohortTable,
         featureSummaryTable = featureSummaryTable,
         oracleTempSchema = cohortDatabaseSchema,
         regimenIngredientsTable = NULL,
         createRegimenStats = FALSE,
         gapBetweenTreatment = gapBetweenTreatment,
         exportFolder = outputFolder,
         databaseId = databaseId,
         databaseName = databaseName,
         databaseDescription = databaseDescription,
         cohortIdsToExcludeFromExecution = cohortIdsToExcludeFromExecution,
         cohortIdsToExcludeFromResultsExport = cohortIdsToExcludeFromResultsExport,
         minCellCount = minCellCount)


# Use the next set of commands to compress results
# and view the output.
preMergeResultsFiles(outputFolder)

# When finished with reviewing the results, use the next command
# upload study results to OHDSI SFTP server:
uploadStudyResults(outputFolder, keyFileName, userName)

