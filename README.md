# Non-Small Cellular Lung Cancer Characterization Study Package 
================================================================

- Analytics use case(s): **Characterization**
- Study type: **Clinical Application**
- Tags: **cancer**
- Study start date: **October 25, 2021**
- Study end date: **-**

Introduction
============



Overview
========



# *******************************************************
# -----------------INSTRUCTIONS -------------------------
# *******************************************************

## How to Run the Study
1. In `R`, you will build an `.Renviron` file. An `.Renviron` is an R environment file that sets variables you will be using in your code. It is encouraged to store these inside your environment so that you can protect sensitive information. Below are brief instructions on how to do this:

````
# The code below makes use of R environment variables (denoted by "Sys.getenv(<setting>)") to 
# allow for protection of sensitive information. If you'd like to use R environment variables stored
# in an external file, this can be done by creating an .Renviron file in the root of the folder
# where you have cloned this code. For more information on setting environment variables please refer to: 
# https://stat.ethz.ch/R-manual/R-devel/library/base/html/readRenviron.html
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
#    USE_SUBSET = FALSE
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
#    USE_SUBSET = TRUE/FALSE. When set to TRUE, this will allow for runnning this package with a 
#    subset of the cohorts/features. This is used for testing. PLEASE NOTE: This is only enabled
#    by setting this environment variable.
#
# Once you have established an .Renviron file, you must restart your R session for R to pick up these new
# variables. 
````

*Note: If you are using the `DatabaseConnector` package for the first time, then you may also need to download the JDBC drivers to your database. See the [package documentation](https://ohdsi.github.io/DatabaseConnector/reference/jdbcDrivers.html), you can do this with a command like `DatabaseConnector::downloadJdbcDrivers(dbms="redshift", pathToDriver="/my-home-folder/jdbcdrivers")`.*

*Note: if you run into 403 errors from Github URLs when installing the package, you may have exceeded your Github API rate limit. If you have a Github account, then you can create a personal access token (PAT) using the link https://github.com/settings/tokens/new?scopes=repo,gist&description=R:GITHUB_PAT, and add that to your local environment, for example using `credentials::set_github_pat()` (install the package with `install.packages("credentials")` if you don't have it). The counter should also reset after an hour, so alternatively you can wait for that to happen.*

3. Great work! Now you have set-up your environment and installed the library that will run the package. You can use the following `R` script to load in your library and configure your environment connection details:

```
# *******************************************************
# SECTION 2: Running the package ---------------------------------------------------------------
# *******************************************************
source('SankeyPlot.R')
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

outputFolder <- normalizePath(file.path(outputFolderPath, databaseId))
cdmDatabaseSchema <- Sys.getenv("CDM_SCHEMA")
cohortDatabaseSchema <- Sys.getenv("COHORT_SCHEMA")
cohortTable <- paste0("NSCLC_", databaseId)
cohortStagingTable <- paste0(cohortTable, "_stg")
featureSummaryTable <- paste0(cohortTable, "_smry")
databaseName <- 'db_name'
cohortIdsToExcludeFromExecution <- c()
cohortIdsToExcludeFromResultsExport <- NULL

# For uploading the results. You should have received the key file from the study coordinator, input the correct path here:
keyFileName <- "your-home-folder-here/.ssh/study-data-site-NSCLC"
userName <- "study-data-site-NSCLC"

# Run cohort diagnostics -----------------------------------
NSCLCCharacterization::runCohortDiagnostics <- function(
  connection,
  connectionDetails,
  cdmDatabaseSchema,
  cohortDatabaseSchema,
  createCohorts = TRUE,
  cohortTable,
  tempEmulationSchema,
  outputFolder,
  databaseId,
  databaseName,
  databaseDescription = "Unknown",
  cohortStagingTable
)


# To view the results:
# Optional: if there are results zip files from multiple sites in a folder, this merges them, which will speed up starting the viewer:
CohortDiagnostics::preMergeDiagnosticsFiles(file.path(outputFolder, "diagnosticsExport"))

# Use this to view the results. Multiple zip files can be in the same folder. If the files were pre-merged, this is automatically detected:
CohortDiagnostics::launchDiagnosticsExplorer(file.path(outputFolder, "diagnosticsExport"))


# To explore a specific cohort in the local database, viewing patient profiles:
CohortDiagnostics::launchCohortExplorer(connectionDetails,
                                        cdmDatabaseSchema,
                                        cohortDatabaseSchema,
                                        cohortTable,
                                        cohortId)

# When finished with reviewing the diagnostics, use the next command
# to upload the diagnostic results
uploadDiagnosticsResults(outputFolder, keyFileName, userName)

devtools::install_github("A1exanderAlexeyuk/OncologyRegimenFinder")
library(OncologyRegimenFinder)
writeDatabaseSchema <- "your_schema_to_write" # should be the same as cohortDatabaseSchema
cdmDatabaseSchema <- "cdm_schema"
vocabularyTable <- "vocabulary_table"
cohortTable <- "cohort_table"
regimenTable <- "regimen_table"
regimenIngredientsTable <- "name_of_your_regimen_stats_table" #sql db an output on OncologyRegimenFinder
gapBetweenTreatment <- 120 # specify gap between lines what will be used as a difinition on TTD
dateLagInput <- 30
OncologyRegimenFinder::createRegimens(connectionDetails,
                                      cdmDatabaseSchema,
                                      writeDatabaseSchema,
                                      cohortTable,
                                      rawEventTable,
                                      regimenTable,
                                      regimenIngredientTable,
                                      vocabularyTable,
                                      cancerConceptId = 4115276,
                                      dateLagInput = 30,
                                      generateVocabTable = FALSE,
                                      generateRawEvents = FALSE
)


# Use this to run the study. The results will be stored in a zip file called
# 'Results_<databaseId>.zip in the outputFolder.
runStudy(connectionDetails,
         connection,
         cdmDatabaseSchema,
         writeDatabaseSchema,
         tempEmulationSchema,
         cohortDatabaseSchema,
         cohortStagingTable,
         cohortTable,
         featureSummaryTable,
         regimenIngredientsTable,
         createRegimenStats = TRUE,
         gapBetweenTreatment,
         exportFolder = outputFolder,
         databaseId,
         databaseName,
         dropRegimenStatsTable = FALSE, # optional - drop created table
         databaseDescription = "")


# Use the next set of commands to compress results
# and view the output.
preMergeResultsFiles(outputFolder)

# When finished with reviewing the results, use the next command
# upload study results to OHDSI SFTP server:
uploadStudyResults(outputFolder, keyFileName, userName)
