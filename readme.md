# Non-Small Cellular Lung Cancer Characterization Study Package 
================================================================

- Analytics use case(s): **Characterization**
- Study type: **Clinical Application**
- Tags: **cancer**
- Protocol: **[NSCLCCharacterization](https://github.com/A1exanderAlexeyuk/NSCLCCharacterization/blob/main/NSCLCCharacterization.docx)**
- Study start date: **October 25, 2021**
- Study end date: **-**

The overarching aim of this study is to characterize patients with metastatic NSCLC with and without liver metastasis at the time of diagnosis with metastatic NSCLC. 
To describe demographics and clinical characteristics of patients with metastatic NSCLC and metastatic NSCLC patients with and without metastasis to liver who received systemic antineoplastic treatment 
To characterize detailed treatment patterns among patients with metastatic NSCLC, and metastatic NSCLC patients with and without metastasis to liver. Specifically, 
Distribution of treatment regimens, dose, cycle, and scheduling per systemic anti-neoplastic line of treatment LoT (up to two LoTs) 
Treatment flow across 1s and 2nd (treatment pathways)
Time to treatment discontinuation 
Time to next treatment 
To estimate overall survival (OS) of patients with metastatic NSCLC, metastatic NSCLC patients with and without metastasis to liver. 

The study is carried out by [Odysseus Data Services](https://odysseusinc.com) using the [OHDSI](https://www.ohdsi.org/) frameworks

### FAQ
#### *What do I need to do to run the package?*
OHDSI study repos are designed to have information in the README.md (where you are now) to provide you with instructions on how to navigate the repo. This package has two major components:
1. [CohortDiagnostics](http://www.github.com/ohdsi/cohortDiagnostics) - an OHDSI R package used to perform diagnostics and characterization around the fitness of use of the study phenotypes on your CDM. By running this package you will allow study leads to understand: cohort inclusion rule attrition, inspect source code lists for a phenotype, find orphan codes that should be in a particular concept set but are not, compute incidnece across calendar years, age and gender, break down index events into specific concepts that triggered then, compute overlap of two cohorts and compute basic characteristics of selected cohorts. This package will be requested of all sites. It is run on all available data not just your NSCL cancer populations. This allows us to understand how the study phenotypes perform in your database and identify any potential gaps in the phenotype definitions.
2. RunStudy - The part of the package that analyzes the therapy received by patients, survival and some other statistics

#### *I have a problem running the code or want to contribute a fix or enhancement.*
Please review the questions below, and if that doesn't answer it consider filing an issue in the Github tracker for the project: [issues](https://github.com/A1exanderAlexeyuk/NSCLCCharacterization/issues)

#### *I don't understand the organization of this Github Repo.*
The study repo has the following major pieces:
- `R` folder = the folder which will provide the R library the scripts it needs to execute this study
- `extras` folder = the folder where we store a copy of the instructions (called `CodeToRun.R`) below and other files that the study needs to do things like package maintenance or talk with the Shiny app. Aside from `CodeToRun.R`, you can largely ignore the rest of these files.
- `inst` folder = This is the "install" folder. It contains the most important parts of the study: the study cohort JSONs (analogous to what ATLAS shows you in the Export tab), the study settings, a sub-folder that contains information to the Shiny app, and the study cohort SQL scripts that [SqlRender](https://cran.r-project.org/web/packages/SqlRender/index.html) will use to translate these into your RDBMS.

Below you will find instructions for how to bring this package into your `R`/ `RStudio` environment. Note that if you are not able to connect to the internet in `R`/ `RStudio` to download pacakges, you will have to pull the [file](https://github.com/A1exanderAlexeyuk/NSCLCCharacterization.git). 

#### *I see you've got a reference `Renviron` but I've never used that? What do I do?*
You can install a package like `usethis` to quickly access your Renviron file.  `usethis` :package: has a useful helper function to modify `.Renviron`:

`usethis::edit_r_environ()` will open your user .Renviron which is in your home

`usethis::edit_r_environ("project")` will open the one in your project

Your Renviron file will pop-up through these commands. It will give you the opportunity to edit it as the directions instruct. If you need more help, consider reviewing this [R Community Resource](https://rviews.rstudio.com/2017/04/19/r-for-enterprise-understanding-r-s-startup/).

#### *What should I do if I get an error when I run the package?*
If you have any issues running the package, please report bugs / roadblocks via [GitHub Issues](https://github.com/A1exanderAlexeyuk/NSCLCCharacterization/issues) on this repo. Where possible, we ask you share error logs and snippets of warning messages that come up in your `R` console. You may also attach screenshots. Please include the RDMBS (aka your SQL dialect) you work on. If possible, run `traceback()` in your `R` and paste this into your error as well. The study leads will triage these errors with you.

#### *What should I do when I finish?*
If you finish running a study package and upload results to the SFTP, please post a message in the *Data sources and study execution* channel in Teams to notify you have dropped results in the folder. If your upload is unsucessful, please add the results to Teams directly.

## Package Requirements
- A database in [Common Data Model version 5](https://github.com/OHDSI/CommonDataModel) in one of these platforms: SQL Server, Oracle, PostgreSQL, IBM Netezza, Apache Impala, Amazon RedShift, or Microsoft APS.
- R version 4.0.0 or newer (3.5.0 or newer should also work, but then you will need a [backport](https://github.com/thehyve/CohortDiagnostics/tree/backport21_r3) of CohortDiagnostics)
- On Windows: [RTools](http://cran.r-project.org/bin/windows/Rtools/)
- [Java](http://java.com)
- Suggested: 25 GB of free disk space

See [this video](https://youtu.be/DjVgbBGK4jM) for instructions on how to set up the R environment on Windows.

## Note
The results of the analysis depend on the [package](https://github.com/A1exanderAlexeyuk/OncologyRegimenFinder) that allows you to receive the modes of antineoplastic therapy in patients

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
devtools::install_github("OHDSI/DatabaseConnector")
library(DatabaseConnector)
devtools::install_github("OHDSI/SqlRender")
library(SqlRender)
devtools::install_github("A1exanderAlexeyuk/NSCLCCharacterization")
library(NSCLCCharacterization)
devtools::install_github("A1exanderAlexeyuk/OncologyRegimenFinder")
library(OncologyRegimenFinder)

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
NSCLCCharacterization::runCohortDiagnostics(
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


source("extras/SankeyPlot.R")
# categorizedRegimensInfo - csv file what will be created after RunStudy
# cohortDefinitionId - 101 or 102 or 103
#output - sankey plot
createSankeyPlot(
  categorizedRegimensInfo,
  cohortDefinitionId
)


