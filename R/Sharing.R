rootFTPFolder <- function() {
  return("/Task5/")
}

#' @export
uploadDiagnosticsResults <- function(outputFolder, privateKeyFileName, userName) {
  uploadResults(file.path(outputFolder, "diagnostics"), privateKeyFileName, userName, remoteFolder = paste0(rootFTPFolder(), "CohortDiagnostics"))
}

#' @export
uploadStudyResults <- function(outputFolder, privateKeyFileName, userName) {
  uploadResults(outputFolder, privateKeyFileName, userName, remoteFolder = paste0(rootFTPFolder(), "StudyResults"))
}

#' Upload results to OHDSI server
#'
#' @details
#' This function uploads the 'Results_<databaseId>.zip' to the OHDSI SFTP server. Before sending, you can inspect the zip file,
#' wich contains (zipped) CSV files. You can send the zip file from a different computer than the one on which is was created.
#'
#' @param privateKeyFileName   A character string denoting the path to the RSA private key provided by the study coordinator.
#' @param userName             A character string containing the user name provided by the study coordinator.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#'
uploadResults <- function(outputFolder, privateKeyFileName, userName, remoteFolder) {
  fileName <- list.files(outputFolder, "^Results_.*.zip$", full.names = TRUE)
  if (length(fileName) == 0) {
    stop("Could not find results file in folder. Did you run (and complete) execute?")
  }
  if (length(fileName) > 1) {
    stop("Multiple results files found. Don't know which one to upload")
  }
  OhdsiSharing::sftpUploadFile(privateKeyFileName = privateKeyFileName,
                               userName = userName,
                               remoteFolder = remoteFolder,
                               fileName = fileName)
  ParallelLogger::logInfo("Finished uploading")
}
