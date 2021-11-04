#' @export
#' !!!!! leave only 1 table ( regimenTable)
OncoRegimenFinderA1::createRegimens(connectionDetails = connectionDetails,
                                    cdmDatabaseSchema = cdmDatabaseSchema,
                                    writeDatabaseSchema=writeDatabaseSchema,
                                    cohortTable = cohortTable,
                                    regimenTable = regimenTable,
                                    rawEventTable = rawEventTable,
                                    regimenIngredientTable = regimenIngredientTable,
                                    vocabularyTable = vocabularyTable,
                                    drugClassificationIdInput = 21601387,
                                    cancerConceptId = 4115276,
                                    dateLagInput = dateLagInput,
                                    regimenRepeats = 5,
                                    generateVocabTable = FALSE,
                                    sampleSize = 999999999999,
                                    generateRawEvents = FALSE)
