const readMetadataFiles = require('./processFiles');
const { testConnection } = require('./databaseHelpers');
const { processGeneralSessions, processVideoInteractionSessions, processAssessmentsSubmissions, processQuizSessions } = require('./processLogs');

const directoryPath = './ECObuild4x-2T2020';

// readMetadataFiles(directoryPath);
// processGeneralSessions();
// testConnection();
// processVideoInteractionSessions();
// processAssessmentsSubmissions();
processQuizSessions();