const directoryPath = './FP101x-3T2015';
const readMetadataFiles = require('./processFiles');
const { testConnection } = require('./databaseHelpers');
const { processGeneralSessions, processVideoInteractionSessions } = require('./processLogs');

// readMetadataFiles(directoryPath);
// processGeneralSessions();
// testConnection();
processVideoInteractionSessions();