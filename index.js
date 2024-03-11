const { readMetadataFiles } = require("./processFiles");
const { testConnection } = require("./databaseHelpers");
const {
  processGeneralSessions,
  processVideoInteractionSessions,
  processAssessmentsSubmissions,
  processQuizSessions,
} = require("./processLogs");

const directoryPath =
  "W:/staff-umbrella/gdicsmoocs/Working copy/EX101x_2T2018_run6 - Copy";

readMetadataFiles(directoryPath);
processGeneralSessions();
testConnection();
processVideoInteractionSessions();
processAssessmentsSubmissions();
processQuizSessions();
