const { readMetadataFiles } = require("./processFiles");
const {
  testConnection,
  clearDatabase,
  clearSessionsCollections,
} = require("./databaseHelpers");
const {
  processGeneralSessions,
  processVideoInteractionSessions,
  processAssessmentsSubmissions,
  processQuizSessions,
  processORASessions,
} = require("./processLogs");

const fs = require("fs");
const path = require("path");

/**
 * Function that identifies the log files for each course run
 * @param {string} directoryPath The path to the directory containing the course run directories
 * @returns {Object} An object with course run directory names as keys and an array of log file paths as values
 */
async function identifyLogFilesPerCourseRun(directoryPath) {
  const fileEnding = ".log.gz";
  let logFilesPerCourseRun = {};
  let directories = await fs.promises.readdir(directoryPath);
  for (let dirName of directories) {
    if (courses.some((course) => dirName.includes(course))) {
      let coursePath = path.join(path.resolve(directoryPath), dirName);
      let files = [];
      let fileNames = await fs.promises.readdir(coursePath);
      for (let fileName of fileNames) {
        if (fileName.endsWith(fileEnding)) {
          files.push(path.join(coursePath, fileName));
        }
      }

      if (files.length === 0) {
        console.warn(`No log files found for ${dirName}`);
      }

      logFilesPerCourseRun[dirName] = files;
    }
  }

  return logFilesPerCourseRun;
}

/**
 * Function that runs all the necessary functions to process the sessions for a course run
 * @param {string} courseRunDirName The name of the course run directory
 * @param {string[]} logFiles The log file paths for the course run
 */
async function processSessionsForCourseRun(courseRunDirName, logFiles) {
  await processGeneralSessions(courseRunDirName, logFiles);
  await processVideoInteractionSessions(courseRunDirName, logFiles);
  await processAssessmentsSubmissions(courseRunDirName, logFiles);
  await processQuizSessions(courseRunDirName, logFiles);
  await processORASessions(courseRunDirName, logFiles);
}

/**
 * Function that processes all the metadata files for a course run and then processes the sessions
 * @param {string} courseRunDirName The name of the course run directory
 * @param {string[]} logFiles The log file paths for the course run
 * @param {string} coursesDirectory The top-level directory all course runs are in
 */
async function processCourseRun(courseRunDirName, logFiles, coursesDirectory) {
  await readMetadataFiles(
    path.join(coursesDirectory, courseRunDirName),
    courseRunDirName,
  );
  await processSessionsForCourseRun(courseRunDirName, logFiles);
}

async function main() {
  const testing = false;

  let courses = ["EX101x", "FP101x", "ST1x", "UnixTx"];
  let workingDirectory = "W:/staff-umbrella/gdicsmoocs/Working copy";

  if (testing) {
    courses = ["UnixTx"];
    workingDirectory =
      "W:/staff-umbrella/gdicsmoocs/Working copy/scripts/testing";
  }
  try {
    testConnection();

    if (testing) {
      await clearDatabase();
    } else {
      await clearSessionsCollections();
    }

    const logFilesPerCourseRun =
      await identifyLogFilesPerCourseRun(workingDirectory);

    const courseRunPromises = [];

    for (const [courseRunDirName, logFiles] of Object.entries(
      logFilesPerCourseRun,
    )) {
      courseRunPromises.push(
        processCourseRun(courseRunDirName, logFiles, workingDirectory),
      );
    }

    await Promise.all(courseRunPromises);
  } catch (error) {
    console.error(error);
  }
}

main();
