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
const { promisify } = require("util");
const zlib = require("zlib");

const cliProgress = require("cli-progress");

const gunzip = promisify(zlib.gunzip);

/**
 * Function to check if a file has a valid gzip header.
 * @param {string} filePath Path to the file to check.
 * @returns {Promise<boolean>} Promise that resolves to true if the file is valid, false otherwise.
 */
async function isValidGzipFile(filePath) {
  try {
    const fileBuffer = await fs.promises.readFile(filePath);
    await gunzip(fileBuffer);
    return true;
  } catch (error) {
    console.warn(`${filePath} does not have a valid gzip header.`);
    return false;
  }
}

/**
 * Function that identifies the log files for each course run
 * @param {string} directoryPath The path to the directory containing the course run directories
 * @param {string[]} courses The course names to look for
 * @returns {Object} An object with course run directory names as keys and an array of log file paths as values
 */
async function identifyLogFilesPerCourseRun(directoryPath, courses) {
  const fileEnding = ".log.gz";
  const identifyLogFilesBar = new cliProgress.MultiBar(
    {
      clearOnComplete: false,
      hideCursor: true,
      format:
        " {bar} | Identifying log files for {course_run} | {value}/{total} | Duration: {duration_formatted} | ETA: {eta_formatted}",
    },
    cliProgress.Presets.shades_classic
  );

  let logFilesPerCourseRun = {};
  let directories = await fs.promises.readdir(directoryPath);
  for (let dirName of directories) {
    if (courses.some((course) => dirName.includes(course))) {
      let coursePath = path.join(path.resolve(directoryPath), dirName);
      let files = [];
      let fileNames = await fs.promises.readdir(coursePath);
      const bar = identifyLogFilesBar.create(fileNames.length, 0, {
        course_run: dirName,
      });
      for (let fileName of fileNames) {
        if (fileName.endsWith(fileEnding)) {
          const fullPath = path.join(coursePath, fileName);
          if (await isValidGzipFile(fullPath)) {
            files.push(fullPath);
          } else {
            console.warn(`Skipping invalid gzip file: ${fullPath}`);
          }
        }
        bar.increment();
      }

      if (files.length === 0) {
        console.warn(`No valid gzip log files found for ${dirName}`);
      }

      logFilesPerCourseRun[dirName] = files;
    }
  }
  identifyLogFilesBar.stop();
  return logFilesPerCourseRun;
}

/**
 * Function that runs all the necessary functions to process the sessions for a course run
 * @param {string} courseRunDirName The name of the course run directory
 * @param {string[]} logFiles The log file paths for the course run
 * @param {cliProgress.MultiBar} bar The progress bar
 */
async function processSessionsForCourseRun(courseRunDirName, logFiles, bar) {
  const processGeneralSessionsBar = bar.create(logFiles.length, 0, {
    course_run: courseRunDirName,
    task: "General Sessions",
  });
  await processGeneralSessions(
    courseRunDirName,
    logFiles,
    processGeneralSessionsBar
  );
  processGeneralSessionsBar.stop();

  const processVideoInteractionSessionsBar = bar.create(logFiles.length, 0, {
    course_run: courseRunDirName,
    task: "Video Interaction Sessions",
  });
  await processVideoInteractionSessions(
    courseRunDirName,
    logFiles,
    processVideoInteractionSessionsBar
  );
  processVideoInteractionSessionsBar.stop();

  const processAssessmentsSubmissionsBar = bar.create(logFiles.length, 0, {
    course_run: courseRunDirName,
    task: "Assessments Submissions",
  });
  await processAssessmentsSubmissions(
    logFiles,
    processAssessmentsSubmissionsBar
  );
  processAssessmentsSubmissionsBar.stop();

  const processQuizSessionsBar = bar.create(logFiles.length, 0, {
    course_run: courseRunDirName,
    task: "Quiz Sessions",
  });
  await processQuizSessions(courseRunDirName, logFiles, processQuizSessionsBar);
  processQuizSessionsBar.stop();

  const processORASessionsBar = bar.create(logFiles.length, 0, {
    course_run: courseRunDirName,
    task: "ORA Sessions",
  });
  await processORASessions(courseRunDirName, logFiles, processORASessionsBar);
  processORASessionsBar.stop();
}

/**
 * Function that processes all the metadata files for a course run and then processes the sessions
 * @param {string} courseRunDirName The name of the course run directory
 * @param {string[]} logFiles The log file paths for the course run
 * @param {string} coursesDirectory The top-level directory all course runs are in
 * @param {cliProgress.MultiBar} bar The progress bar
 */
async function processCourseRun(
  courseRunDirName,
  logFiles,
  coursesDirectory,
  bar
) {
  await readMetadataFiles(
    path.join(coursesDirectory, courseRunDirName),
    courseRunDirName
  );
  await processSessionsForCourseRun(courseRunDirName, logFiles, bar);
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

    await clearDatabase();

    const logFilesPerCourseRun = await identifyLogFilesPerCourseRun(
      workingDirectory,
      courses
    );

    const logProcessingBar = new cliProgress.MultiBar(
      {
        clearOnComplete: false,
        hideCursor: true,
        format:
          " {bar} | Processing {task} for {course_run} | {value}/{total} | Duration: {duration_formatted} | ETA: {eta_formatted}",
      },
      cliProgress.Presets.shades_grey
    );

    for (let courseRunDirName in logFilesPerCourseRun) {
      const logFiles = logFilesPerCourseRun[courseRunDirName];
      await processCourseRun(
        courseRunDirName,
        logFiles,
        workingDirectory,
        logProcessingBar
      );
    }

    logProcessingBar.stop();
  } catch (error) {
    console.error(error);
  }
}

main();
