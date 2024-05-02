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
  processForumSessions,
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
    return false;
  }
}

/**
 * Function that identifies the log files for each course run
 * @param {string} coursesDirectory The path to the directory containing the course run directories
 * @param {string[]} courses The course names to look for
 * @returns {Object} An object with course run directory names as keys and an array of log file paths as values
 */
async function identifyLogFilesPerCourseRun(coursesDirectory, courses) {
  const fileEnding = ".log.gz";

  let logFilesPerCourseRun = {};
  let warnings = [];
  let directories = await fs.promises.readdir(coursesDirectory);

  const courseDirectoryNames = directories.filter((dirName) =>
    courses.some((course) => dirName.includes(course))
  );

  for (let courseDirectoryName of courseDirectoryNames) {
    const courseDirectory = path.join(coursesDirectory, courseDirectoryName);
    const courseDirStats = await fs.promises.stat(courseDirectory);
    if (!courseDirStats.isDirectory()) {
      warnings.push(`${courseDirectoryName} is not a directory`);
      continue;
    }

    let courseFiles = await fs.promises.readdir(courseDirectory);
    let totalFiles = courseFiles.length;
    let courseRunBar = new cliProgress.SingleBar(
      {
        total: totalFiles,
        hideCursor: true,
        barsize: 40,
        etaBuffer: 20,
        forceRedraw: true,
        format:
          "{bar} | Identifying and verifying log files for {course_run} | {value}/{total} | Duration: {duration_formatted} | ETA: {eta_formatted}",
        clearOnComplete: true,
      },
      cliProgress.Presets.shades_classic
    );

    courseRunBar.start(totalFiles, 0, { course_run: courseDirectoryName });

    let files = [];
    for (let fileName of courseFiles) {
      if (fileName.endsWith(fileEnding)) {
        const fullPath = path.join(courseDirectory, fileName);
        if (await isValidGzipFile(fullPath)) {
          files.push(fullPath);
        } else {
          warnings.push(`Skipping invalid gzip file: ${fullPath}`);
        }
      }
      courseRunBar.increment();
    }

    if (files.length === 0) {
      warnings.push(`No valid gzip log files found for ${courseDirectoryName}`);
    }

    logFilesPerCourseRun[courseDirectoryName] = files;

    courseRunBar.stop();
  }

  for (let warning of warnings) {
    console.warn(warning);
  }

  if (warnings.length > 0) {
    console.log("\n");
  }

  return logFilesPerCourseRun;
}

/**
 * Function that runs all the necessary functions to process the sessions for a course run
 * @param {string} courseRunDirName The name of the course run directory
 * @param {string[]} logFiles The log file paths for the course run
 */
async function processSessionsForCourseRun(courseRunDirName, logFiles) {
  const sessionsBar = new cliProgress.SingleBar(
    {
      hideCursor: true,
      barsize: 40,
      etaBuffer: 20,
      forceRedraw: true,
      clearOnComplete: true,
      format:
        " {bar} | Processing sessions for {course_run} | {value}/{total} | Duration: {duration_formatted} | ETA: {eta_formatted}",
    },
    cliProgress.Presets.shades_classic
  );

  const numberOfLogProcessingFunctions = 6;
  sessionsBar.start(numberOfLogProcessingFunctions, 0, {
    course_run: courseRunDirName,
  });

  await processGeneralSessions(courseRunDirName, logFiles);
  sessionsBar.increment();

  await processVideoInteractionSessions(courseRunDirName, logFiles);
  sessionsBar.increment();

  await processAssessmentsSubmissions(logFiles);
  sessionsBar.increment();

  await processQuizSessions(courseRunDirName, logFiles);
  sessionsBar.increment();

  await processORASessions(courseRunDirName, logFiles);
  sessionsBar.increment();

  await processForumSessions(courseRunDirName, logFiles);
  sessionsBar.increment();

  sessionsBar.stop();
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
    courseRunDirName
  );
  await processSessionsForCourseRun(courseRunDirName, logFiles);
}

async function main() {
  const dev = false;

  let courses = ["FP101x", "UnixTx", "EX101x", "ST1x"];
  let workingDirectory = "W:/staff-umbrella/gdicsmoocs/Working copy";

  if (dev) {
    courses = ["UnixTx", "EX101x"];
    workingDirectory =
      "W:/staff-umbrella/gdicsmoocs/Working copy/scripts/testing";
  }
  try {
    testConnection(dev);

    await clearDatabase(dev);

    const logFilesPerCourseRun = await identifyLogFilesPerCourseRun(
      workingDirectory,
      courses
    );

    for (let [courseRunDirName, logFiles] of Object.entries(
      logFilesPerCourseRun
    )) {
      await processCourseRun(courseRunDirName, logFiles, workingDirectory);
    }
  } catch (error) {
    console.error(error);
  }
}

main();
