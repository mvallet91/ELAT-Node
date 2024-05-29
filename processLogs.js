const { mongoQuery, mongoInsert } = require("./databaseHelpers");
const { processNull, getNextDay, courseElementsFinder } = require("./helpers");
const createReadStream = require("fs").createReadStream;
const createGunzip = require("zlib").createGunzip;
const createInterface = require("readline").createInterface;

async function* readLines(logFile) {
  const fileStream = createReadStream(logFile);
  const gzipStream = createGunzip();
  const rl = createInterface({
    input: fileStream.pipe(gzipStream),
  });

  for await (const line of rl) {
    yield line;
  }
}

/**
 * This function will read the records in the logfile and extract all interactions from students with the course,
 * process their values, like start, end and duration, and finally store them in the database
 * @param {string} courseRunName - The name of the course run
 * @param {string[]} logFiles - The list of log files to process
 */
async function processGeneralSessions(courseRunName, logFiles) {
  let courseMetadataMap = await mongoQuery("metadata", {
    name: courseRunName,
  });
  courseMetadataMap = courseMetadataMap[0]["object"];

  const courseId = courseMetadataMap["course_id"];

  const currentCourseId = courseId.slice(
    courseId.indexOf("+") + 1,
    courseId.lastIndexOf("+") + 7,
  );

  for (const logFile of logFiles) {
    let learnerAllEventLogs = [];
    let updatedLearnerAllEventLogs = [];
    let sessionRecord = [];
    learnerAllEventLogs = JSON.parse(
      JSON.stringify(updatedLearnerAllEventLogs),
    );

    let courseLearnerIdSet = new Set();
    for (const courseLearnerId in learnerAllEventLogs) {
      courseLearnerIdSet.add(courseLearnerId);
    }

    for await (const line of readLines(logFile)) {
      if (line.length < 10 || !line.includes(currentCourseId)) {
        continue;
      }

      let jsonObject = JSON.parse(line);
      if (
        !Object.prototype.hasOwnProperty.call(jsonObject["context"], "user_id")
      ) {
        continue;
      }
      let globalLearnerId = jsonObject["context"]["user_id"];
      let eventType = jsonObject["event_type"];

      if (globalLearnerId !== "") {
        let courseId = jsonObject["context"]["course_id"];
        let courseLearnerId = courseId + "_" + globalLearnerId;
        let eventTime = new Date(jsonObject["time"]);
        if (courseLearnerIdSet.has(courseLearnerId)) {
          learnerAllEventLogs[courseLearnerId].push({
            event_time: eventTime,
            event_type: eventType,
          });
        } else {
          learnerAllEventLogs[courseLearnerId] = [
            { event_time: eventTime, event_type: eventType },
          ];
          courseLearnerIdSet.add(courseLearnerId);
        }
      }
    }

    for (let courseLearnerId in learnerAllEventLogs) {
      let eventLogs = learnerAllEventLogs[courseLearnerId];

      eventLogs.sort(function (a, b) {
        return new Date(a.event_time) - new Date(b.event_time);
      });

      let sessionId = null,
        startTime = null,
        endTime = null,
        finalTime = null;

      for (let i in eventLogs) {
        if (startTime == null) {
          startTime = new Date(eventLogs[i]["event_time"]);
          endTime = new Date(eventLogs[i]["event_time"]);
        } else {
          let verificationTime = new Date(endTime);
          if (
            new Date(eventLogs[i]["event_time"]) >
            verificationTime.setMinutes(verificationTime.getMinutes() + 30)
          ) {
            let sessionId =
              courseLearnerId +
              "_" +
              startTime.getTime() +
              "_" +
              endTime.getTime();

            let duration = (endTime - startTime) / 1000;

            if (duration > 5) {
              let array = [
                sessionId,
                courseLearnerId,
                startTime,
                endTime,
                duration,
              ];
              sessionRecord.push(array);
            }

            finalTime = new Date(eventLogs[i]["event_time"]);

            //Re-initialization
            sessionId = "";
            startTime = new Date(eventLogs[i]["event_time"]);
            endTime = new Date(eventLogs[i]["event_time"]);
          } else {
            if (eventLogs[i]["event_type"] === "page_close") {
              endTime = new Date(eventLogs[i]["event_time"]);
              sessionId =
                courseLearnerId +
                "_" +
                startTime.getTime() +
                "_" +
                endTime.getTime();
              let duration = (endTime - startTime) / 1000;

              if (duration > 5) {
                let array = [
                  sessionId,
                  courseLearnerId,
                  startTime,
                  endTime,
                  duration,
                ];
                sessionRecord.push(array);
              }
              sessionId = "";
              startTime = null;
              endTime = null;

              finalTime = new Date(eventLogs[i]["event_time"]);
            } else {
              endTime = new Date(eventLogs[i]["event_time"]);
            }
          }
        }
      }
      if (finalTime != null) {
        let newLogs = [];
        for (let x in eventLogs) {
          let log = eventLogs[x];
          if (new Date(log["event_time"]) >= finalTime) {
            newLogs.push(log);
          }
        }
        updatedLearnerAllEventLogs[courseLearnerId] = newLogs;
      }
    }

    let updatedSessionRecord = [];
    let sessionIdSet = new Set();
    for (let array of sessionRecord) {
      let sessionId = array[0];
      if (!sessionIdSet.has(sessionId)) {
        sessionIdSet.add(sessionId);
        updatedSessionRecord.push(array);
      }
    }

    sessionRecord = updatedSessionRecord;
    if (sessionRecord.length > 0) {
      let data = [];
      for (let x in sessionRecord) {
        let array = sessionRecord[x];
        let sessionId = array[0];
        let courseLearnerId = array[1];
        let startTime = array[2];
        let endTime = array[3];
        let duration = processNull(array[4]);
        let values = {
          session_id: sessionId,
          course_learner_id: courseLearnerId,
          start_time: startTime,
          end_time: endTime,
          duration: duration,
        };
        data.push(values);
      }
      await mongoInsert("sessions", data);
    }
  }
}

/**
 * This function will read the records in the logfile and extract all interactions from students with the forums,
 * process their values, like duration and times they search, to finally store them in the database
 * @param {string} courseRunName - The name of the course run
 * @param {Array} logFiles - The list of log files
 */
async function processVideoInteractionSessions(courseRunName, logFiles) {
  let courseMetadataMap = await mongoQuery("metadata", {
    name: courseRunName,
  });

  courseMetadataMap = courseMetadataMap[0]["object"];
  const courseId = courseMetadataMap["course_id"],
    currentCourseId = courseId.slice(
      courseId.indexOf("+") + 1,
      courseId.lastIndexOf("+") + 7,
    );

  const videoEventTypes = [
    "hide_transcript",
    "edx.video.transcript.hidden",
    "edx.video.closed_captions.hidden",
    "edx.video.closed_captions.shown",
    "load_video",
    "edx.video.loaded",
    "pause_video",
    "edx.video.paused",
    "play_video",
    "edx.video.played",
    "seek_video",
    "edx.video.position.changed",
    "show_transcript",
    "edx.video.transcript.shown",
    "speed_change_video",
    "stop_video",
    "edx.video.stopped",
    "video_hide_cc_menu",
    "edx.video.language_menu.hidden",
    "video_show_cc_menu",
    "edx.video.language_menu.shown",
  ];

  for (const logFile of logFiles) {
    let videoInteractionMap = {},
      learnerVideoEventLogs = {},
      courseLearnerIdSet = new Set();
    for await (const line of readLines(logFile)) {
      if (line.length < 10 || !line.includes(currentCourseId)) {
        continue;
      }
      let jsonObject = JSON.parse(line);
      if (videoEventTypes.includes(jsonObject["event_type"])) {
        if (!("user_id" in jsonObject["context"])) {
          continue;
        }
        let globalLearnerId = jsonObject["context"]["user_id"];
        if (globalLearnerId !== "") {
          const courseId = jsonObject["context"]["course_id"],
            courseLearnerId = courseId + "_" + globalLearnerId,
            eventTime = new Date(jsonObject["time"]),
            eventType = jsonObject["event_type"];
          let videoId = "",
            newTime = 0,
            oldTime = 0,
            newSpeed = 0,
            oldSpeed = 0,
            eventJsonObject;

          if (typeof jsonObject["event"] === "string") {
            eventJsonObject = JSON.parse(jsonObject["event"]);
          } else {
            eventJsonObject = jsonObject["event"];
          }
          videoId = eventJsonObject["id"];
          videoId = videoId.replace("-", "://");
          videoId = videoId.replace(/-/g, "/");
          if ("new_time" in eventJsonObject && "old_time" in eventJsonObject) {
            newTime = eventJsonObject["new_time"];
            oldTime = eventJsonObject["old_time"];
          }
          if (
            "new_speed" in eventJsonObject &&
            "old_speed" in eventJsonObject
          ) {
            newSpeed = eventJsonObject["new_speed"];
            oldSpeed = eventJsonObject["old_speed"];
          }
          if (
            ["seek_video", "edx.video.position.changed"].includes(eventType)
          ) {
            if (newTime != null && oldTime != null) {
              if (courseLearnerIdSet.has(courseLearnerId)) {
                learnerVideoEventLogs[courseLearnerId].push({
                  event_time: eventTime,
                  event_type: eventType,
                  video_id: videoId,
                  new_time: newTime,
                  old_time: oldTime,
                });
              } else {
                learnerVideoEventLogs[courseLearnerId] = [
                  {
                    event_time: eventTime,
                    event_type: eventType,
                    video_id: videoId,
                    new_time: newTime,
                    old_time: oldTime,
                  },
                ];
                courseLearnerIdSet.add(courseLearnerId);
              }
            }
            continue;
          }
          if (["speed_change_video"].includes(eventType)) {
            if (courseLearnerIdSet.has(courseLearnerId)) {
              learnerVideoEventLogs[courseLearnerId].push({
                event_time: eventTime,
                event_type: eventType,
                video_id: videoId,
                new_speed: newSpeed,
                old_speed: oldSpeed,
              });
            } else {
              learnerVideoEventLogs[courseLearnerId] = [
                {
                  event_time: eventTime,
                  event_type: eventType,
                  video_id: videoId,
                  new_speed: newSpeed,
                  old_speed: oldSpeed,
                },
              ];
              courseLearnerIdSet.add(courseLearnerId);
            }
            continue;
          }
          if (courseLearnerIdSet.has(courseLearnerId)) {
            learnerVideoEventLogs[courseLearnerId].push({
              event_time: eventTime,
              event_type: eventType,
              video_id: videoId,
            });
          } else {
            learnerVideoEventLogs[courseLearnerId] = [
              {
                event_time: eventTime,
                event_type: eventType,
                video_id: videoId,
              },
            ];
            courseLearnerIdSet.add(courseLearnerId);
          }
        }
      }
      if (!videoEventTypes.includes(jsonObject["event_type"])) {
        if (!("user_id" in jsonObject["context"])) {
          continue;
        }
        let globalLearnerId = jsonObject["context"]["user_id"];
        if (globalLearnerId !== "") {
          let courseId = jsonObject["context"]["course_id"],
            courseLearnerId = courseId + "_" + globalLearnerId,
            eventTime = new Date(jsonObject["time"]),
            eventType = jsonObject["event_type"];
          if (courseLearnerIdSet.has(courseLearnerId)) {
            learnerVideoEventLogs[courseLearnerId].push({
              event_time: eventTime,
              event_type: eventType,
            });
          } else {
            learnerVideoEventLogs[courseLearnerId] = [
              { event_time: eventTime, event_type: eventType },
            ];
            courseLearnerIdSet.add(courseLearnerId);
          }
        }
      }
    }

    for (let courseLearnerId in learnerVideoEventLogs) {
      let eventLogs = learnerVideoEventLogs[courseLearnerId];
      eventLogs.sort(function (a, b) {
        return new Date(a.event_time) - new Date(b.event_time);
      });
      let videoId = "",
        videoStartTime = null,
        timesForwardSeek = 0,
        durationForwardSeek = 0,
        timesBackwardSeek = 0,
        durationBackwardSeek = 0,
        speedChangeLastTime = "",
        timesSpeedUp = 0,
        timesSpeedDown = 0,
        pauseCheck = false,
        pauseStartTime = null;
      for (let log of eventLogs) {
        if (["play_video", "edx.video.played"].includes(log["event_type"])) {
          videoStartTime = new Date(log["event_time"]);
          videoId = log["video_id"];
          if (pauseCheck) {
            let durationPause =
              (new Date(log["event_time"]) - pauseStartTime) / 1000;
            let videoInteractionId =
              courseLearnerId + "_" + videoId + "_" + pauseStartTime.getTime();
            if (durationPause > 2 && durationPause < 600) {
              if (videoInteractionId in videoInteractionMap) {
                if (
                  Object.prototype.hasOwnProperty.call(
                    videoInteractionMap[videoInteractionId],
                    "times_pause",
                  )
                ) {
                  videoInteractionMap[videoInteractionId]["times_pause"] =
                    videoInteractionMap[videoInteractionId]["times_pause"] + 1;
                  videoInteractionMap[videoInteractionId]["duration_pause"] =
                    videoInteractionMap[videoInteractionId]["duration_pause"] +
                    durationPause;
                } else {
                  videoInteractionMap[videoInteractionId]["times_pause"] = 1;
                  videoInteractionMap[videoInteractionId]["duration_pause"] =
                    durationPause;
                }
              }
            }
            pauseCheck = false;
          }
          continue;
        }
        if (videoStartTime != null) {
          let verificationTime = new Date(videoStartTime);
          if (
            log["event_time"] >
            verificationTime.setMinutes(verificationTime.getMinutes() + 30)
          ) {
            videoStartTime = null;
            videoId = "";
          } else {
            // Seek
            if (
              ["seek_video", "edx.video.position.changed"].includes(
                log["event_type"],
              ) &&
              videoId === log["video_id"]
            ) {
              if (log["new_time"] > log["old_time"]) {
                timesForwardSeek++;
                durationForwardSeek += log["new_time"] - log["old_time"];
              }
              if (log["new_time"] < log["old_time"]) {
                timesBackwardSeek++;
                durationBackwardSeek += log["old_time"] - log["new_time"];
              }
              continue;
            }

            // Speed Changes
            if (
              log["event_type"] === "speed_change_video" &&
              videoId === log["video_id"]
            ) {
              if (speedChangeLastTime === "") {
                speedChangeLastTime = log["event_time"];
                let oldSpeed = log["old_speed"];
                let newSpeed = log["new_speed"];
                if (oldSpeed < newSpeed) {
                  timesSpeedUp++;
                }
                if (oldSpeed > newSpeed) {
                  timesSpeedDown++;
                }
              } else {
                if ((log["event_time"] - speedChangeLastTime) / 1000 > 10) {
                  let oldSpeed = log["old_speed"];
                  let newSpeed = log["new_speed"];
                  if (oldSpeed < newSpeed) {
                    timesSpeedUp++;
                  }
                  if (oldSpeed > newSpeed) {
                    timesSpeedDown++;
                  }
                }
                speedChangeLastTime = log["event_time"];
              }
              continue;
            }

            // Pause/Stop Situation
            if (
              [
                "pause_video",
                "edx.video.paused",
                "stop_video",
                "edx.video.stopped",
              ].includes(log["event_type"]) &&
              videoId === log["video_id"]
            ) {
              let watchDuration =
                  (new Date(log["event_time"]) - videoStartTime) / 1000,
                videoEndTime = new Date(log["event_time"]),
                videoInteractionId =
                  courseLearnerId +
                  "_" +
                  videoId +
                  "_" +
                  videoEndTime.getTime();
              if (watchDuration > 5) {
                videoInteractionMap[videoInteractionId] = {
                  course_learner_id: courseLearnerId,
                  video_id: videoId,
                  type: "video",
                  watch_duration: watchDuration,
                  times_forward_seek: timesForwardSeek,
                  duration_forward_seek: durationForwardSeek,
                  times_backward_seek: timesBackwardSeek,
                  duration_backward_seek: durationBackwardSeek,
                  times_speed_up: timesSpeedUp,
                  times_speed_down: timesSpeedDown,
                  start_time: videoStartTime,
                  end_time: videoEndTime,
                };
              }
              if (
                ["pause_video", "edx.video.paused"].includes(log["event_type"])
              ) {
                pauseCheck = true;
                pauseStartTime = new Date(videoEndTime);
              }
              timesForwardSeek = 0;
              durationForwardSeek = 0;
              timesBackwardSeek = 0;
              durationBackwardSeek = 0;
              speedChangeLastTime = "";
              timesSpeedUp = 0;
              timesSpeedDown = 0;
              videoStartTime = null;
              videoId = "";
              continue;
            }

            // Page Changed/Session Closed
            if (!videoEventTypes.includes(log["event_type"])) {
              let videoEndTime = new Date(log["event_time"]);
              let watchDuration = (videoEndTime - videoStartTime) / 1000;
              let videoInteractionId =
                courseLearnerId + "_" + videoId + "_" + videoEndTime.getTime();
              if (watchDuration > 5) {
                videoInteractionMap[videoInteractionId] = {
                  course_learner_id: courseLearnerId,
                  video_id: videoId,
                  type: "video",
                  watch_duration: watchDuration,
                  times_forward_seek: timesForwardSeek,
                  duration_forward_seek: durationForwardSeek,
                  times_backward_seek: timesBackwardSeek,
                  duration_backward_seek: durationBackwardSeek,
                  times_speed_up: timesSpeedUp,
                  times_speed_down: timesSpeedDown,
                  start_time: videoStartTime,
                  end_time: videoEndTime,
                };
              }
              timesForwardSeek = 0;
              durationForwardSeek = 0;
              timesBackwardSeek = 0;
              durationBackwardSeek = 0;
              speedChangeLastTime = "";
              timesSpeedUp = 0;
              timesSpeedDown = 0;
              videoStartTime = null;
              videoId = "";
            }
          }
        }
      }
    }

    let videoInteractionRecord = [];
    for (let interactionId in videoInteractionMap) {
      const videoInteractionId = interactionId,
        courseLearnerId =
          videoInteractionMap[interactionId]["course_learner_id"],
        videoId = videoInteractionMap[interactionId]["video_id"],
        duration = videoInteractionMap[interactionId]["watch_duration"],
        timesForwardSeek =
          videoInteractionMap[interactionId]["times_forward_seek"],
        durationForwardSeek =
          videoInteractionMap[interactionId]["duration_forward_seek"],
        timesBackwardSeek =
          videoInteractionMap[interactionId]["times_backward_seek"],
        durationBackwardSeek =
          videoInteractionMap[interactionId]["duration_backward_seek"],
        timesSpeedUp = videoInteractionMap[interactionId]["times_speed_up"],
        timesSpeedDown = videoInteractionMap[interactionId]["times_speed_down"],
        startTime = videoInteractionMap[interactionId]["start_time"],
        endTime = videoInteractionMap[interactionId]["end_time"];

      let timesPause = 0,
        durationPause = 0;

      if (
        Object.prototype.hasOwnProperty.call(
          videoInteractionMap[interactionId],
          "times_pause",
        )
      ) {
        timesPause = videoInteractionMap[interactionId]["times_pause"];
        durationPause = videoInteractionMap[interactionId]["duration_pause"];
      }

      videoInteractionMap[videoInteractionId]["session_id"] =
        videoInteractionId;
      videoInteractionMap[videoInteractionId]["times_pause"] = timesPause;
      videoInteractionMap[videoInteractionId]["duration_pause"] = durationPause;

      let array = [
        videoInteractionId,
        courseLearnerId,
        videoId,
        duration,
        timesForwardSeek,
        durationForwardSeek,
        timesBackwardSeek,
        durationBackwardSeek,
        timesSpeedUp,
        timesSpeedDown,
        timesPause,
        durationPause,
        startTime,
        endTime,
      ];
      array = array.map(function (value) {
        if (typeof value === "number") {
          return Math.round(value);
        } else {
          return value;
        }
      });
      videoInteractionRecord.push(videoInteractionMap[videoInteractionId]);
    }
    if (videoInteractionRecord.length > 0) {
      await mongoInsert("video_interactions", videoInteractionRecord);
    }
  }
}

/**
 * This function will read the records in the log database and extract the submissions and (automatic) assessments
 * of quiz questions, process their values, like timestamp or grade, to finally store them in the database
 * @param {string[]} logFiles - The list of log files
 */
async function processAssessmentsSubmissions(logFiles) {
  let submissionEventCollection = ["problem_check"];

  for (const logFile of logFiles) {
    let submissionData = [],
      assessmentData = [];
    for await (const line of readLines(logFile)) {
      let jsonObject = JSON.parse(line);
      if (submissionEventCollection.includes(jsonObject["event_type"])) {
        if (!("user_id" in jsonObject["context"])) {
          continue;
        }
        const globalLearnerId = jsonObject["context"]["user_id"];
        if (globalLearnerId === "") {
          continue;
        }
        const courseId = jsonObject["context"]["course_id"],
          courseLearnerId = courseId + "_" + globalLearnerId,
          eventTime = new Date(jsonObject["time"]);
        let questionId = "",
          grade = "",
          maxGrade = "";
        if (typeof jsonObject["event"] === "object") {
          questionId = jsonObject["event"]["problem_id"];
          if (
            "grade" in jsonObject["event"] &&
            "max_grade" in jsonObject["event"]
          ) {
            grade = jsonObject["event"]["grade"];
            maxGrade = jsonObject["event"]["max_grade"];
          }
        }
        if (questionId !== "") {
          const submissionId = courseLearnerId + "_" + questionId;
          const values = {
            submission_id: submissionId,
            course_learner_id: courseLearnerId,
            question_id: questionId,
            submission_timestamp: eventTime,
          };
          submissionData.push(values);
          if (grade !== "" && maxGrade !== "") {
            const values = {
              assessment_id: submissionId,
              course_learner_id: courseLearnerId,
              max_grade: maxGrade,
              grade: grade,
            };
            assessmentData.push(values);
          }
        }
      }
    }
    if (assessmentData.length > 0) {
      await mongoInsert("assessments", assessmentData);
    }
    if (submissionData.length > 0) {
      await mongoInsert("submissions", submissionData);
    }
  }
}

/**
 * This function will read the records in the logfile and extract the submissions and (automatic) assessments
 * of quiz questions, process their values, like timestamp or grade, to finally store them in the database
 * @param {string} courseRunName - The name of the course run
 * @param {string[]} logFiles - The list of log files
 */
async function processQuizSessions(courseRunName, logFiles) {
  let courseMetadataMap = await mongoQuery("metadata", {
    name: courseRunName,
  });
  courseMetadataMap = courseMetadataMap[0]["object"];
  const courseId = courseMetadataMap["course_id"];
  const currentCourseId = courseId.slice(
    courseId.indexOf("+") + 1,
    courseId.lastIndexOf("+") + 7,
  );

  let childParentMap = courseMetadataMap["child_parent_map"];

  for (const logFile of logFiles) {
    let learnerAllEventLogs = {},
      updatedLearnerAllEventLogs = {},
      quizSessions = {};

  for (let i = 0; i < logFiles.length; i++) {
    const logFile = logFiles[i];
    (learnerAllEventLogs = updatedLearnerAllEventLogs),
      (updatedLearnerAllEventLogs = {});
    let courseLearnerIdSet = new Set();

    if (learnerAllEventLogs.length > 0) {
      for (const courseLearnerId of learnerAllEventLogs) {
        courseLearnerIdSet.add(courseLearnerId);
      }
    }

    const submissionEventCollection = [
      "problem_check",
      "save_problem_check",
      "problem_check_fail",
      "save_problem_check_fail",
      "problem_graded",
      "problem_rescore",
      "problem_rescore_fail",
      "problem_reset",
      "reset_problem",
      "reset_problem_fail",
      "problem_save",
      "save_problem_fail",
      "save_problem_success",
      "problem_show",
      "showanswer",
    ];

    for await (const line of readLines(logFile)) {
      if (line.length < 10 || !line.includes(currentCourseId)) {
        continue;
      }

      const jsonObject = JSON.parse(line);
      if (!("user_id" in jsonObject["context"])) {
        continue;
      }
      const globalLearnerId = jsonObject["context"]["user_id"],
        event_type = jsonObject["event_type"];

      if (globalLearnerId === "") {
        continue;
      }
      const courseId = jsonObject["context"]["course_id"],
        courseLearnerId = courseId + "_" + globalLearnerId,
        eventTime = new Date(jsonObject["time"]);
      const event = jsonObject["event"];
      if (courseLearnerId in learnerAllEventLogs) {
        learnerAllEventLogs[courseLearnerId].push({
          event_time: eventTime,
          event_type: event_type,
          event: event,
        });
      } else {
        learnerAllEventLogs[courseLearnerId] = [
          {
            event_time: eventTime,
            event_type: event_type,
            event: event,
          },
        ];
      }
    }

    // TODO: VERIFY QUIZ SESSIONS, not catching problem id
    // different structure from original logs to mongodb

    for (const courseLearnerId in learnerAllEventLogs) {
      if (
        !Object.prototype.hasOwnProperty.call(
          learnerAllEventLogs,
          courseLearnerId,
        )
      ) {
        continue;
      }
      let eventLogs = learnerAllEventLogs[courseLearnerId];
      eventLogs.sort(function (a, b) {
        return b.event_type - a.event_type;
      });
      eventLogs.sort(function (a, b) {
        return new Date(a.event_time) - new Date(b.event_time);
      });
      let sessionId = "",
        startTime = null,
        endTime = null,
        finalTime = null;
      for (const i in eventLogs) {
        if (sessionId === "") {
          if (
            eventLogs[i]["event_type"].includes("problem+block") ||
            eventLogs[i]["event_type"].includes("_problem;_") ||
            submissionEventCollection.includes(eventLogs[i]["event_type"])
          ) {
            let questionId = "";
            if (eventLogs[i]["event_type"].includes("problem+block")) {
              // questionId = eventTypeArray[4];
              questionId = eventLogs[i]["event"].problem_id;
            }
            if (eventLogs[i]["event_type"].includes("_problem;_")) {
              // questionId = eventTypeArray[6].replace(/;_/g, '/');
              questionId = eventLogs[i]["event"]["problem_id"];
            }
            if (Array.isArray(eventLogs[i]["event"])) {
              const input = eventLogs[i]["event"][0];
              const blockId = input.split("_")[1];
              const key = Object.keys(childParentMap).find((key) =>
                key.includes(blockId)
              );
              if (key) {
                questionId = key;
              }
            }
            if (questionId in childParentMap) {
              sessionId =
                "quiz_session_" +
                childParentMap[questionId] +
                "_" +
                courseLearnerId;
              startTime = new Date(eventLogs[i]["event_time"]);
              endTime = new Date(eventLogs[i]["event_time"]);
            }
          }
        } else {
          if (
            eventLogs[i]["event_type"].includes("problem+block") ||
            eventLogs[i]["event_type"].includes("_problem;_") ||
            submissionEventCollection.includes(eventLogs[i]["event_type"])
          ) {
            let verificationTime = new Date(endTime);
            if (
              new Date(eventLogs[i]["event_time"]) >
              verificationTime.setMinutes(verificationTime.getMinutes() + 30)
            ) {
              if (sessionId in quizSessions) {
                quizSessions[sessionId]["time_array"].push({
                  start_time: startTime,
                  end_time: endTime,
                });
              } else {
                quizSessions[sessionId] = {
                  course_learner_id: courseLearnerId,
                  time_array: [{ start_time: startTime, end_time: endTime }],
                };
              }
              finalTime = eventLogs[i]["event_time"];
              if (
                eventLogs[i]["event_type"].includes("problem+block") ||
                eventLogs[i]["event_type"].includes("_problem;_") ||
                submissionEventCollection.includes(eventLogs[i]["event_type"])
              ) {
                let questionId = "";
                if (eventLogs[i]["event_type"].includes("problem+block")) {
                  // questionId = event_type_array[4];
                  questionId = eventLogs[i]["event"].problem_id;
                }
                if (eventLogs[i]["event_type"].includes("_problem;_")) {
                  // questionId = event_type_array[6].replace(/;_/g, '/');
                  questionId = eventLogs[i]["event"].problem_id;
                }
                if (questionId in childParentMap) {
                  sessionId =
                    "quiz_session_" +
                    childParentMap[questionId] +
                    "_" +
                    courseLearnerId;
                  startTime = new Date(eventLogs[i]["event_time"]);
                  endTime = new Date(eventLogs[i]["event_time"]);
                } else {
                  sessionId = "";
                  startTime = null;
                  endTime = null;
                }
              }
            } else {
              endTime = new Date(eventLogs[i]["event_time"]);
            }
          } else {
            let verificationTime = new Date(endTime);
            if (
              eventLogs[i]["event_time"] <=
              verificationTime.setMinutes(verificationTime.getMinutes() + 30)
            ) {
              endTime = new Date(eventLogs[i]["event_time"]);
            }
            if (sessionId in quizSessions) {
              quizSessions[sessionId]["time_array"].push({
                start_time: startTime,
                end_time: endTime,
              });
            } else {
              quizSessions[sessionId] = {
                course_learner_id: courseLearnerId,
                time_array: [{ start_time: startTime, end_time: endTime }],
              };
            }
            finalTime = new Date(eventLogs[i]["event_time"]);
            sessionId = "";
            startTime = null;
            endTime = null;
          }
        }
      }

      if (finalTime != null) {
        let newLogs = [];
        for (let log of eventLogs) {
          if (log["event_time"] >= finalTime) {
            newLogs.push(log);
          }
        }
        updatedLearnerAllEventLogs[courseLearnerId] = newLogs;
      }
    }
    for (let sessionId in quizSessions) {
      if (!Object.prototype.hasOwnProperty.call(quizSessions, sessionId)) {
        continue;
      }
      if (Object.keys(quizSessions[sessionId]["time_array"]).length > 1) {
        let startTime = null;
        let endTime = null;
        let updatedTimeArray = [];
        for (
          let i = 0;
          i < Object.keys(quizSessions[sessionId]["time_array"]).length;
          i++
        ) {
          let verificationTime = new Date(endTime);
          if (i === 0) {
            startTime = new Date(
              quizSessions[sessionId]["time_array"][i]["start_time"],
            );
            endTime = new Date(
              quizSessions[sessionId]["time_array"][i]["end_time"],
            );
          } else if (
            new Date(quizSessions[sessionId]["time_array"][i]["start_time"]) >
            verificationTime.setMinutes(verificationTime.getMinutes() + 30)
          ) {
            updatedTimeArray.push({
              start_time: startTime,
              end_time: endTime,
            });
            startTime = new Date(
              quizSessions[sessionId]["time_array"][i]["start_time"],
            );
            endTime = new Date(
              quizSessions[sessionId]["time_array"][i]["end_time"],
            );
            if (
              i ===
              Object.keys(quizSessions[sessionId]["time_array"]).length - 1
            ) {
              updatedTimeArray.push({
                start_time: startTime,
                end_time: endTime,
              });
            }
          } else {
            endTime = new Date(
              quizSessions[sessionId]["time_array"][i]["end_time"],
            );
            if (
              i ===
              Object.keys(quizSessions[sessionId]["time_array"]).length - 1
            ) {
              updatedTimeArray.push({
                start_time: startTime,
                end_time: endTime,
              });
            }
          }
        }
        quizSessions[sessionId]["time_array"] = updatedTimeArray;
      }
    }

    let quizSessionRecord = [];
    for (let sessionId in quizSessions) {
      if (!Object.prototype.hasOwnProperty.call(quizSessions, sessionId)) {
        continue;
      }
      let courseLearnerId = quizSessions[sessionId]["course_learner_id"];
      for (
        let i = 0;
        i < Object.keys(quizSessions[sessionId]["time_array"]).length;
        i++
      ) {
        let startTime = new Date(
          quizSessions[sessionId]["time_array"][i]["start_time"],
        );
        let endTime = new Date(
          quizSessions[sessionId]["time_array"][i]["end_time"],
        );
        if (startTime < endTime) {
          let duration = (endTime - startTime) / 1000;
          let finalSessionId =
            sessionId + "_" + startTime.getTime() + "_" + endTime.getTime();
          if (duration > 5) {
            let array = [
              finalSessionId,
              courseLearnerId,
              startTime,
              endTime,
              duration,
            ];
            quizSessionRecord.push(array);
          }
        }
      }
    }
    if (quizSessionRecord.length > 0) {
      let data = [];
      for (let array of quizSessionRecord) {
        let sessionId = array[0];
        let courseLearnerId = array[1];
        let startTime = array[2];
        let endTime = array[3];
        let duration = processNull(array[4]);
        let values = {
          session_id: sessionId,
          course_learner_id: courseLearnerId,
          start_time: startTime,
          end_time: endTime,
          duration: duration,
        };
        data.push(values);
      }
      await mongoInsert("quiz_sessions", data);
    }
  }
}

/**
 * Processing to find all important information from an Open Response Assessment record
 * @param {Object} fullEvent Complete record of an ORA evebt
 * @returns {Object} oraInfo
 */
function getORAEventTypeAndElement(fullEvent) {
  let eventType = "",
    element = "",
    meta = false;
  if (fullEvent["event_type"].includes("openassessmentblock")) {
    eventType = fullEvent["event_type"];
    eventType = eventType.slice(eventType.indexOf(".") + 1);
    element = fullEvent["context"]["module"]["usage_key"];
    element = element.slice(element.lastIndexOf("@") + 1);
  }
  if (fullEvent["event_type"].includes("openassessment+block")) {
    eventType = fullEvent["event_type"];
    eventType = eventType.slice(eventType.lastIndexOf("/") + 1);
    element = fullEvent["event_type"];
    element = element.slice(element.lastIndexOf("@") + 1);
    element = element.slice(0, element.indexOf("/"));
    meta = true;
  }
  let oraInfo = {
    event_type: eventType,
    element: element,
    meta: meta,
  };
  return oraInfo;
}

/**
 *
 * @param {string} courseRunName The name of the course run
 * @param {string[]} logFiles The log files to be processed
 */
async function processORASessions(courseRunName, logFiles) {
  let courseMetadataMap = await mongoQuery("metadata", {
    name: courseRunName,
  });
  courseMetadataMap = courseMetadataMap[0]["object"];
  const courseId = courseMetadataMap["course_id"];
  const currentCourseId = courseId.slice(
    courseId.indexOf("+") + 1,
    courseId.lastIndexOf("+") + 7,
  );

  let learnerAllEventLogs = {};
  let updatedLearnerAllEventLogs = {};

  for (const logFile of logFiles) {
    learnerAllEventLogs = updatedLearnerAllEventLogs;
    updatedLearnerAllEventLogs = {};
    let oraEvents = {};
    let oraSessionsRecord = [];
    let courseLearnerIdSet = new Set();
    for (const courseLearnerId in learnerAllEventLogs) {
      courseLearnerIdSet.add(courseLearnerId);
    }
    for await (const line of readLines(logFile)) {
      if (line.length < 10 || !line.includes(currentCourseId)) {
        continue;
      }
      const jsonObject = JSON.parse(line);
      if (!("user_id" in jsonObject["context"])) {
        continue;
      }
      const globalLearnerId = jsonObject["context"]["user_id"],
        event_type = jsonObject["event_type"];
      if (globalLearnerId === "") {
        continue;
      }
      const courseId = jsonObject["context"]["course_id"],
        course_learner_id = courseId + "_" + globalLearnerId,
        event_time = new Date(jsonObject["time"]);

      let event = {
        event_time: event_time,
        event_type: event_type,
        full_event: jsonObject,
      };
      if (course_learner_id in learnerAllEventLogs) {
        learnerAllEventLogs[course_learner_id].push(event);
      } else {
        learnerAllEventLogs[course_learner_id] = [event];
      }
    }

    for (const courseLearnerId in learnerAllEventLogs) {
      let eventLogs = learnerAllEventLogs[courseLearnerId];
      eventLogs.sort(function (a, b) {
        return b.event_type - a.event_type;
      });
      eventLogs.sort(function (a, b) {
        return new Date(a.event_time) - new Date(b.event_time);
      });
      let sessionId = "",
        startTime = null,
        endTime = null,
        currentStatus = "",
        currentElement = "",
        saveCount = 0,
        peerAssessmentCount = 0,
        selfAssessed = false,
        submitted = false,
        eventType = "",
        meta = false;

      let learnerOraEvents = [];
      for (const i in eventLogs) {
        if (sessionId === "") {
          if (eventLogs[i]["event_type"].includes("openassessment")) {
            startTime = new Date(eventLogs[i]["event_time"]);
            endTime = new Date(eventLogs[i]["event_time"]);
            let eventDetails = getORAEventTypeAndElement(
              eventLogs[i]["full_event"],
            );
            currentElement = eventDetails.element;
            eventType = eventDetails.eventType;
            meta = eventDetails.meta;
            sessionId = "ora_session_" + courseLearnerId + "_" + currentElement;

            if (meta === true && currentStatus === "") {
              currentStatus = "viewed";
            } else if (eventType === "save_submission") {
              saveCount++;
              currentStatus = "saved";
            } else if (eventType === "create_submission") {
              submitted = true;
              currentStatus = "submitted";
            } else if (eventType === "self_assess") {
              selfAssessed = true;
              currentStatus = "selfAssessed";
            } else if (eventType === "peer_assess" && meta === false) {
              peerAssessmentCount++;
              currentStatus = "assessingPeers";
            }

            learnerOraEvents.push(
              "Empty id: " + currentStatus + "_" + meta + "_" + eventType,
            );
          }
        } else {
          if (eventLogs[i]["event_type"].includes("openassessment")) {
            let eventDetails = getORAEventTypeAndElement(
              eventLogs[i]["full_event"],
            );
            currentElement = eventDetails.element;
            eventType = eventDetails.eventType;
            meta = eventDetails.meta;

            let verificationTime = new Date(endTime);
            if (
              new Date(eventLogs[i]["event_time"]) >
              verificationTime.setMinutes(verificationTime.getMinutes() + 30)
            ) {
              sessionId =
                sessionId + "_" + startTime.getTime() + "_" + endTime.getTime();

              const duration = (endTime - startTime) / 1000;

              if (duration > 5) {
                let array = [
                  sessionId,
                  courseLearnerId,
                  saveCount,
                  peerAssessmentCount,
                  submitted,
                  selfAssessed,
                  startTime,
                  endTime,
                  duration,
                  currentElement,
                ];
                oraSessionsRecord.push(array);
              }
              learnerOraEvents.push(
                "Over 30 min, to store: " +
                  currentStatus +
                  "_" +
                  meta +
                  "_" +
                  eventType,
              );

              sessionId =
                "ora_session_" + courseLearnerId + "_" + currentElement;
              startTime = new Date(eventLogs[i]["event_time"]);
              endTime = new Date(eventLogs[i]["event_time"]);
              if (meta === true && currentStatus === "") {
                currentStatus = "viewed";
              } else if (eventType === "save_submission") {
                saveCount++;
                currentStatus = "saved";
              } else if (eventType === "create_submission") {
                submitted = true;
                currentStatus = "submitted";
              } else if (eventType === "self_assess") {
                selfAssessed = true;
                currentStatus = "selfAssessed";
              } else if (eventType === "peer_assess" && meta === false) {
                peerAssessmentCount++;
                currentStatus = "assessingPeers";
              }
              learnerOraEvents.push(
                "Over 30 min, new: " +
                  currentStatus +
                  "_" +
                  meta +
                  "_" +
                  eventType,
              );
            } else {
              endTime = new Date(eventLogs[i]["event_time"]);
              if (meta === true && currentStatus === "") {
                currentStatus = "viewed";
              } else if (eventType === "save_submission") {
                saveCount++;
                currentStatus = "saved";
              } else if (eventType === "create_submission") {
                submitted = true;
                currentStatus = "submitted";
              } else if (eventType === "self_assess") {
                selfAssessed = true;
                currentStatus = "selfAssessed";
              } else if (eventType === "peer_assess" && meta === false) {
                peerAssessmentCount++;
                currentStatus = "assessingPeers";
              }
              learnerOraEvents.push(
                "Under 30 min: " + currentStatus + "_" + meta + "_" + eventType,
              );
            }
          } else {
            learnerOraEvents.push(
              "Not ORA, to store: " +
                currentStatus +
                "_" +
                meta +
                "_" +
                eventType,
            );

            let verificationTime = new Date(endTime);
            if (
              eventLogs[i]["event_time"] <=
              verificationTime.setMinutes(verificationTime.getMinutes() + 30)
            ) {
              endTime = new Date(eventLogs[i]["event_time"]);
            }
            sessionId =
              sessionId + "_" + startTime.getTime() + "_" + endTime.getTime();
            const duration = (endTime - startTime) / 1000;

            if (duration > 5) {
              let array = [
                sessionId,
                courseLearnerId,
                saveCount,
                peerAssessmentCount,
                submitted,
                selfAssessed,
                startTime,
                endTime,
                duration,
                currentElement,
              ];
              oraSessionsRecord.push(array);
            }

            sessionId = "";
            startTime = null;
            endTime = null;
            meta = false;
            eventType = "";
            saveCount = 0;
            selfAssessed = false;
            peerAssessmentCount = 0;
          }
        }
      }
      if (learnerOraEvents.length > 0) {
        oraEvents[courseLearnerId] = learnerOraEvents;
      }
    }
    if (oraSessionsRecord.length > 0) {
      let data = [];
      for (let array of oraSessionsRecord) {
        let sessionId = array[0];
        const courseLearnerId = array[1],
          saveCount = processNull(array[2]),
          peerAssessmentCount = array[3],
          submitted = array[4],
          selfAssessed = array[5],
          startTime = array[6],
          endTime = array[7],
          duration = processNull(array[8]),
          elementId = array[9];
        let values = {
          session_id: sessionId,
          course_learner_id: courseLearnerId,
          times_save: saveCount,
          times_peer_assess: peerAssessmentCount,
          submitted: submitted,
          self_assessed: selfAssessed,
          start_time: startTime,
          end_time: endTime,
          duration: duration,
          assessment_id: elementId,
        };
        data.push(values);
      }
      mongoInsert("ora_sessions", data);
    }
  }
}

/**
 * This function will read the records in the logfile and extract all interactions from students with the forums,
 * process their values, like duration and times they search, to finally store them in the database
 * @param {string} courseRunName - The name of the course run
 * @param {string[]} logFiles - The list of log files
 */
async function processForumSessions(courseRunName, logFiles) {
  let courseMetadataMap = await mongoQuery("metadata", {
    name: courseRunName,
  });
  courseMetadataMap = courseMetadataMap[0]["object"];

  const courseId = courseMetadataMap["course_id"];
  const currentCourseId = courseId.slice(
    courseId.indexOf("+") + 1,
    courseId.lastIndexOf("+") + 7
  );

  let startDate = new Date(courseMetadataMap["start_date"]);
  let currentDate = new Date(startDate);
  let forumEventTypes = [
    "edx.forum.comment.created",
    "edx.forum.comment.created",
    "edx.forum.response.created",
    "edx.forum.response.voted",
    "edx.forum.thread.created",
    "edx.forum.thread.voted",
    "edx.forum.searched",
  ];

  let learnerAllEventLogs = {};
  let updatedAllEventLogs = {};
  let forumSessionRecord = [];

  for (let logFile of logFiles) {
    learnerAllEventLogs = {};
    learnerAllEventLogs = updatedAllEventLogs;
    updatedAllEventLogs = {};
    let courseLearnerIdSet = new Set();
    if (learnerAllEventLogs.length > 0) {
      for (let courseLearnerId of learnerAllEventLogs) {
        courseLearnerIdSet.add(courseLearnerId);
      }
    }

    for await (const line of readLines(logFile)) {
      if (line.length < 10 || !line.includes(currentCourseId)) {
        continue;
      }
      let jsonObject = JSON.parse(line);
      if (
        !("user_id" in jsonObject["context"]) ||
        jsonObject["context"]["user_id"] === ""
      ) {
        continue;
      }
      const globalLearnerId = jsonObject["context"]["user_id"];
      let eventType = jsonObject["event_type"];
      if (
        eventType.includes("/discussion/") ||
        forumEventTypes.includes(eventType)
      ) {
        if (eventType !== "edx.forum.searched") {
          eventType = "forum_activity";
        }
      }
      if (globalLearnerId !== "") {
        const courseId = jsonObject["context"]["course_id"],
          courseLearnerId = courseId + "_" + globalLearnerId,
          eventTime = new Date(jsonObject["time"]);

        let eventPage = "";
        if ("page" in jsonObject) {
          if (jsonObject["page"] === null) {
            eventPage = "";
          } else {
            eventPage = jsonObject["page"];
          }
        }
        let eventPath = "";
        if ("path" in jsonObject) {
          eventPath = jsonObject["path"];
        }
        let eventReferer = "";
        if ("referer" in jsonObject) {
          eventReferer = jsonObject["referer"];
        }
        if (courseLearnerIdSet.has(courseLearnerId)) {
          learnerAllEventLogs[courseLearnerId].push({
            event_time: eventTime,
            event_type: eventType,
            page: eventPage,
            path: eventPath,
            referer: eventReferer,
          });
        } else {
          learnerAllEventLogs[courseLearnerId] = [
            {
              event_time: eventTime,
              event_type: eventType,
              page: eventPage,
              path: eventPath,
              referer: eventReferer,
            },
          ];
          courseLearnerIdSet.add(courseLearnerId);
        }
      }
    }

    for (const learner in learnerAllEventLogs) {
      const courseLearnerId = learner,
        courseId = courseLearnerId.split("_")[0];
      let eventLogs = learnerAllEventLogs[learner];
      eventLogs.sort(function (a, b) {
        return b.event_type - a.event_type;
      });
      eventLogs.sort(function (a, b) {
        return new Date(a.event_time) - new Date(b.event_time);
      });
      let sessionId = "",
        startTime = null,
        endTime = null,
        finalTime = null,
        timesSearch = 0,
        sessionRelatedElementPrevious = "",
        sessionRelatedElementCurrent = "";
      for (let i in eventLogs) {
        let relatedElementCurrent = courseElementsFinder(
          eventLogs[i],
          courseId
        );
        if (sessionId === "") {
          if (
            ["forum_activity", "edx.forum.searched"].includes(
              eventLogs[i]["event_type"]
            )
          ) {
            sessionId = "forum_session_" + courseLearnerId;
            startTime = new Date(eventLogs[i]["event_time"]);
            endTime = new Date(eventLogs[i]["event_time"]);
            if (eventLogs[i]["event_type"] === "edx.forum.searched") {
              timesSearch++;
            }
            sessionRelatedElementCurrent = relatedElementCurrent;
          }
        } else {
          if (
            ["forum_activity", "edx.forum.searched"].includes(
              eventLogs[i]["event_type"]
            )
          ) {
            let verification_time = new Date(endTime);
            if (
              new Date(eventLogs[i]["event_time"]) >
              verification_time.setMinutes(verification_time.getMinutes() + 30)
            ) {
              sessionId =
                sessionId + "_" + startTime.getTime() + "_" + endTime.getTime();
              // session_id = session_id + '_' + start_time + '_' + end_time;
              const duration = (endTime - startTime) / 1000;

              if (duration > 5) {
                let relatedElementId = "";
                if (sessionRelatedElementCurrent !== "") {
                  relatedElementId = sessionRelatedElementCurrent;
                } else {
                  relatedElementId = sessionRelatedElementPrevious;
                }
                let array = [
                  sessionId,
                  courseLearnerId,
                  timesSearch,
                  startTime,
                  endTime,
                  duration,
                  relatedElementId,
                ];
                forumSessionRecord.push(array);
              }
              finalTime = new Date(eventLogs[i]["event_time"]);

              sessionId = "forum_session_" + courseLearnerId;
              startTime = new Date(eventLogs[i]["event_time"]);
              endTime = new Date(eventLogs[i]["event_time"]);
              if (eventLogs[i]["event_type"] === "edx.forum.searched") {
                timesSearch = 1;
              }
              sessionRelatedElementCurrent = relatedElementCurrent;
            } else {
              endTime = new Date(eventLogs[i]["event_time"]);
              if (eventLogs[i]["event_type"] === "edx.forum.searched") {
                timesSearch++;
              }
              if (sessionRelatedElementCurrent === "") {
                sessionRelatedElementCurrent = relatedElementCurrent;
              }
            }
          } else {
            let verificationTime = new Date(endTime);
            if (
              new Date(eventLogs[i]["event_time"]) <=
              verificationTime.setMinutes(verificationTime.getMinutes() + 30)
            ) {
              endTime = new Date(eventLogs[i]["event_time"]);
            }
            sessionId =
              sessionId + "_" + startTime.getTime() + "_" + endTime.getTime();
            const duration = (endTime - startTime) / 1000;

            if (duration > 5) {
              let relElementId = "";
              if (sessionRelatedElementCurrent !== "") {
                relElementId = sessionRelatedElementCurrent;
              } else {
                relElementId = sessionRelatedElementPrevious;
              }
              let array = [
                sessionId,
                courseLearnerId,
                timesSearch,
                startTime,
                endTime,
                duration,
                relElementId,
              ];
              forumSessionRecord.push(array);
            }
            finalTime = new Date(eventLogs[i]["event_time"]);
            sessionId = "";
            startTime = null;
            endTime = null;
            timesSearch = 0;
          }
          if (relatedElementCurrent !== "") {
            sessionRelatedElementPrevious = relatedElementCurrent;
          }
        }
      }
      if (finalTime != null) {
        let newLogs = [];
        for (let log of eventLogs) {
          if (new Date(log["event_time"]) >= finalTime) {
            newLogs.push(log);
          }
        }
        updatedAllEventLogs[courseLearnerId] = newLogs;
      }
    }

    currentDate = getNextDay(currentDate);
  }

  if (forumSessionRecord.length > 0) {
    let data = [];
    for (let array of forumSessionRecord) {
      let sessionId = array[0];
      const courseLearnerId = array[1],
        timesSearch = processNull(array[2]),
        startTiem = array[3],
        endTime = array[4],
        duration = processNull(array[5]),
        relElementId = array[6];
      let values = {
        session_id: sessionId,
        course_learner_id: courseLearnerId,
        times_search: timesSearch,
        start_time: startTiem,
        end_time: endTime,
        duration: duration,
        relevent_element_id: relElementId,
      };
      data.push(values);
    }

    if (data.length > 0) {
      mongoInsert("forum_sessions", data);
    }
  }
}

module.exports = {
  processGeneralSessions,
  processVideoInteractionSessions,
  processAssessmentsSubmissions,
  processQuizSessions,
  processORASessions,
  processForumSessions,
};
