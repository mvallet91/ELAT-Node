const config = require("./config");
const fs = require("fs").promises;
const path = require("path");
const getDayDiff = require("./helpers").getDayDiff;
const compareDatetime = require("./helpers").compareDatetime;
const learnerSegmentation = require("./helpers").learnerSegmentation;
const processNull = require("./helpers").processNull;
const cleanUnicode = require("./helpers").cleanUnicode;
const escapeString = require("./helpers").escapeString;
const mongoInsert = require("./databaseHelpers").mongoInsert;

/**
 * Function that reads the metadata files for a course run and processes them
 * @param {string} coursePath Path to the directory containing the metadata files
 * @param {string} courseRunName Name of the course run
 */
async function readMetadataFiles(coursePath, courseRunName) {
  let processedFiles = [];
  const sqlType = "sql",
    jsonType = "json",
    mongoType = "mongo";
  const files = await fs.readdir(coursePath);

  for (let file of files) {
    const filePath = path.join(coursePath, file);
    if (file.includes("zip")) {
      console.error("Metadata files have to be unzipped!");
      continue;
    }
    if (
      file.includes(sqlType) ||
      file.includes(jsonType) ||
      file.includes(mongoType)
    ) {
      const content = await fs.readFile(filePath, "utf8");
      processedFiles.push({ key: file, value: content });
    }
  }
  await processMetadataFiles(processedFiles, courseRunName);
}

/**
 * Handles all the functions to process the metadata files, starting from the course structure, then the construction of a dictionary of files by their name, to then call the appropriate function to process each one by the data they contain, and finally store the data in the corresponding tables of the database
 * @param {FileList} files Array of all files
 */
async function processMetadataFiles(files, courseRunName) {
  let courseMetadataMap = ExtractCourseInformation(files);
  if (Object.keys(courseMetadataMap).length < 1) {
    console.warn("Course structure file is missing");
  } else {
    let segmentationType = { type: config.segmentationType };

    await mongoInsert("webdata", [
      { name: "segmentation", object: segmentationType },
    ]);

    let courseRecord = [],
      courseId = courseMetadataMap.course_id,
      fileMap = {};
    courseRecord.push([
      courseMetadataMap["course_id"],
      courseMetadataMap["course_name"],
      courseMetadataMap["start_time"],
      courseMetadataMap["end_time"],
    ]);

    let shortId = courseId.slice(courseId.indexOf(":") + 1);
    shortId = shortId.replace("+", "-").replace("+", "-");
    for (let file of files) {
      let fileName = file["key"];
      let shortName = fileName.slice(
        fileName.indexOf(shortId) + shortId.length + 1,
        fileName.indexOf(".")
      );
      fileMap[shortName] = file["value"];
    }

    let requiredFiles = [
      "student_courseenrollment-prod-analytics",
      "certificates_generatedcertificate-prod-analytics",
      "auth_userprofile-prod-analytics",
      "prod",
    ];

    if (
      !requiredFiles.every(function (x) {
        return x in fileMap;
      })
    ) {
      console.warn("Some files are missing");
    } else {
      let courseElementRecord = [];
      for (let elementId in courseMetadataMap["element_time_map"]) {
        let element_start_time = new Date(
          courseMetadataMap["element_time_map"][elementId]
        );
        let week =
          getDayDiff(courseMetadataMap["start_time"], element_start_time) / 7 +
          1;
        let array = [
          elementId,
          courseMetadataMap["element_type_map"][elementId],
          week,
          courseMetadataMap["course_id"],
        ];
        courseElementRecord.push(array);
      }

      let enrollmentValues = processEnrollment(
        courseId,
        fileMap["student_courseenrollment-prod-analytics"],
        courseMetadataMap
      );

      let certificateValues = processCertificates(
        fileMap["certificates_generatedcertificate-prod-analytics"],
        enrollmentValues,
        courseMetadataMap
      );

      let learnerAuthMap = {};
      if ("auth_user-prod-analytics" in fileMap) {
        let learnerAuthMap = processAuthMap(
          fileMap["auth_user-prod-analytics"],
          enrollmentValues
        );
      }

      let groupMap = {};
      if ("course_groups_cohortmembership-prod-analytics" in fileMap) {
        groupMap = processGroups(
          courseId,
          fileMap["course_groups_cohortmembership-prod-analytics"],
          enrollmentValues
        );
      }

      let demographicValues = processDemographics(
        courseId,
        fileMap["auth_userprofile-prod-analytics"],
        enrollmentValues,
        learnerAuthMap
      );

      let forumInteractionRecords = processForumPostingInteraction(
        courseId,
        fileMap["prod"],
        courseMetadataMap
      );

      let rows = [];
      let course_id = courseMetadataMap["course_id"];
      let course_name = courseMetadataMap["course_name"];
      let start_time = courseMetadataMap["start_time"];
      let end_time = courseMetadataMap["end_time"];
      let values = {
        course_id: course_id,
        course_name: course_name,
        start_time: start_time,
        end_time: end_time,
      };
      rows.push(values);
      await mongoInsert("courses", rows);

      if (courseElementRecord.length > 0) {
        let data = [];
        for (let array of courseElementRecord) {
          let element_id = array[0];
          let element_type = array[1];
          let week = processNull(array[2]);
          let course_id = array[3];
          let values = {
            element_id: element_id,
            element_type: element_type,
            week: week,
            course_id: course_id,
          };
          data.push(values);
        }
        await mongoInsert("course_elements", data);
      }

      if (enrollmentValues.learnerIndexRecord.length > 0) {
        let data = [];
        for (let array of enrollmentValues.learnerIndexRecord) {
          let globalLearnerId = array[0];
          let course_id = array[1];
          let courseLearnerId = array[2];
          let values = {
            global_Learner_id: globalLearnerId.toString(),
            course_id: course_id,
            course_learner_id: courseLearnerId,
          };
          data.push(values);
        }
        await mongoInsert("learner_index", data);
      }

      if (certificateValues.courseLearnerRecord.length > 0) {
        let data = [];
        for (let array of certificateValues.courseLearnerRecord) {
          let course_learner_id = array[0],
            final_grade = parseFloat(processNull(array[1])),
            enrollment_mode = array[2],
            certificate_status = array[3],
            register_time = new Date(processNull(array[4])),
            segment = array[5],
            values = {
              course_learner_id: course_learner_id,
              final_grade: final_grade,
              enrollment_mode: enrollment_mode,
              certificate_status: certificate_status,
              register_time: register_time,
              group_type: "",
              group_name: "",
              segment: segment,
            };
          if (course_learner_id in groupMap) {
            values["group_type"] = groupMap[course_learner_id][0];
            values["group_name"] = groupMap[course_learner_id][1];
          }
          data.push(values);
        }
        await mongoInsert("course_learner", data);
      }

      if (demographicValues.learnerDemographicRecord.length > 0) {
        let data = [];
        for (let array of demographicValues.learnerDemographicRecord) {
          let course_learner_id = processNull(array[0]),
            gender = array[1],
            year_of_birth = parseInt(processNull(array[2])),
            level_of_education = array[3],
            country = array[4],
            email = array[5],
            segment = array[6];
          email = email.replace(/"/g, "");
          let values = {
            course_learner_id: course_learner_id,
            gender: gender,
            year_of_birth: year_of_birth,
            level_of_education: level_of_education,
            country: country,
            email: email,
            segment: segment,
          };
          data.push(values);
        }
        await mongoInsert("learner_demographic", data);
      }

      if (forumInteractionRecords.length > 0) {
        let data = [];
        for (let array of forumInteractionRecords) {
          let post_id = processNull(array[0]),
            course_learner_id = array[1],
            post_type = array[2],
            post_title = cleanUnicode(array[3]),
            post_content = cleanUnicode(array[4]),
            post_timestamp = array[5],
            post_parent_id = array[6],
            post_thread_id = array[7];
          let values = {
            post_id: post_id,
            course_learner_id: course_learner_id,
            post_type: post_type,
            post_title: post_title,
            post_content: post_content,
            post_timestamp: post_timestamp,
            post_parent_id: post_parent_id,
            post_thread_id: post_thread_id,
          };
          data.push(values);
        }
        await mongoInsert("forum_interaction", data);
      }

      let quizQuestionMap = courseMetadataMap["quiz_question_map"],
        blockTypeMap = courseMetadataMap["block_type_map"],
        elementTimeMapDue = courseMetadataMap["element_time_map_due"],
        quizData = [];
      for (let questionId in quizQuestionMap) {
        let questionDue = "",
          questionWeight = quizQuestionMap[questionId],
          quizQuestionParent =
            courseMetadataMap["child_parent_map"][questionId];
        if (questionDue === "" && quizQuestionParent in elementTimeMapDue) {
          questionDue = elementTimeMapDue[quizQuestionParent];
        }
        while (!(quizQuestionParent in blockTypeMap)) {
          quizQuestionParent =
            courseMetadataMap["child_parent_map"][quizQuestionParent];
          if (questionDue === "" && quizQuestionParent in elementTimeMapDue) {
            questionDue = elementTimeMapDue[quizQuestionParent];
          }
        }
        let quizQuestionType = blockTypeMap[quizQuestionParent];
        questionDue = processNull(questionDue);
        let values = {
          question_id: questionId,
          question_type: quizQuestionType,
          question_weight: questionWeight,
          question_due: new Date(questionDue),
        };
        quizData.push(values);
      }
      await mongoInsert("quiz_questions", quizData);
      await mongoInsert("metadata", [
        { name: courseRunName, object: courseMetadataMap },
      ]);
    }
  }
}

/**
 * Processing of the course structure file, this function will handle all the course elements to generate the metadata object
 * @param {FileList} files Array of all files
 * @returns {object} courseMetadataMap Object with the course metadata information
 */
function ExtractCourseInformation(files) {
  let courseMetadataMap = {};
  let i = 0;
  for (let file of files) {
    i++;
    let fileName = file["key"];
    if (!fileName.includes("course_structure")) {
      if (i === files.length) {
        console.warn("Course structure file is missing!");
        return courseMetadataMap;
      }
    } else {
      let child_parent_map = {};
      let element_time_map = {};

      let element_time_map_due = {};
      let element_type_map = {};
      let element_without_time = [];

      let quiz_question_map = {};
      let block_type_map = {};

      let order_map = {};
      let element_name_map = {};

      let jsonObject = JSON.parse(file["value"]);
      for (let record in jsonObject) {
        if (jsonObject[record]["category"] === "course") {
          let course_id = record;
          if (course_id.startsWith("block-")) {
            course_id = course_id.replace("block-", "course-");
            course_id = course_id.replace("+type@course+block@course", "");
          }
          if (course_id.startsWith("i4x://")) {
            course_id = course_id.replace("i4x://", "");
            course_id = course_id.replace("course/", "");
          }
          courseMetadataMap["course_id"] = course_id;
          courseMetadataMap["course_name"] =
            jsonObject[record]["metadata"]["display_name"];

          courseMetadataMap["start_date"] = new Date(
            jsonObject[record]["metadata"]["start"]
          );
          courseMetadataMap["end_date"] = new Date(
            jsonObject[record]["metadata"]["end"]
          );

          courseMetadataMap["start_time"] = new Date(
            courseMetadataMap["start_date"]
          );
          courseMetadataMap["end_time"] = new Date(
            courseMetadataMap["end_date"]
          );

          let elementPosition = 0;

          for (let child of jsonObject[record]["children"]) {
            elementPosition++;
            child_parent_map[child] = record;
            order_map[child] = elementPosition;
          }
          element_time_map[record] = new Date(
            jsonObject[record]["metadata"]["start"]
          );
          element_type_map[record] = jsonObject[record]["category"];
        } else {
          let element_id = record;
          element_name_map[element_id] =
            jsonObject[element_id]["metadata"]["display_name"];
          let elementPosition = 0;

          for (let child of jsonObject[element_id]["children"]) {
            elementPosition++;
            child_parent_map[child] = element_id;
            order_map[child] = elementPosition;
          }

          if ("start" in jsonObject[element_id]["metadata"]) {
            element_time_map[element_id] = new Date(
              jsonObject[element_id]["metadata"]["start"]
            );
          } else {
            element_without_time.push(element_id);
          }

          if ("due" in jsonObject[element_id]["metadata"]) {
            element_time_map_due[element_id] = new Date(
              jsonObject[element_id]["metadata"]["due"]
            );
          }

          element_type_map[element_id] = jsonObject[element_id]["category"];
          if (jsonObject[element_id]["category"] === "problem") {
            if ("weight" in jsonObject[element_id]["metadata"]) {
              quiz_question_map[element_id] =
                jsonObject[element_id]["metadata"]["weight"];
            } else {
              quiz_question_map[element_id] = 1.0;
            }
          }
          if (jsonObject[element_id]["category"] === "sequential") {
            if ("display_name" in jsonObject[element_id]["metadata"]) {
              block_type_map[element_id] =
                jsonObject[element_id]["metadata"]["display_name"];
            }
          }
        }
      }
      for (let element_id of element_without_time) {
        let element_start_time = "";
        while (element_start_time === "") {
          let element_parent = child_parent_map[element_id];
          while (
            !Object.prototype.hasOwnProperty.call(
              element_time_map,
              element_parent
            )
          ) {
            element_parent = child_parent_map[element_parent];
          }
          element_start_time = element_time_map[element_parent];
        }
        element_time_map[element_id] = element_start_time;
      }
      courseMetadataMap["element_time_map"] = element_time_map;
      courseMetadataMap["element_time_map_due"] = element_time_map_due;
      courseMetadataMap["element_type_map"] = element_type_map;
      courseMetadataMap["quiz_question_map"] = quiz_question_map;
      courseMetadataMap["child_parent_map"] = child_parent_map;
      courseMetadataMap["block_type_map"] = block_type_map;
      courseMetadataMap["order_map"] = order_map;
      courseMetadataMap["element_name_map"] = element_name_map;

      return courseMetadataMap;
    }
  }
}

/**
 * Processing of the student enrollment file, to create the course_learner table with the learner information
 * @param {string} courseId Current course id
 * @param {string} inputFile String with contents of the enrollment file
 * @param {object} courseMetadataMap Object with the course metadata information
 * @returns {{enrolledLearnerSet: *, learnerIndexRecord: *, learnerModeMap: *, learnerEnrollmentTimeMap: *, courseLearnerMap: *}}
 */
function processEnrollment(courseId, inputFile, courseMetadataMap) {
  let courseLearnerMap = {};
  let learnerEnrollmentTimeMap = {};
  let enrolledLearnerSet = new Set();
  let learnerIndexRecord = [];
  let learnerModeMap = {};
  let learnerSegmentMap = {};

  let lines = inputFile.split("\n");
  for (let line of lines.slice(1)) {
    let record = line.split("\t");
    if (record.length < 2) {
      continue;
    }
    let active = record[4];
    if (active === "0") {
      continue;
    }
    let globalLearnerId = record[0],
      time = new Date(record[2]),
      courseLearnerId = courseId + "_" + globalLearnerId,
      mode = record[4];
    if (compareDatetime(courseMetadataMap["end_time"], time) === 1) {
      enrolledLearnerSet.add(globalLearnerId);
      let array = [globalLearnerId, courseId, courseLearnerId];
      learnerIndexRecord.push(array);
      courseLearnerMap[globalLearnerId] = courseLearnerId;
      learnerEnrollmentTimeMap[globalLearnerId] = time;
      learnerModeMap[globalLearnerId] = mode;
      learnerSegmentMap[globalLearnerId] = learnerSegmentation(globalLearnerId);
    }
  }
  return {
    courseLearnerMap: courseLearnerMap,
    learnerEnrollmentTimeMap: learnerEnrollmentTimeMap,
    enrolledLearnerSet: enrolledLearnerSet,
    learnerIndexRecord: learnerIndexRecord,
    learnerModeMap: learnerModeMap,
    learnerSegmentMap: learnerSegmentMap,
  };
}

/**
 * Processing of the certificates file, to handle the certificate status of the learners
 * @param {string} inputFile String with contents of the certificates file
 * @param {object} enrollmentValues Object with the enrollment values returned by the processEnrollment function
 * @param {object} courseMetadataMap Object with the course metadata information
 * @returns {{certifiedLearners: *, courseLearnerRecord: *, uncertifiedLearners: *}}
 */
function processCertificates(inputFile, enrollmentValues, courseMetadataMap) {
  let uncertifiedLearners = 0,
    certifiedLearners = 0,
    courseLearnerRecord = [];

  let radioValue = config.metaOptions;
  if (radioValue === undefined) {
    radioValue = "allStudents";
  }

  let certificateMap = {};

  for (let line of inputFile.split("\n")) {
    let record = line.split("\t");
    if (record.length < 7) {
      continue;
    }
    let globalLearnerId = record[0],
      final_grade = record[1],
      certificate_status = record[3];
    if (globalLearnerId in enrollmentValues.courseLearnerMap) {
      certificateMap[globalLearnerId] = {
        final_grade: final_grade,
        certificate_status: certificate_status,
      };
    }
  }

  if (radioValue) {
    if (radioValue === "completed") {
      for (let globalLearnerId in certificateMap) {
        if (
          certificateMap[globalLearnerId]["certificate_status"] ===
          "downloadable"
        ) {
          let course_learner_id =
              enrollmentValues.courseLearnerMap[globalLearnerId],
            final_grade = certificateMap[globalLearnerId]["final_grade"],
            enrollment_mode = enrollmentValues.learnerModeMap[globalLearnerId],
            certificate_status =
              certificateMap[globalLearnerId]["certificate_status"],
            register_time =
              enrollmentValues.learnerEnrollmentTimeMap[globalLearnerId],
            segment = enrollmentValues.learnerSegmentMap[globalLearnerId];
          let array = [
            course_learner_id,
            final_grade,
            enrollment_mode,
            certificate_status,
            register_time,
            segment,
          ];
          courseLearnerRecord.push(array);
          certifiedLearners++;
        } else {
          uncertifiedLearners++;
        }
      }
    } else {
      for (let globalLearnerId in enrollmentValues.courseLearnerMap) {
        let course_learner_id =
            enrollmentValues.courseLearnerMap[globalLearnerId],
          final_grade = null,
          enrollment_mode = enrollmentValues.learnerModeMap[globalLearnerId],
          certificate_status = null,
          register_time =
            enrollmentValues.learnerEnrollmentTimeMap[globalLearnerId],
          segment = enrollmentValues.learnerSegmentMap[globalLearnerId];
        if (globalLearnerId in certificateMap) {
          final_grade = certificateMap[globalLearnerId]["final_grade"];
          certificate_status =
            certificateMap[globalLearnerId]["certificate_status"];
        }
        let array = [
          course_learner_id,
          final_grade,
          enrollment_mode,
          certificate_status,
          register_time,
          segment,
        ];
        if (radioValue === "allStudents") {
          if (certificate_status === "downloadable") {
            certifiedLearners++;
          } else {
            uncertifiedLearners++;
          }
          courseLearnerRecord.push(array);
        } else if (radioValue === "inCourseDates") {
          if (new Date(register_time) <= new Date(courseMetadataMap.end_date)) {
            if (certificate_status === "downloadable") {
              certifiedLearners++;
            } else {
              uncertifiedLearners++;
            }
            courseLearnerRecord.push(array);
          }
        }
      }
    }
    return {
      certifiedLearners: certifiedLearners,
      uncertifiedLearners: uncertifiedLearners,
      courseLearnerRecord: courseLearnerRecord,
    };
  }
}

/**
 * Processing of the auth file, to handle email and check if a user is staff
 * @param {string} inputFile String with contents of the auth file
 * @param {object} enrollmentValues Object with the enrollment values returned by the processEnrollment function
 */
function processAuthMap(inputFile, enrollmentValues) {
  let learnerAuthMap = {};
  for (let line of inputFile.split("\n")) {
    let record = line.split("\t");
    if (enrollmentValues.enrolledLearnerSet.has(record[0])) {
      learnerAuthMap[record[0]] = {
        mail: record[4],
        staff: record[6],
      };
    }
  }
  return learnerAuthMap;
}

/**
 * Processing of group file, to assign cohort or group number to learners
 * @param {string} courseId Current course id
 * @param {string} inputFile String with contents of the group file
 * @param {object} enrollmentValues Object with the enrollment values returned by the processEnrollment function
 */
function processGroups(courseId, inputFile, enrollmentValues) {
  let groupMap = {};
  for (let line of inputFile.split("\n")) {
    let record = line.split("\t");
    if (record.length < 3) {
      continue;
    }
    let globalLearnerId = record[0],
      groupType = record[2],
      groupName = record[3],
      courseLearnerId = courseId + "_" + globalLearnerId;
    if (enrollmentValues.enrolledLearnerSet.has(globalLearnerId)) {
      groupMap[courseLearnerId] = [groupType, groupName];
    }
  }
  return groupMap;
}

/**
 * Processing of user profile file, to handle learner demographics
 * @param {string} courseId Current course id
 * @param {string} inputFile String with contents of the user profile file
 * @param {object} enrollmentValues Object with the enrollment values returned by the processEnrollment function
 * @param learnerAuthMap
 * @returns {{learnerDemographicRecord: *}}
 */
function processDemographics(
  courseId,
  inputFile,
  enrollmentValues,
  learnerAuthMap
) {
  let learnerDemographicRecord = [];
  // hash_id	language	gender	year_of_birth	level_of_education	goals	country
  for (let line of inputFile.split("\n")) {
    let record = line.split("\t");
    if (record.length < 5) {
      continue;
    }
    let globalLearnerId = record[0],
      gender = record[2],
      yearOfBirth = record[3],
      levelOfEducation = record[4],
      country = record[6],
      courseLearnerId = courseId + "_" + globalLearnerId;
    if (enrollmentValues.enrolledLearnerSet.has(globalLearnerId)) {
      let learnerMail = "";
      if (globalLearnerId in learnerAuthMap) {
        learnerMail = learnerAuthMap[globalLearnerId]["mail"];
      }
      let array = [
        courseLearnerId,
        gender,
        yearOfBirth,
        levelOfEducation,
        country,
        learnerMail,
        enrollmentValues.learnerSegmentMap[globalLearnerId],
      ];
      learnerDemographicRecord.push(array);
    }
  }
  return { learnerDemographicRecord: learnerDemographicRecord };
}

/**
 * Processing of prod file, containing forum posts, to handle learners interactions, such as posting or answering in forums
 * @param {string} courseId Current course id
 * @param {string} inputFile String with contents of the forum interaction file
 * @param {object} courseMetadataMap Object with the course metadata information
 * @returns {array} forumInteractionRecords Array with arrays of interaction records
 */
function processForumPostingInteraction(
  courseId,
  inputFile,
  courseMetadataMap
) {
  let forumInteractionRecords = [];
  let lines = inputFile.split("\n");
  for (let line of lines) {
    if (line.length < 9) {
      continue;
    }
    let jsonObject = JSON.parse(line);
    let postId = jsonObject["_id"]["$oid"];
    let courseLearnerId = courseId + "_" + jsonObject["author_id"];

    let postType = jsonObject["_type"];
    if (postType === "CommentThread") {
      postType += "_" + jsonObject["thread_type"];
    }
    if ("parent_id" in jsonObject && jsonObject["parent_id"] !== "") {
      postType = "Comment_Reply";
    }

    let postTitle = "";
    if (Object.prototype.hasOwnProperty.call(jsonObject, "title")) {
      postTitle = '"' + jsonObject["title"] + '"';
    }

    let postContent = '"' + jsonObject["body"] + '"';
    let postTimestamp = new Date(jsonObject["created_at"]);

    let postParentId = "";
    if (Object.prototype.hasOwnProperty.call(jsonObject, "parent_id")) {
      postParentId = jsonObject["parent_id"]["$oid"];
    }

    let postThreadId = "";
    if (Object.prototype.hasOwnProperty.call(jsonObject, "comment_thread_id")) {
      postThreadId = jsonObject["comment_thread_id"]["$oid"];
    }
    let array = [
      postId,
      courseLearnerId,
      postType,
      postTitle,
      escapeString(postContent),
      postTimestamp,
      postParentId,
      postThreadId,
    ];
    if (new Date(postTimestamp) < new Date(courseMetadataMap["end_time"])) {
      forumInteractionRecords.push(array);
    }
  }
  return forumInteractionRecords;
}

module.exports = { readMetadataFiles };
