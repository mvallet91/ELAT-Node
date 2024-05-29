const config = require("./config");

/**
 * Manages a rule to separate students by their id, for example for A/B testing
 * @param {string} learnerId Identifier of the learner
 * @returns {string} segment The segment they belong to
 */
function learnerSegmentation(learnerId, segmentation) {
  if (!segmentation) {
    segmentation = config.segmentationType;
    // let segmentationType = {'type': config.segmentationType};
  }
  let segment = "none";
  if (String(learnerId).includes("_")) {
    learnerId = Number(learnerId.split("_")[1]);
  }
  if (segmentation === "ab") {
    if (learnerId % 2 === 0) {
      segment = "A";
    } else {
      segment = "B";
    }
  }
  return segment;
}

/**
 * Replaces a value in an array by a given index
 * @param {array} array Array to be modified
 * @param {number} index Position of the value to be modified
 * @param {string} value New value
 * @returns {array} replaceInArray Array with the replaced value
 */
function replaceAt(array, index, value) {
  const replaceInArray = array.slice(0);
  replaceInArray[index] = value;
  return replaceInArray;
}

/**
 * Compares two datetime elements and returns positive if the first is after the second, and negative otherwise
 * @param {Date} a_datetime First datetime element
 * @param {Date} b_datetime Second datetime element
 * @returns {number} the result of the comparison
 */
function compareDatetime(a_datetime, b_datetime) {
  a_datetime = new Date(a_datetime);
  b_datetime = new Date(b_datetime);
  if (a_datetime < b_datetime) {
    return -1;
  } else if (a_datetime > b_datetime) {
    return 1;
  } else {
    return 0;
  }
}

/**
 * Null verification to avoid insertion issues
 * @param {string} inputString String to be verified
 * @returns {string|null}
 */
function processNull(inputString) {
  if (typeof inputString === "string") {
    if (inputString.length === 0 || inputString === "NULL") {
      return null;
    } else {
      return inputString;
    }
  } else {
    return inputString;
  }
}

/**
 * Unicode cleaning for forum posts
 * @param {string} text Post to be cleaned
 * @returns {string} text Cleaned text
 */
function cleanUnicode(text) {
  if (typeof text === "string") {
    return text.normalize("NFC");
  } else {
    return text;
  }
}

/**
 * Process escaped values for forum posts
 * @param {string} text Post to be cleaned
 * @returns {string} text Cleaned text
 */
function escapeString(text) {
  return text
    .replace(/[\\]/g, "\\\\")
    .replace(/["]/g, '\\"')
    .replace(/[/]/g, "\\/")
    .replace(/[\b]/g, "\\b")
    .replace(/[\f]/g, "\\f")
    .replace(/[\n]/g, "\\n")
    .replace(/[\r]/g, "\\r")
    .replace(/[\t]/g, "\\t");
}

/**
 * Returns a date element for the following day
 * @param {Date} current_day
 * @returns {Date} nextDay
 */
function getNextDay(current_day) {
  current_day.setDate(current_day.getDate() + 1);
  return current_day;
}

/**
 * Returns the number of days between dates
 * @param {Date} beginDate
 * @param {Date} endDate
 * @returns {number}
 */
function getDayDiff(beginDate, endDate) {
  let count = 0;
  while (endDate.getDate() - beginDate.getDate() >= 1) {
    endDate.setDate(endDate.getDate() - 1);
    count += 1;
  }
  return count;
}

/**
 * Returns the course element related to a record from a log
 * @param {Object} eventlog Record of an event
 * @param {string} course_id Course identifier
 * @returns {string} Id of the related element
 */
function courseElementsFinder(eventlog, course_id) {
  let elementsID = coucourseElementsFinder_string(
    eventlog["event_type"],
    course_id
  );
  if (elementsID === "") {
    elementsID = coucourseElementsFinder_string(eventlog["path"], course_id);
  }
  if (elementsID === "") {
    elementsID = coucourseElementsFinder_string(eventlog["page"], course_id);
  }
  if (elementsID === "") {
    elementsID = coucourseElementsFinder_string(eventlog["referer"], course_id);
  }
  return elementsID;
}

/**
 * Processing for for the courseElementsFinder function
 * @param {Object} eventlog_item
 * @param {string} course_id Course identifier
 * @returns {string} Id of the related element
 */
function coucourseElementsFinder_string(eventlog_item, course_id) {
  let elementsId = "";
  let courseId_filtered = course_id;
  if (course_id.split(":").length > 1) {
    courseId_filtered = course_id.split(":")[1];
  }

  if (
    elementsId === "" &&
    eventlog_item.includes("+type@") &&
    eventlog_item.includes("block-v1:")
  ) {
    let templist = eventlog_item.split("/");
    for (let tempstring of templist) {
      if (tempstring.includes("+type@") && tempstring.includes("block-v1:")) {
        elementsId = tempstring;
      }
    }
  }
  if (elementsId === "" && eventlog_item.includes("courseware/")) {
    let templist = eventlog_item.split("/");
    let tempflag = false;
    for (let tempstring of templist) {
      if (tempstring === "courseware") {
        tempflag = true;
      } else {
        if (tempflag === true && tempstring !== "") {
          elementsId =
            "block-v1:" +
            courseId_filtered +
            "+type@chapter+block@" +
            tempstring;
          break;
        }
      }
    }
  }
  return elementsId;
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
    eventType: eventType,
    element: element,
    meta: meta,
  };
  return oraInfo;
}

module.exports = {
  getDayDiff,
  getNextDay,
  getORAEventTypeAndElement,
  courseElementsFinder,
  coucourseElementsFinder_string,
  processNull,
  compareDatetime,
  cleanUnicode,
  escapeString,
  learnerSegmentation,
  replaceAt,
};
