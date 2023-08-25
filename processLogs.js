const { MongoClient } = require('mongodb');
const credentials = require('./credentials');
const { mongoQuery, mongoInsert } = require('./databaseHelpers');
const { processNull } = require('./helpers');


async function processGeneralSessions() {    
    let courseMetadataMap = await mongoQuery('metadata', {
        'name': 'metadata_map'
    })
    courseMetadataMap = courseMetadataMap[0]['object'];

    let current_course_id = courseMetadataMap["course_id"];
    current_course_id = current_course_id.slice(current_course_id.indexOf('+') + 1, current_course_id.lastIndexOf('+') + 7);
    console.log('Current course id:', current_course_id);

    // Query the MongoDB collection for the log records
    const logCollection = await mongoQuery('clickstream', {});
    let learner_all_event_logs = [];
    let updated_learner_all_event_logs = {};
    let session_record = [];

    let course_learner_id_set = new Set();

    for (let jsonObject of logCollection){
        if (jsonObject['context'].hasOwnProperty('user_id') === false ){ continue; }
        let global_learner_id = jsonObject["context"]["user_id"];
        let event_type = jsonObject["event_type"];

        if (global_learner_id !== ''){
            let course_id = jsonObject["context"]["course_id"];

            let course_learner_id = course_id + "_" + global_learner_id;

            let event_time = new Date(jsonObject["time"]);

            if (course_learner_id_set.has(course_learner_id)){
                learner_all_event_logs[course_learner_id].push({"event_time":event_time, "event_type":event_type});
            } else {
                learner_all_event_logs[course_learner_id] = [{"event_time":event_time, "event_type":event_type}];
                course_learner_id_set.add(course_learner_id);
            }
        }
    }
    console.log('Number of course_learner_ids:', course_learner_id_set.size);

    for (let course_learner_id in learner_all_event_logs){
        let event_logs = learner_all_event_logs[course_learner_id];

        event_logs.sort(function(a, b) {
            return new Date(a.event_time) - new Date(b.event_time) ;
        });

        let session_id = null,
            start_time = null,
            end_time = null,
            final_time = null;
        for (let i in event_logs){
            if (start_time == null){
                start_time = new Date(event_logs[i]["event_time"]);
                end_time = new Date(event_logs[i]["event_time"]);
            } else {
                let verification_time = new Date(end_time);
                if (new Date(event_logs[i]["event_time"]) > verification_time.setMinutes(verification_time.getMinutes() + 30)){

                    // let session_id = course_learner_id + '_' + start_time + '_' + end_time;
                    let session_id = course_learner_id + '_' + start_time.getTime() + '_' + end_time.getTime();

                    let duration = (end_time - start_time)/1000;

                    if (duration > 5){
                        let array = [session_id, course_learner_id, start_time, end_time, duration];
                        session_record.push(array);
                    }

                    final_time = new Date(event_logs[i]["event_time"]);

                    //Re-initialization
                    session_id = "";
                    start_time = new Date(event_logs[i]["event_time"]);
                    end_time = new Date(event_logs[i]["event_time"]);

                } else {
                    if (event_logs[i]["event_type"] === "page_close"){
                        end_time = new Date(event_logs[i]["event_time"]);
                        session_id = course_learner_id + '_' + start_time.getTime() + '_' + end_time.getTime();
                        let duration = (end_time - start_time)/1000;

                        if (duration > 5){
                            let array = [session_id, course_learner_id, start_time, end_time, duration];
                            session_record.push(array);
                        }
                        session_id = "";
                        start_time = null;
                        end_time = null;

                        final_time = new Date(event_logs[i]["event_time"]);

                    } else {
                        end_time = new Date(event_logs[i]["event_time"]);
                    }
                }
            }
        }
        if (final_time != null){
            let new_logs = [];
            for (let x in event_logs){
                let log = event_logs[x];
                if (new Date(log["event_time"]) >= final_time){
                    new_logs.push(log);
                }
            }
            updated_learner_all_event_logs[course_learner_id] = new_logs;
        }
    }

    let updated_session_record = [];
    let session_id_set = new Set();
    for (let array of session_record){
        let session_id = array[0];
        if (!(session_id_set.has(session_id))){
            session_id_set.add(session_id);
            updated_session_record.push(array);
        }
    }

    console.log('Number of sessions:', updated_session_record.length);
    session_record = updated_session_record;
    if (session_record.length > 0){
        let data = [];
        for (let x in session_record){
            let array = session_record[x];
            let session_id = array[0];
            let course_learner_id = array[1];
            let start_time = array[2];
            let end_time = array[3];
            let duration = processNull(array[4]);
            let values = {'session_id':session_id, 'course_learner_id':course_learner_id,
                'start_time':start_time,
                'end_time': end_time, 'duration':duration};
            data.push(values);
        }
        await mongoInsert('sessions', data);
    } else {
        console.log('no session info');
    }
}

module.exports = {processGeneralSessions};