const { MongoClient } = require("mongodb");
const credentials = require("./credentials");

// Database Name
const dbName = "edx_test";
const username = encodeURIComponent(credentials.admin.user);
const password = encodeURIComponent(credentials.admin.pwd);
const mogodb_url = credentials.mongodb_url;

const dev = true;

const url = `mongodb://${mogodb_url}`;
// const url = `mongodb://${username}:${password}@${mogodb_url}`;

async function testConnection() {
  const client = new MongoClient(url);
  try {
    await client.connect();
    console.log("Connected successfully to the MongoDB server!");
    const db = client.db(dbName);
    const collections = await db.collections();
    console.log(
      "Collections in the database:",
      collections.map((c) => c.collectionName),
    );
  } catch (err) {
    console.error("Connection failed:", err);
  } finally {
    await client.close();
  }
}

async function mongoInsert(collectionName, dataRows) {
  const client = new MongoClient(url);
  try {
    await client.connect();
    const collection = client.db(dbName).collection(collectionName);

    for (let v of dataRows) {
      for (let field of Object.keys(v)) {
        if (field.includes("time")) {
          let date = v[field];
          v[field] = new Date(date);
        }
      }
    }
    await collection.insertMany(dataRows);
  } catch (err) {
    console.error(err);
  } finally {
    await client.close();
  }
}

async function mongoQuery(collectionName, query = {}, limit = 0) {
  const client = new MongoClient(url, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  try {
    if (collectionName === "clickstream" && dev) {
      limit = 10000;
    }
    await client.connect();
    const collection = client.db(dbName).collection(collectionName);
    const result = await collection.find(query).limit(limit).toArray();
    return result;
  } catch (err) {
    console.error("An error occurred while querying MongoDB:", err);
  } finally {
    await client.close();
  }
}

async function deleteIfExists(collectionName) {
  const client = new MongoClient(url);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collections = await db
      .listCollections({ name: collectionName })
      .toArray();
    if (collections.length > 0) {
      await db.collection(collectionName).drop();
      console.log(`Dropped existing collection: ${collectionName}`);
    } else {
      console.log(`Collection ${collectionName} does not exist.`);
    }
  } catch (err) {
    console.error(
      `An error occurred while checking/deleting collection ${collectionName}:`,
      err,
    );
  } finally {
    await client.close();
  }
}

async function clearDatabase() {
  const client = new MongoClient(url);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collections = await db.collections();
    for (let collection of collections) {
      await db.collection(collection.collectionName).drop();
    }
    console.log("Cleared database");
  } catch (err) {
    console.error("An error occurred while clearing the database:", err);
  } finally {
    await client.close();
  }
}

async function clearSessionsCollections() {
  // table names: sessions, video_interactions, assessments, submissions, quiz_sessions.
  const client = new MongoClient(url);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collections = await db.collections();
    for (let collection of collections) {
      if (
        collection.collectionName === "sessions" ||
        collection.collectionName === "video_interactions" ||
        collection.collectionName === "assessments" ||
        collection.collectionName === "submissions" ||
        collection.collectionName === "quiz_sessions"
      ) {
        await db.collection(collection.collectionName).drop();
      }
    }
    console.log("Cleared sessions collections\n");
  } catch (err) {
    console.error(
      "An error occurred while clearing the sessions collections:",
      err,
    );
  } finally {
    await client.close();
  }
}

module.exports = {
  testConnection,
  mongoInsert,
  mongoQuery,
  deleteIfExists,
  clearDatabase,
  clearSessionsCollections,
};
