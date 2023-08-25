const { MongoClient } = require('mongodb');
const credentials = require('./credentials');

// Database Name
const dbName = 'edx_test';
const username = encodeURIComponent(credentials.admin.user);
const password = encodeURIComponent(credentials.admin.pwd);
const mogodb_url = credentials.mongodb_url;

const url = `mongodb://${username}:${password}@${mogodb_url}`;

async function testConnection() {
  const client = new MongoClient(url);
  try {
    await client.connect();
    console.log("Connected successfully to the MongoDB server!");
    const db = client.db(dbName);
    const collections = await db.collections();
    console.log("Collections in the database:", collections.map(c => c.collectionName));
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

    // From original code, not sure why this is here
    // if (collectionName === 'forum_interaction'){
    //   console.log('Deleting existing forum_interaction collection')
    //   await deleteIfExists(collectionName);
    // }

    // Process the dataRows as needed (e.g., handling date fields)
    for (let v of dataRows) {
      for (let field of Object.keys(v)) {
        if (field.includes('time')) {
          let date = v[field];
          v[field] = new Date(date);
        }
      }
    }
    const result = await collection.insertMany(dataRows);
    let info = '';
    if (collectionName === 'webdata' || collectionName === 'metadata') {info = dataRows[0]['name']} else {info = dataRows.length}
    if (dataRows.length > 0 ) {
        let today = new Date();
        let time = today.getHours() + ":" + today.getMinutes() + ":" + today.getSeconds() + '.' + today.getMilliseconds();
        console.log('Successfully added', info, 'to' , collectionName, ' at ', time);
    }
  } catch (err) {
    console.error(err);
  } 
  finally {
    await client.close();
  }
}


async function mongoQuery(collectionName, query = {}, limit = 0) {
  const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true });
  try {
      await client.connect();
      const collection = client.db(dbName).collection(collectionName);
      const result = await collection.find(query).limit(limit).toArray();
      return result;
  } catch (err) {
      console.error('An error occurred while querying MongoDB:', err);
  } finally {
      await client.close();
  }
}


async function deleteIfExists(collectionName) {
  const client = new MongoClient(url);
  try {
    await client.connect();
    const db = client.db(dbName);
    const collections = await db.listCollections({ name: collectionName }).toArray();
    if (collections.length > 0) {
      await db.collection(collectionName).drop();
      console.log(`Dropped existing collection: ${collectionName}`);
    } else {
      console.log(`Collection ${collectionName} does not exist.`);
    }
  } catch (err) {
    console.error(`An error occurred while checking/deleting collection ${collectionName}:`, err);
  } finally {
    await client.close();
  }
}

module.exports = {testConnection, mongoInsert, mongoQuery, deleteIfExists};
