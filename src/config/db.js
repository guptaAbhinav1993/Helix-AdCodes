const { MongoClient } = require('mongodb');
require('dotenv').config();

const DB_NAME = 'xapads_usp';

let client = null;
let db = null;

/**
 * Connect to MongoDB Atlas (USP-cluster) and return the xapads_usp database.
 * Reuses the same connection on subsequent calls.
 */
async function connect() {
  const uri = process.env.MONGODB_URI && String(process.env.MONGODB_URI).trim();
  if (!uri) {
    throw new Error(
      'Missing MONGODB_URI in environment. Add it to .env (see .env.example).'
    );
  }
  if (db) return db;
  client = new MongoClient(uri);
  await client.connect();
  db = client.db(DB_NAME);
  return db;
}

/**
 * Get the xapads_usp database. Must call connect() first.
 */
function getDb() {
  if (!db) throw new Error('Not connected. Call connect() first.');
  return db;
}

/**
 * Get a collection by name (for working with "tables").
 */
function getCollection(name) {
  return getDb().collection(name);
}

/**
 * Get the underlying MongoClient (e.g. for admin commands).
 */
function getClient() {
  if (!client) throw new Error('Not connected. Call connect() first.');
  return client;
}

/**
 * Close the MongoDB connection.
 */
async function close() {
  if (client) {
    await client.close();
    client = null;
    db = null;
  }
}

module.exports = {
  connect,
  getDb,
  getCollection,
  getClient,
  close,
  DB_NAME,
};
