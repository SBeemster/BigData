// Packages
const MongoClient = require("mongodb").MongoClient;

// Settings
const url = 'mongodb://localhost:27017';
const databaseName = "rdw";
const sourceCollection = "sourceData";
const targetCollection = "massa_cilinderinhoud";

// Runtime Variables
let client;

MongoClient.connect(url)
    .then(mongoClient => {
        client = mongoClient;
        return client.db(databaseName).collection(targetCollection);
    })
    .then(collection => {
        collection.find({}).forEach(docuement => {
            // markeer as bezocht
            // zoek naar buren
            // bla bla -> rest van algortime
        });
    })