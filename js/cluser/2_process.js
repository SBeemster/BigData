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
        // goeie promise van maken
        let massaMax = collection.find().sort({maximum_massa_samenstelling:1}).limit(1).toArray();
        let massaMax = collection.find().sort({maximum_massa_samenstelling:-1}).limit(1).toArray();
        let cilinderinhoudMin = collection.find().sort({cilinderinhoud:1}).limit(1).toArray();
        let cilinderinhoudMax = collection.find().sort({cilinderinhoud:-1}).limit(1).toArray();

        return massaMax;
    })
    .then(() => {
        // normaliseren van coordinatne
    })
    .then(collection => {
        return collection.createIndex(
            { coordinates: "2d" },
            { min: 0, max: 1 }
        );
    })
    .then(() => {
        client.close();
    })
    .catch(error => {
        console.log(error.message);
    });