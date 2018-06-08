// Packages
const MongoClient = require('mongodb').MongoClient;

// Settings
const url = 'mongodb://localhost:27017';
const databaseName = "rdw";
const sourceCollection = "sourceData";
const targetCollection = "massa_cilinderinhoud";

// Runtime Variables
let client;

// Aggregate stages
const query = {
    voertuigsoort: {
        $exists: true
    },
    maximum_massa_samenstelling: {
        $exists: true,
        $ne: "0",
        $ne: "",
    },
    cilinderinhoud: {
        $exists: true,
        $ne: "0",
        $ne: "",
    }
};

const project = {
    voertuigsoort: true,
    maximum_massa_samenstelling: true,
    cilinderinhoud: true
};

const addFields = {
    reachability_distance: null,
    coordinates: ["$maximum_massa_samenstelling", "$cilinderinhoud"]
}

// Workflow
MongoClient.connect(url)
    .then(mongoClient => {
        client = mongoClient;
        return client.db(databaseName).collection(sourceCollection);
    })
    .then(collection => {
        // Aggregate returns no promise, for some reason -> so manually
        return new Promise(function (resolve, reject) {
            collection.aggregate([
                { $match: query },
                { $project: project },
                { $addFields: addFields },
                { $out: targetCollection }
            ], (error, success) => {
                if (error) {
                    reject(error);
                } else {
                    // Resolve by returning newly created collection 
                    resolve(client.db(databaseName).collection(targetCollection));
                }
            });
        })
    })
    .then(collection => {
        return collection.count();
    })
    .then(count => {
        console.log(`aggregate done: ${count} records`)
    })
    .then(() => {
        client.close()
    })
    .catch(error => {
        console.log(error.message)
    });