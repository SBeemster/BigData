/* jshint esversion:6 */

// Settings
const database = "rdw";
const targetCollection = "sourceData";
const documentLimit = 9999;

// Packages
const http = require("http");
const Q = require("q");
const mongojs = require('mongojs');
const db = mongojs(database, [targetCollection]);

// Workflow
cleanUp()
    .then(get)
    .then(insert)
    .then(report => console.log(`Inserted: ${report.length} documents`))
    .then(dbClose)
    .catch(error => console.error(`Error: ${error.message}`));

// Functions
function cleanUp() {
    let deferred = Q.defer();

    db[targetCollection].remove(function (error, value) {
        if (error) {
            deferred.reject(new Error(`cleanUp - ${error.message}`));
        } else {
            console.log("cleanUp completed");
            deferred.resolve(value);
        }
    });

    return deferred.promise;
}

function get() {
    let deferred = Q.defer();

    let targetPath = "/resource/m9d7-ebf2.json"
    targetPath += documentLimit > 0 ? `?$limit=${documentLimit}` : "";

    http.get({
        host: "opendata.rdw.nl",
        path: targetPath,
    }, res => {
        const { statusCode } = res;

        if (statusCode !== 200) {
            res.resume(); // consume response data to free up memory
            deferred.reject(new Error(`get - Status Code: ${statusCode}`));
        }

        let data = "";
        res.on("data", (chunk) => {
            data += chunk;
        });
        res.on("end", () => {
            console.log("get completed");
            deferred.resolve(data);
        });

    }).on("error", (error) => {
        deferred.reject(new Error(`get - ${error.message}`));
    });

    return deferred.promise;
}

function insert(data) {
    let deferred = Q.defer();

    db[targetCollection].insert(JSON.parse(data), function (error, value) {
        if (error) {
            console.error(`insert - ${error.message}`);
        } else {
            console.log("insert completed");
            deferred.resolve(value);
        }
    });

    return deferred.promise;
}

function dbClose() {
    let deferred = Q.defer();

    db.close(function (error, value) {
        if (error) {
            deferred.reject(new Error(`dbClose - ${error.message}`));
        } else {
            console.log("dbClose completed");
            deferred.resolve(value);
        }
    });

    return deferred.promise;
}
