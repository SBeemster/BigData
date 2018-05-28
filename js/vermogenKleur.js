/* jshint esversion:6 */

// Settings
const database = "rdw";
const sourceCollection = "sourceData";
const targetCollection = "vermogen_per_kleur";

// Packages
const Q = require("q");
const mongojs = require("mongojs");
const db = mongojs(database, [sourceCollection, targetCollection]);

// Workflow
cleanUp()
    .then(mapReduce)
    .then(dbClose)
    .catch(error => console.error(`Error: ${error.message}`));

// Data Workers
const mapper = function () {
    emit({ kleur: this.eerste_kleur, vermogen: this.vermogen_massarijklaar }, 1);
};

const reducer = function (key, value) {
    return Array.sum(value);
};

const query = {
    voertuigsoort: {
        $eq: "Personenauto"
    },
    eerste_kleur: { 
        $exists: true, 
        $ne: "Niet geregistreerd"
    },
    vermogen_massarijklaar: {
        $exists: true,
        $ne: "0"
    }
};

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

function mapReduce() {
    let deferred = Q.defer();

    db[sourceCollection].mapReduce(
        mapper,
        reducer,
        {
            out: targetCollection,
            query, query
        },
        function (error, value) {
            if (error) {
                deferred.reject(new Error(`mapReduce - ${error.message}`));
            } else {
                console.log("mapReduce completed");
                deferred.resolve(value);
            }
        }
    );

    return deferred.promise;
}

function find() {
    let deferred = Q.defer();
    db[targetCollection].find(function (error, value) {
        if (error) {
            deferred.reject(new Error(`find - ${erro.messager}`));
        } else {
            console.log("find completed");
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