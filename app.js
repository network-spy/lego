const MongoClient = require('mongodb').MongoClient;

const mongoDbUrl = 'mongodb://127.0.0.1:27017/world';

MongoClient.connect(mongoDbUrl, function(err, db) {
    if (err) {
        console.log(err);
        return;
    }
    console.log("Connected to server.");
    clearStat(db).then(
        result => {
            console.log(result);
            processZips(db).then(
                result => {
                    console.log(result);
                    closeMongoDBConnection(db);
                },
                error => {
                    console.log(error);
                    closeMongoDBConnection(db);
                }
            );
        },
        error => {
            console.log(error);
            closeMongoDBConnection(db);
        }
    );
});


let closeMongoDBConnection = (db) => {
    db.close();
    console.log("Disconnected from server.");
};


let clearStat = (db) => {
	return new Promise((resolve, reject) => {
        db.collection('stat').deleteMany({}, function(err, results) {
            if (err) {
                reject(err);
			}
            resolve('Stat data cleared');
        });
    });
};


let processZips = (db) => {
    return new Promise((resolve, reject) => {
        db.collection('zip').find({}, {"_id":1}).each((err, zipCode) => {
            if (zipCode == null) {
                resolve('Zips precessed');
            } else {
                findRestaurantsByZip(db, zipCode._id).then(
                    result => {
                        insertToStat(db, zipCode, result).then(
                            result => {
                                console.log('Inserted: ');
                                console.dir(result);
                            },
                            error => {
                                reject(error);
                            }
                        );
                    },
                    error => {
                        reject(error);
                    }
                );
            }
        });
    });
};


let findRestaurantsByZip = (db, zipCode) => {
    return new Promise((resolve, reject) => {
        db.collection('restaurant').find({"address.zipcode": zipCode}).toArray((err, restaurants) => {
            if (err) {
                reject(err);
            }
            resolve(restaurants);
        });
    });
};


let insertToStat = (db, zip, restaurants) => {
	return new Promise((resolve, reject) => {
        let statDocument = {};
        statDocument.zip_code = zip;
        statDocument.restaurants = restaurants;
        db.collection('stat').insertOne(statDocument).then(
            result => {
                resolve(statDocument);
            },
            error => {
                reject(error);
            }
        );
    });
};