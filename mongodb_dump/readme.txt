sudo systemctl start mongodb
mongoimport --collection zip --db world --file zips.json 
mongoimport --collection restaurant --db world --file restaurants.json