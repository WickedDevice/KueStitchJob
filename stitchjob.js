var kue = require('kue')
  , queue = kue.createQueue();
var fs = require('fs');
path = require('path');
var moment = require('moment');
var jStat = require('jStat').jStat;

let getDirectories = (srcpath) => {
  return fs.readdirSync(srcpath).filter( (file) => {
    return fs.statSync(path.join(srcpath, file)).isDirectory();
  });
}

let known_topic_prefixes = [
  "/orgs/wd/aqe/temperature",
  "/orgs/wd/aqe/humidity",
  "/orgs/wd/aqe/no2",
  "/orgs/wd/aqe/co",
  "/orgs/wd/aqe/so2",
  "/orgs/wd/aqe/o3",
  "/orgs/wd/aqe/particulate",
  "/orgs/wd/aqe/co2",
  "/orgs/wd/aqe/voc"
];

let getEggModelType = (serialNumber, extantTopics) => {
  if(extantTopics.indexOf("/orgs/wd/aqe/no2") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/no2/" + serialNumber) >= 0){
    if(extantTopics.indexOf("/orgs/wd/aqe/co") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/co/" + serialNumber) >= 0){
      return "model A";
    }
    else if(extantTopics.indexOf("/orgs/wd/aqe/o3") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/o3/" + serialNumber) >= 0){
      return "model J";
    }
    else {
      return "uknown no2";
    }
  }
  else if(extantTopics.indexOf("/orgs/wd/aqe/so2") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/so2/" + serialNumber) >= 0){ 
    if(extantTopics.indexOf("/orgs/wd/aqe/o3") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/o3/" + serialNumber) >= 0){
      return "model B";
    }
    else{
      return "unknown so2";
    }
  }
  else if(extantTopics.indexOf("/orgs/wd/aqe/particulate") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/particulate/" + serialNumber) >= 0){
    return "model C";
  }
  else if(extantTopics.indexOf("/orgs/wd/aqe/co2") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/co2/" + serialNumber) >= 0){
    return "model D";
  }
  else if(extantTopics.indexOf("/orgs/wd/aqe/voc") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/voc/" + serialNumber) >= 0){
    return "model E";
  }

  return "unknown";
};

let determineTimebase = (serialNumber, items, uniqueTopics) => {
  let temperatureTopic = null;
  if(uniqueTopics.indexOf("/orgs/wd/aqe/temperature") >= 0){
    temperatureTopic = "/orgs/wd/aqe/temperature";
  }
  else if(uniqueTopics.indexOf("/orgs/wd/aqe/temperature/" + serialNumber) >= 0){
    temperatureTopic = "/orgs/wd/aqe/temperature/" + serialNumber;
  }
  
  if(!temperatureTopic){
    return null;
  }

  let timeDiffs = [];
  let lastTime = null;

  items.filter( (item) => {
    return item.topic == temperatureTopic;
  }).forEach( (item) => {
    if(!lastTime){
      lastTime = moment(item.timestamp);
    }
    else{
      let currentTimestamp = moment(item.timestamp);
      timeDiffs.push(currentTimestamp.diff(lastTime, "milliseconds"));
      lastTime = currentTimestamp;
    }
  });

  // determine the standard deviation of the time differences
  // filter out any that are outside 1 standard deviation from the mean
  let stdev = jStat.stdev(timeDiffs);
  let mean = jStat.mean(timeDiffs);
  // remove outliers
  timeDiffs = timeDiffs.filter( (diff) => {
    return (diff >= mean - stdev) && (diff <= mean + stdev);
  });

  // recompute the mean
  return jStat.mean(timeDiffs);
};

let appendHeaderRow = (model, filepath, temperatureUnits) => {
  if(!temperatureUnits){
    temperatureUnits = "???";
  }

  let headerRow = `timestamp,temperature[${temperatureUnits}],humidity[%],`;

  switch(model){
  case "model A":
    headerRow += "no2[ppb],co[ppm],no2[V],co[V]";
    break;
  case "model B":
    headerRow += "so2[ppb],o3[ppb],so2[V],o3[V]";
    break;
  case "model C":
    headerRow += "pm[ug/m^3],pm[V]";
    break;
  case "model D":
    headerRow += "co2[ppm]";
    break;
  case "model E":
    headerRow += "co2[ppm],tvoc[ppb],resistance[ohm]";
    break;
  case "model J":
    headerRow += "no2[ppb],o3[ppb],no2_we[V],no2_aux[V],o3[V]";
    break;
  default:
    break;
  }  

  headerRow += ",latitude[deg],longitude[deg],altitude[deg]\r\n";
  fs.appendFileSync(filepath, headerRow);
};   

let getTemperatureUnits = (items) => {
  for(let ii = 0; ii < items.length; ii++){
    let item = items[ii];
    if(item.topic.indexOf("temperature") >= 0){
      return item["converted-units"];
    }
  };
};

queue.process('stitch', (job, done) => {
  // the download job is going to need the following parameters
  //    save_path - the full path to where the result should be saved
  //    user_id   - the user id that made the request
  //    email     - the email address that should be notified on zip completed
  //    sequence  - the sequence number within this request chain

  // 1. for each folder in save_path, create an empty csv file with the same name
  getDirectories(job.data.save_path).forEach( (dir) => {
    fs.closeSync(fs.openSync(`${job.data.save_path}/${dir}.csv`, 'w'));
  });

  // 2. for each folder in save_path, analyze file "1.json" to infer the type of Egg
  //    model, generate an appropriate header row and append it to the csv file, and 
  //    save the egg model in a variable for later dependencies, and establish the
  //    time base of the data in the file. In order to determine the time base,
  //    analyze the time differences between adjacent messages on the temperature
  //    topic (which all Eggs have)
  getDirectories(job.data.save_path).forEach( (dir) => {
    let items = require(`${job.data.save_path}/${dir}/1.json`);

    // collect the unique topics in the first batch of messages
    let uniqueTopics = {};
    items.forEach( (item) => {
      uniqueTopics[item.topic] = 1;
    });
    uniqueTopics = Object.keys(uniqueTopics);
    
    job.log(uniqueTopics);
    let modelType = getEggModelType(dir, uniqueTopics);
    job.log(`Egg Serial Number ${dir} is ${modelType} type`); 

    let temperatureUnits = getTemperatureUnits(items);
    appendHeaderRow(modelType, `${job.data.save_path}/${dir}.csv`, temperatureUnits);

    let timeBase = determineTimebase(dir, items, uniqueTopics);
    job.log(`Egg Serial Number ${dir} has timebase of ${timeBase} ms`);

    // 3. for each folder in save_path, list the files in that folder
    //    load them into memory one at a time, and process records in each file
    //    generating one csv record at a time and appending to the csv file
    //    progressively as you go
    let currentRecord = [];
    fs.readdirSync(`${job.data.save_path}/${dir}/`).forEach( (filename) => {
      let fullPathToFile = `${job.data.save_path}/${dir}/${filename}`;
      require(fullPathToFile).forEach( (datum, index) => {
        if(index == 0){ 
          // special case, use this timestamp
          
        }
        // TODO: determine if this datum fits into the timeframe of the current record
        //       if it does, then just add it

        // TODO: if it doesn't, then append the stringified current record to the csv file
        //       then reset the current record
        //       and add this datum to it, and use it's timestamp

      });            
    });    

    // TODO: if the current_record is not blank, then add it to the CSV file

  });

  done();
});

process.once( 'uncaughtException', function(err){
  console.error( 'Something bad happened: ', err );
  queue.shutdown( 1000, function(err2){
    console.error( 'Kue shutdown result: ', err2 || 'OK' );
    process.exit( 0 );
  });
});
