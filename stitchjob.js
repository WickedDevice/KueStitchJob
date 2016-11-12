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

    let timeBase = determineTimebase(dir, items, uniqueTopics);
    job.log(`Egg Serial Number ${dir} has timebase of ${timeBase} ms`);

    // 3. for each folder in save_path, list the files in that folder
    //    load them into memory one at a time, and process records in each file
    //    generating one csv record at a time and appending to the csv file
    //    progressively as you go
        
 
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
