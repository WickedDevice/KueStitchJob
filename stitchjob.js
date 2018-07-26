//jshint esversion: 6
// require('heapdump');

const promiseDoWhilst = require('promise-do-whilst');
const kue = require('kue'), queue = kue.createQueue();
const fs = require('fs');
const path = require('path');
const moment = require('moment');
const jStat = require('jStat').jStat;

/*
function generateHeapDumpAndStats(){
  console.log("generateHeapDumpAndStats");
  //1. Force garbage collection every time this function is called
  try {
    global.gc();
  } catch (e) {
    console.log("You must run program with 'node --expose-gc index.js' or 'npm start'");
    process.exit();
  }

  //2. Output Heap stats
  var heapUsed = process.memoryUsage().heapUsed;
  console.log("Program is using " + heapUsed + " bytes of Heap.")

  //3. Get Heap dump
  process.kill(process.pid, 'SIGUSR2');
}

setInterval(generateHeapDumpAndStats, 30000);
*/

const getDirectories = (srcpath) => {
  return fs.readdirSync(srcpath).filter((file) => {
    return fs.statSync(path.join(srcpath, file)).isDirectory();
  });
};

const known_topic_prefixes = [
  "/orgs/wd/aqe/temperature",
  "/orgs/wd/aqe/humidity",
  "/orgs/wd/aqe/no2",
  "/orgs/wd/aqe/co",
  "/orgs/wd/aqe/so2",
  "/orgs/wd/aqe/o3",
  "/orgs/wd/aqe/particulate",
  "/orgs/wd/aqe/co2",
  "/orgs/wd/aqe/voc",
  "/orgs/wd/aqe/pressure",
  "/orgs/wd/aqe/battery",
  "/orgs/wd/aqe/water/temperature",
  "/orgs/wd/aqe/water/conductivity",
  "/orgs/wd/aqe/water/turbidity",
  "/orgs/wd/aqe/water/ph"
];

const invalid_value_string = "---";

const isNumeric = (n) => {
  return !isNaN(parseFloat(n)) && isFinite(n);
};

const valueOrInvalid = (value) => {
  if (!isNumeric(value)) {
    return invalid_value_string;
  }

  return value;
};

const addMessageToRecord = (message, model, compensated, instantaneous, record, hasPressure, hasBattery) => {
  const natural_topic = message.topic.replace(`/${message['serial-number']}`, '');
  if (known_topic_prefixes.indexOf(natural_topic) < 0) {
    if (natural_topic.indexOf("heartbeat") < 0) {
      console.log("UNKNOWN TOPIC ENCOUNTERED", natural_topic);
    }
    return;
  }

  // does this message contain location data?
  let latitude = null;
  let longitude = null;
  let altitude = null;

  if (message.latitude && message.longitude) { // the old way
    latitude = +message.latitude;
    longitude = +message.longitude;
  }
  else if (message.__location && message.__location.lat && message.__location.lon) { // the new way
    latitude = +message.__location.lat;
    longitude = +message.__location.lon;
  }

  if (message.__location && message.__location.alt) { // the new way
    altitude = +message.__location.alt;
  }
  else if (message.altitude) { // the old way
    altitude = +message.altitude;
  }


  // in CSV, GPS are always the last three coordinates, patch them in if we've got them
  record[getRecordLengthByModelType(model, hasPressure, hasBattery) - 1] = valueOrInvalid(altitude);
  record[getRecordLengthByModelType(model, hasPressure, hasBattery) - 2] = valueOrInvalid(longitude);
  record[getRecordLengthByModelType(model, hasPressure, hasBattery) - 3] = valueOrInvalid(latitude);

  // console.log("Model is: ", model);
  if (message.topic.indexOf("/orgs/wd/aqe/temperature") >= 0) {
    record[0] = message.timestamp;
    if (!compensated && !instantaneous) {
      record[1] = valueOrInvalid(message['raw-value']);
    }
    else if (compensated && !instantaneous) {
      record[1] = valueOrInvalid(message['converted-value']);
    }
    else if (!compensated && instantaneous) {
      record[1] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
    }
    else if (compensated && instantaneous) {
      record[1] = valueOrInvalid(message['converted-value']);
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/humidity") >= 0) {
    if (!compensated && !instantaneous) {
      record[2] = valueOrInvalid(message['raw-value']);
    }
    else if (compensated && !instantaneous) {
      record[2] = valueOrInvalid(message['converted-value']);
    }
    else if (!compensated && instantaneous) {
      record[2] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
    }
    else if (compensated && instantaneous) {
      record[2] = valueOrInvalid(message['converted-value']);
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/no2") >= 0) {
    if (model === 'model A') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    } else if (model === 'model J') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['converted-value']);
        record[5] = valueOrInvalid(message['raw-value']);
        record[6] = valueOrInvalid(message['raw-value2']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-value']);
        record[6] = valueOrInvalid(message['raw-value2']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['converted-value']);
        record[5] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
        record[6] = valueOrInvalid(message['raw-instant-value2'] || message['raw-value2']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
        record[6] = valueOrInvalid(message['raw-instant-value2'] || message['raw-value2']);
      }
    } else if (model === 'model K') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model Q') {
      if (!compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model R') {
      if (!compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model S') {
      if (!compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/so2") >= 0) {
    if (model === 'model B') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[5] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model S') {
      if (!compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model U') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }    
    else if (model === 'model Y') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['converted-value']);
        record[4] = valueOrInvalid(message['raw-value']);
        record[5] = valueOrInvalid(message['raw-value2']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
        record[5] = valueOrInvalid(message['raw-value2']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['converted-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
        record[5] = valueOrInvalid(message['raw-instant-value2'] || message['raw-value2']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
        record[5] = valueOrInvalid(message['raw-instant-value2'] || message['raw-value2']);
      }
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/o3") >= 0) {
    if (model === 'model B') {
      if (!compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    } else if (model === 'model J') {
      if (!compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['converted-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[4] = valueOrInvalid(message['converted-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model R') {
      if (!compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model T') {
      if (!compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/particulate") >= 0) {
    if (model === 'model C') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model G') {
      record[4] = valueOrInvalid(message.pm1p0);
      record[5] = valueOrInvalid(message.pm2p5);
      record[6] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model K') {
      record[5] = valueOrInvalid(message.pm1p0);
      record[6] = valueOrInvalid(message.pm2p5);
      record[7] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model L') {
      record[5] = valueOrInvalid(message.pm1p0);
      record[6] = valueOrInvalid(message.pm2p5);
      record[7] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model N') {
      record[3] = valueOrInvalid(message.pm1p0);
      record[4] = valueOrInvalid(message.pm2p5);
      record[5] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model P') {
      record[4] = valueOrInvalid(message.pm1p0);
      record[5] = valueOrInvalid(message.pm2p5);
      record[6] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model Q') {
      record[3] = valueOrInvalid(message.pm1p0);
      record[4] = valueOrInvalid(message.pm2p5);
      record[5] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model R') {
      record[3] = valueOrInvalid(message.pm1p0);
      record[4] = valueOrInvalid(message.pm2p5);
      record[5] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model S') {
      record[3] = valueOrInvalid(message.pm1p0);
      record[4] = valueOrInvalid(message.pm2p5);
      record[5] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model T') {
      record[3] = valueOrInvalid(message.pm1p0);
      record[4] = valueOrInvalid(message.pm2p5);
      record[5] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model U') {
      record[5] = valueOrInvalid(message.pm1p0);
      record[6] = valueOrInvalid(message.pm2p5);
      record[7] = valueOrInvalid(message.pm10p0);
    }
    else if (model === 'model Y') {
      record[6] = valueOrInvalid(message.pm1p0);
      record[7] = valueOrInvalid(message.pm2p5);
      record[8] = valueOrInvalid(message.pm10p0);
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/pressure") >= 0) {
    let pressureIndex = -4;
    if(hasBattery) {
      pressureIndex--;
    }
    record[getRecordLengthByModelType(model, hasPressure, hasBattery) + pressureIndex] = valueOrInvalid(message.pressure);
    if ((record[getRecordLengthByModelType(model, hasPressure, hasBattery) - 1] === undefined) && !!message.altitude) {
      record[getRecordLengthByModelType(model, hasPressure, hasBattery) - 1] = valueOrInvalid(message.altitude);
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/battery") >= 0) {
    record[getRecordLengthByModelType(model, hasPressure, hasBattery) - 4] = valueOrInvalid(message['converted-value']);
  }  
  else if (message.topic.indexOf("/orgs/wd/aqe/co2") >= 0) {
    if (['model D', 'model G', 'model M', 'model P', 'model V'].indexOf(model) >= 0) {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['raw-instant-value']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['raw-instant-value']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
      }
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/co") >= 0) {
    if (model === 'model A') {
      if (!compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[4] = valueOrInvalid(message['compensated-value']);
        record[6] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model L') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-value']);
        record[4] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model Q') {
      if (!compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[6] = valueOrInvalid(message['compensated-value']);
        record[7] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
    else if (model === 'model T') {
      if (!compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (compensated && !instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-value']);
      }
      else if (!compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
      else if (compensated && instantaneous) {
        record[8] = valueOrInvalid(message['compensated-value']);
        record[9] = valueOrInvalid(message['raw-instant-value'] || message['raw-value']);
      }
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/voc") >= 0) {
    if (model === 'model E') {
      if (!compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['converted-co2']);
        record[4] = valueOrInvalid(message['converted-tvoc']);
        record[5] = valueOrInvalid(message['converted-resistance']);
      }
      else if (compensated && !instantaneous) {
        record[3] = valueOrInvalid(message['compensated-co2']);
        record[4] = valueOrInvalid(message['compensated-tvoc']);
        record[5] = valueOrInvalid(message['compensated-resistance']);
      }
      else if (!compensated && instantaneous) {
        record[3] = valueOrInvalid(message['raw-instant-co2']);
        record[4] = valueOrInvalid(message['raw-instant-tvoc']);
        record[5] = valueOrInvalid(message['raw-instant-resistance']);
      }
      else if (compensated && instantaneous) {
        record[3] = valueOrInvalid(message['compensated-instant-co2']);
        record[4] = valueOrInvalid(message['compensated-instant-tvoc']);
        record[5] = valueOrInvalid(message['compensated-instant-resistance']);
      }
    }
    else if (model === 'model P') {
      if (!compensated && !instantaneous) {
        record[7] = valueOrInvalid(message['converted-co2']);
        record[8] = valueOrInvalid(message['converted-tvoc']);
        record[9] = valueOrInvalid(message['converted-resistance']);
      }
      else if (compensated && !instantaneous) {
        record[7] = valueOrInvalid(message['compensated-co2']);
        record[8] = valueOrInvalid(message['compensated-tvoc']);
        record[9] = valueOrInvalid(message['compensated-resistance']);
      }
      else if (!compensated && instantaneous) {
        record[7] = valueOrInvalid(message['raw-instant-co2']);
        record[8] = valueOrInvalid(message['raw-instant-tvoc']);
        record[9] = valueOrInvalid(message['raw-instant-resistance']);
      }
      else if (compensated && instantaneous) {
        record[7] = valueOrInvalid(message['compensated-instant-co2']);
        record[8] = valueOrInvalid(message['compensated-instant-tvoc']);
        record[9] = valueOrInvalid(message['compensated-instant-resistance']);
      }
    }
    else if (model === 'model V') {
      if (!compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['converted-co2']);
        record[5] = valueOrInvalid(message['converted-tvoc']);
        record[6] = valueOrInvalid(message['converted-resistance']);
      }
      else if (compensated && !instantaneous) {
        record[4] = valueOrInvalid(message['compensated-co2']);
        record[5] = valueOrInvalid(message['compensated-tvoc']);
        record[6] = valueOrInvalid(message['compensated-resistance']);
      }
      else if (!compensated && instantaneous) {
        record[4] = valueOrInvalid(message['raw-instant-co2']);
        record[5] = valueOrInvalid(message['raw-instant-tvoc']);
        record[6] = valueOrInvalid(message['raw-instant-resistance']);
      }
      else if (compensated && instantaneous) {
        record[4] = valueOrInvalid(message['compensated-instant-co2']);
        record[5] = valueOrInvalid(message['compensated-instant-tvoc']);
        record[6] = valueOrInvalid(message['compensated-instant-resistance']);
      }
    }
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/water/temperature") >= 0) {
    record[1] = valueOrInvalid(message.value);
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/water/conductivity") >= 0) {
    record[2] = valueOrInvalid(message.value);
    record[3] = valueOrInvalid(message['raw-value']);
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/water/turbidity") >= 0) {
    record[4] = valueOrInvalid(message.value);
    record[5] = valueOrInvalid(message['raw-value']);    
  }
  else if (message.topic.indexOf("/orgs/wd/aqe/water/ph") >= 0) {
    record[6] = valueOrInvalid(message.value);
    record[7] = valueOrInvalid(message['raw-value']);    
  }
};

const refineModelType = (modelType, data) => {
  let dat;
  switch (modelType) {
    case 'model C': // model C must be disambiguated based on message content
      dat = data.find(v => v.topic.indexOf('particulate') >= 0);
      if (dat) {
        if (dat.pm1p0) {
          return 'model N';
        }
      }
      break;
    case 'model U': 
      dat = data.find(v => v.topic.indexOf('so2') >= 0);
      if (dat) {
        if (dat['raw-value2']) {
          return 'model Y';
        }
      }
      break;    
  }
  return modelType;
};

const getEggModelType = (dirname, extantTopics) => {
  let serialNumber = dirname.split("_");
  serialNumber = serialNumber[serialNumber.length - 1]; // the last part of the dirname
  console.log(extantTopics);
  const hasCO2 = extantTopics.indexOf("/orgs/wd/aqe/co2") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/co2/" + serialNumber) >= 0;
  const hasVOC = extantTopics.indexOf("/orgs/wd/aqe/voc") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/voc/" + serialNumber) >= 0;
  const hasCO = extantTopics.indexOf("/orgs/wd/aqe/co") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/co/" + serialNumber) >= 0;
  const hasNO2 = extantTopics.indexOf("/orgs/wd/aqe/no2") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/no2/" + serialNumber) >= 0;
  const hasSO2 = extantTopics.indexOf("/orgs/wd/aqe/so2") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/so2/" + serialNumber) >= 0;
  const hasO3 = extantTopics.indexOf("/orgs/wd/aqe/o3") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/o3/" + serialNumber) >= 0;
  const hasParticulate = extantTopics.indexOf("/orgs/wd/aqe/particulate") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/particulate/" + serialNumber) >= 0;
  const hasConductivity = extantTopics.indexOf("/orgs/wd/aqe/water/conductivity") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/water/conductivity/" + serialNumber) >= 0;
  const hasPh = extantTopics.indexOf("/orgs/wd/aqe/water/ph") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/water/ph/" + serialNumber) >= 0;
  const hasTurbidity = extantTopics.indexOf("/orgs/wd/aqe/water/turbidity") >= 0 || extantTopics.indexOf("/orgs/wd/aqe/water/turbidity/" + serialNumber) >= 0;
  const has = [hasNO2, hasCO, hasSO2, hasO3, hasParticulate, hasCO2, hasVOC, hasConductivity, hasPh, hasTurbidity].reverse();
  const modelCode = has.reduce((t, v) => {
    return t * 2 + (v ? 1 : 0);
  }, 0);
  // modelCode will be a unique value depending on the combination of bits that are set
  // the bits are in reverse order of the has array so hasNO2 is bit 0...
  // the rule is that 'new' sensor presence variables MUST be added to the end of the 'has' array

  switch (modelCode) {
    case 0b11: return 'model A'; // no2 + co
    case 0b1100: return 'model B'; // so2 + o3
    case 0b10000: return 'model C'; // NOTE: there is actually a conflict between C and N here
    case 0b100000: return 'model D'; // co2
    // case 0b1100000: // NOTE: this is just for data recorded before 3/27/2018
    case 0b1000000: return 'model E'; // voc
    case 0b110000: return 'model G'; // co2 + particulate
    case 0b1001: return 'model J'; // Jerry model no2 + o3
    case 0b10001: return 'model K'; // no2 + particulate
    case 0b10010: return 'model L'; // co + particulate
    case 0b110001: return 'model M'; // co2 + pm + no2
    case 0b1110000: return 'model P'; // co2 + pm + voc
    case 0b10011: return 'model Q'; // pm + co + no2 
    case 0b11001: return 'model R'; // pm + o3 + no2
    case 0b10101: return 'model S'; // pm + so2 + no2
    case 0b11010: return 'model T'; // pm + co + o3
    case 0b10100: return 'model U'; // pm + so2 NOTE: there is actually a conflict between U and Y here
    case 0b1100000: return 'model V'; // co2 + voc
    case 0b1110000000: return 'model W'; // conductivity + pH + turbidity + water temperature    
    default:
      if (modelCode !== 0b0) {
        console.log(`Unexpected Model Code: 0b${modelCode.toString(2)}`);
      }
      return 'model H'; // base model
  }
};

const getRecordLengthByModelType = (modelType, hasPressure, hasBattery) => {
  var additionalFields = 0;
  if(hasPressure){
    additionalFields++;
  }
  if(hasBattery) {
    additionalFields++;
  }
  
  switch (modelType) {
    case 'model A':
      return 10 + additionalFields; // time, temp, hum, no2, no2_raw, co, co_raw, lat, lng, alt + [pressure]
    case 'model B':
      return 10 + additionalFields; // time, temp, hum, so2, so2_raw, o3, o3_raw, lat, lng, alt + [pressure]
    case 'model C':
      return 8 + additionalFields; // time, temp, hum, pm, pm_raw, lat, lng, alt + [pressure]
    case 'model D':
      return 7 + additionalFields; // time, temp, hum, co2, lat, lng, alt + [pressure]
    case 'model E':
      return 9 + additionalFields; // time, temp, hum, eco2, voc, resistance, lat, lng, alt + [pressure]
    case 'model G':
      return 10 + additionalFields; // time, temp, hum, co2, pm1p0, pm2p5, pm10p0, lat, lng, alt + [pressure]
    case 'model J':
      return 11 + additionalFields; // time, temp, hum, no2, no2_raw1, no2_raw2, o3, o3_raw, lat, lng, alt + [pressure]
    case 'model H': // BASE
      return 6 + additionalFields; // time, temp, hum, lat, lng, alt + [pressure]
    case 'model K': // NO2 + PM
      return 11 + additionalFields; // time, temp, hum, no2, no2_raw, pm1p0, pm2p5, pm10p0, lat, lng, alt + [pressure]
    case 'model L': // CO + PM
      return 11 + additionalFields; // time, temp, hum, co, co_raw, pm1p0, pm2p5, pm10p0, lat, lng, alt + [pressure]
    case 'model M': // co2 + pm + no2  
      return 12 + additionalFields; // time, temp, hum, co2, pm1p0, pm2p5, pm10p0, no2, no2_raw, lat, lng, alt + [pressure]
    case 'model N': // pm2 only
      return 9 + additionalFields; // time, temp, hum, pm1p0, pm2p5, pm10p0, lat, lng, alt + [pressure]
    case 'model P': // co2 + pm + voc
      return 13 + additionalFields; // time, temp, hum, co2, pm1p0, pm2p5, pm10p0, eco2, voc, res, lat, lng, alt + [pressure]
    case 'model Q':
      return 13 + additionalFields; // time, temp, hum, pm1p0, pm2p5, pm10p0, co_raw, co, no2, no2_raw, lat, lng, alt + [pressure]
    case 'model R':
      return 13 + additionalFields; // time, temp, hum, pm1p0, pm2p5, pm10p0, o3_raw, o3, no2, no2_raw, lat, lng, alt + [pressure]
    case 'model S':
      return 13 + additionalFields; // time, temp, hum, pm1p0, pm2p5, pm10p0, so2_raw, so2, no2_raw, no2,lat, lng, alt + [pressure]
    case 'model T':
      return 13 + additionalFields; // time, temp, hum, pm1p0, pm2p5, pm10p0, o3_raw, o3, co_raw, co, lat, lng, alt + [pressure]      
    case 'model U': // SO2 (kwj) + PM
      return 11 + additionalFields; // time, temp, hum, so2, so2_raw, pm1p0, pm2p5, pm10p0, lat, lng, alt + [pressure]      
    case 'model V': // CO2 + VOC
      return 10 + additionalFields; // time, temp, hum, co2, eco2, voc, resistance, lat, lng, alt + [pressure]
    case 'model W': 
      return 11 + additionalFields; // time, temp, conductivity, conductivity_raw, turbidity, turbidity_raw, ph, ph_raw, lat, lng, alt + [pressure]
    case 'model Y': // SO2 (alphasense) + PM
      return 12 + additionalFields; // time, temp, hum, so2, so2_raw, so2_raw2, pm1p0, pm2p5, pm10p0, lat, lng, alt + [pressure]            
    case 'model H': // base model
      return 6 + additionalFields;
    default:
      return 6 + additionalFields;
  }
};

const determineTimebase = (dirname, items, uniqueTopics) => {
  let serialNumber = dirname.split("_");
  serialNumber = serialNumber[serialNumber.length - 1];
  let temperatureTopic = null;
  if (uniqueTopics.indexOf("/orgs/wd/aqe/temperature") >= 0) {
    temperatureTopic = "/orgs/wd/aqe/temperature";
  }
  else if (uniqueTopics.indexOf("/orgs/wd/aqe/temperature/" + serialNumber) >= 0) {
    temperatureTopic = "/orgs/wd/aqe/temperature/" + serialNumber;
  }

  if (!temperatureTopic) {
    return null;
  }

  let timeDiffs = [];
  let lastTime = null;

  items.filter((item) => {
    return item.topic === temperatureTopic;
  }).forEach((item) => {
    if (!lastTime) {
      lastTime = moment(item.timestamp);
    }
    else {
      const currentTimestamp = moment(item.timestamp);
      timeDiffs.push(currentTimestamp.diff(lastTime, "milliseconds"));
      lastTime = currentTimestamp;
    }
  });

  // determine the standard deviation of the time differences
  // filter out any that are outside 1 standard deviation from the mean
  const stdev = jStat.stdev(timeDiffs);
  const mean = jStat.mean(timeDiffs);
  // remove outliers
  timeDiffs = timeDiffs.filter((diff) => {
    return (diff >= mean - stdev) && (diff <= mean + stdev);
  });

  // recompute the mean
  return jStat.mean(timeDiffs);
};

const appendHeaderRow = (model, filepath, temperatureUnits, hasPressure, hasBattery) => {
  if (!temperatureUnits) {
    temperatureUnits = "???";
  }

  let headerRow = `timestamp,temperature[${temperatureUnits}],humidity[%],`;

  switch (model) {
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
      headerRow += "eco2[ppm],tvoc[ppb],resistance[ohm]";
      break;
    case "model G":
      headerRow += "co2[ppm],pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3]";
      break;
    case "model J":
      headerRow += "no2[ppb],o3[ppb],no2_we[V],no2_aux[V],o3[V]";
      break;
    case "model K":
      headerRow += "no2[ppb],no2[V],pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3]";
      break;
    case "model L":
      headerRow += "co[ppm],co[V],pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3]";
      break;
    case "model M":
      headerRow += "co2[ppm],pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3],no2[ppb],no2[V]";
      break;
    case "model N":
      headerRow += "pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3]";
      break;
    case "model P":
      headerRow += "co2[ppm],pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3],eco2[ppm],tvoc[ppb],resistance[ohm]";
      break;
    case "model Q":
      headerRow += "pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3],co[ppb],co[V],no2[ppb],no2[V]";
      break;
    case "model R":
      headerRow += "pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3],o3[ppb],o3[V],no2[ppb],no2[V]";
      break;
    case "model S":
      headerRow += "pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3],so2[ppb],so2[V],no2[ppb],no2[V]";
      break;
    case "model T":
      headerRow += "pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3],o3[ppb],o3[V],co[ppm],co[V]";
      break;
    case "model U":
      headerRow += "so2[ppb],so2[V],pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3]";
      break;      
    case "model V":
      headerRow += "co2[ppm],eco2[ppm],tvoc[ppb],resistance[ohm]";
      break;      
    case "model W": 
      headerRow = `timestamp,temperature[${temperatureUnits}],`;
      headerRow += "conductivity[mS/cm], conductivity_raw[V], turbidity[NTU], turbidity_raw[V], ph[n/a], ph_raw[V]";
      break;
    case "model Y":
      headerRow += "so2[ppb],so2_we[V],so2_aux[V],pm1.0[ug/m^3],pm2.5[ug/m^3],pm10.0[ug/m^3]";
      break;            
    case "model H": // base model
      headerRow = headerRow.slice(0, -1); // remove the trailing comma since ther are no additional fields
      break;
    default:
      headerRow = headerRow.slice(0, -1); // remove the trailing comma since ther are no additional fields
      break;
  }

  if (hasPressure) {
    headerRow += ",pressure[Pa]";
  }
  if (hasBattery) {
    headerRow += ",battery[V]";
  }

  headerRow += ",latitude[deg],longitude[deg],altitude[m]\r\n";
  fs.appendFileSync(filepath, headerRow);
};

const getTemperatureUnits = (items) => {
  for (let ii = 0; ii < items.length; ii++) {
    const item = items[ii];
    if (item.topic.indexOf("temperature") >= 0) {
      return item["converted-units"] || item.units || "degC";
    }
  }
};

// possible options for format are 'csv' and 'influx'
// influx makes a record object that looks like
// {
//   measurement: "egg_data",
//   tags: {tag_key: tag_value, ... },
//   fields: {field_key: field_value, ... },
//   timestamp: Date
// }
const convertRecordToString = (record, modelType, hasPressure, hasBattery, utcOffset, tempUnits = 'degC', format = 'csv', rowsWritten = -1, serial = "") => {
  const r = record.slice();
  if (format === 'csv') {
    r[0] = moment(r[0]).utcOffset(utcOffset).format("MM/DD/YYYY HH:mm:ss");
    let num_non_trivial_fields = 0;
    for (let i = 0; i < getRecordLengthByModelType(modelType, hasPressure, hasBattery); i++) {
      if (r[i] === undefined) {
        r[i] = invalid_value_string;
      }
      else {
        num_non_trivial_fields++;
      }
    }

    if (num_non_trivial_fields > 1) {
      return r.join(",") + "\r\n";
    }
    else {
      return "";
    }
  }
  else if (format === 'influx') {
    if (!modelType || modelType.indexOf("unknown") >= 0) {
      modelType = "unknown"; //could have no2 or so2 in it otherwise
    }

    const modelInfluxFieldsMap = {
      "model A": ["", "temperature", "humidity", "no2", "co", "no2_raw", "co_raw", "latitude", "longitude", "altitude"],
      "model B": ["", "temperature", "humidity", "so2", "o3", "so2_raw", "o3_raw", "latitude", "longitude", "altitude"],
      "model C": ["", "temperature", "humidity", "particulate", "particulate_raw", "latitude", "longitude", "altitude"],
      "model D": ["", "temperature", "humidity", "co2", "latitude", "longitude", "altitude"],
      "model E": ["", "temperature", "humidity", "eco2|co2", "voc", "voc_raw", "latitude", "longitude", "altitude"],
      "model G": ["", "temperature", "humidity", "co2", "pm1p0", "pm2p5", "pm10p0", "latitude", "longitude", "altitude"],
      "model H": ["", "temperature", "humidity", "latitude", "longitude", "altitude"],
      "model J": ["", "temperature", "humidity", "no2", "o3", "no2_raw1", "no2_raw2", "o3_raw", "latitude", "longitude", "altitude"],
      "model K": ["", "temperature", "humidity", "no2", "no2_raw", "pm1p0", "pm2p5", "pm10p0", "latitude", "longitude", "altitude"],
      "model L": ["", "temperature", "humidity", "co", "co_raw", "pm1p0", "pm2p5", "pm10p0", "latitude", "longitude", "altitude"],
      "model M": ["", "temperature", "humidity", "co2", "pm1p0", "pm2p5", "pm10p0", "no2", "no2_raw", "latitude", "longitude", "altitude"],
      "model N": ["", "temperature", "humidity", "pm1p0", "pm2p5", "pm10p0", "latitude", "longitude", "altitude"],
      "model P": ["", "temperature", "humidity", "co2", "pm1p0", "pm2p5", "pm10p0", "eco2|co2", "voc", "voc_raw", "latitude", "longitude", "altitude"],
      "model Q": ["", "temperature", "humidity", "pm1p0", "pm2p5", "pm10p0", "o3", "o3_raw", "no2", "no2_raw", "latitude", "longitude", "altitude"],
      "model R": ["", "temperature", "humidity", "pm1p0", "pm2p5", "pm10p0", "co", "co_raw", "no2", "no2_raw", "latitude", "longitude", "altitude"],
      "model S": ["", "temperature", "humidity", "pm1p0", "pm2p5", "pm10p0", "so2", "so2_raw", "no2", "no2_raw", "latitude", "longitude", "altitude"],
      "model T": ["", "temperature", "humidity", "pm1p0", "pm2p5", "pm10p0", "o3", "o3_raw", "co", "co_raw", "latitude", "longitude", "altitude"],
      "model U": ["", "temperature", "humidity", "so2", "so2_raw", "pm1p0", "pm2p5", "pm10p0", "latitude", "longitude", "altitude"],      
      "model V": ["", "temperature", "humidity", "co2", "eco2", "voc", "voc_raw", "latitude", "longitude", "altitude"],      
      "model W": ["", "temperature", "conductivity", "conductivity_raw", "turbidity", "turbidity_raw", "ph", "ph_raw", "latitude", "longitude", "altitude"],
      "model Y": ["", "temperature", "humidity", "so2", "so2_raw", "so2_raw2", "pm1p0", "pm2p5", "pm10p0", "latitude", "longitude", "altitude"],
      "unknown": ["", "temperature", "humidity", "latitude", "longitude", "altitude"]
    };

    const modelInfluxTagsMap = {
      "model A": ["", tempUnits, "%", "ppb", "ppm", "V", "V", "deg", "deg", "m"],
      "model B": ["", tempUnits, "%", "ppb", "ppb", "V", "V", "deg", "deg", "m"],
      "model C": ["", tempUnits, "%", "ug/m^3", "V", "deg", "deg", "m"],
      "model D": ["", tempUnits, "%", "ppm", "deg", "deg", "m"],
      "model E": ["", tempUnits, "%", "ppm", "ppb", "ohms", "deg", "deg", "m"],
      "model G": ["", tempUnits, "%", "ppm", "ug/m^3", "ug/m^3", "ug/m^3", "deg", "deg", "m"],
      "model H": ["", tempUnits, "%", "deg", "deg", "m"],
      "model J": ["", tempUnits, "%", "ppb", "ppb", "V", "V", "V", "deg", "deg", "m"],
      "model K": ["", tempUnits, "%", "ppb", "V", "ug/m^3", "ug/m^3", "ug/m^3", "deg", "deg", "m"],
      "model L": ["", tempUnits, "%", "ppm", "V", "ug/m^3", "ug/m^3", "ug/m^3", "deg", "deg", "m"],
      "model M": ["", tempUnits, "%", "ppm", "ug/m^3", "ug/m^3", "ug/m^3", "ppb", "V", "deg", "deg", "m"],
      "model N": ["", tempUnits, "%", "ug/m^3", "ug/m^3", "ug/m^3", "deg", "deg", "m"],
      "model P": ["", tempUnits, "%", "ppm", "ug/m^3", "ug/m^3", "ug/m^3", "ppm", "ppb", "ohms", "deg", "deg", "m"],
      "model Q": ["", tempUnits, "%", "ug/m^3", "ug/m^3", "ug/m^3", "ppm", "V", "ppb", "V", "deg", "deg", "m"],
      "model R": ["", tempUnits, "%", "ug/m^3", "ug/m^3", "ug/m^3", "ppb", "V", "ppb", "V", "deg", "deg", "m"],
      "model S": ["", tempUnits, "%", "ug/m^3", "ug/m^3", "ug/m^3", "ppb", "V", "ppb", "V", "deg", "deg", "m"],
      "model T": ["", tempUnits, "%", "ug/m^3", "ug/m^3", "ug/m^3", "ppb", "V", "ppm", "V", "deg", "deg", "m"],
      "model U": ["", tempUnits, "%", "ppb", "V", "ug/m^3", "ug/m^3", "ug/m^3", "deg", "deg", "m"],      
      "model V": ["", tempUnits, "%", "ppm", "ppm", "ppb", "ohms", "deg", "deg", "m"],
      "model W": ["", tempUnits, "mS/cm", "V", "NTU", "V", "n/a", "V", "deg", "deg", "m"],
      "model Y": ["", tempUnits, "%", "ppb", "V", "V", "ug/m^3", "ug/m^3", "ug/m^3", "deg", "deg", "m"],      
      "unknown": ["", tempUnits, "%", "deg", "deg", "m"]
    };

    if (hasPressure) {
      // pressure is always going to be assumed to come right before the GPS data
      // if we have pressure, add it in at the expected record offset
      Object.keys(modelInfluxFieldsMap).forEach((key) => {
        modelInfluxFieldsMap[key].splice(modelInfluxFieldsMap[key].length - 3, 0, "pressure");
      });

      Object.keys(modelInfluxTagsMap).forEach((key) => {
        modelInfluxTagsMap[key].splice(modelInfluxTagsMap[key].length - 3, 0, "Pa");
      });
    }

    if (hasBattery) {
      // battery is always going to be assumed to come right before the GPS data (and after pressure)
      // if we have battery, add it in at the expected record offset
      Object.keys(modelInfluxFieldsMap).forEach((key) => {
        modelInfluxFieldsMap[key].splice(modelInfluxFieldsMap[key].length - 3, 0, "battery");
      });

      Object.keys(modelInfluxTagsMap).forEach((key) => {
        modelInfluxTagsMap[key].splice(modelInfluxTagsMap[key].length - 3, 0, "V");
      });
    }    

    if (moment(r[0]).isValid()) {
      const influxRecord = {
        measurement: 'egg_data',
        tags: {},
        fields: {},
        timestamp: moment(r[0]).toDate()
      };

      let num_non_trivial_fields = 0;
      for (let i = 1; i < getRecordLengthByModelType(modelType, hasPressure, hasBattery); i++) {
        if (r[i] !== undefined) {
          num_non_trivial_fields++;
          let fields = modelInfluxFieldsMap[modelType][i];
          fields = fields ? fields.split('|') : []; // field becomes an array no matter what
          const tag = modelInfluxTagsMap[modelType][i];
          if (fields.length) {
            for (let j = 0; j < fields.length; j++) {
              const field = fields[j];
              influxRecord.fields[field] = influxRecord.fields[field] || +r[i];
              if (tag) {
                influxRecord.tags[field + '_units'] = tag;
              }
            }
          }
        }
      }

      if (num_non_trivial_fields > 0) {
        if (serial) {
          influxRecord.tags.serial_number = serial;
        }

        if (rowsWritten > 0) {
          return "," + JSON.stringify(influxRecord);
        }
        else {
          return JSON.stringify(influxRecord);
        }
      }
    }
  }

  return ""; // if nothing else, still return a blank string
};

queue.process('stitch', 3, (job, done) => {
  // the download job is going to need the following parameters
  //    save_path - the full path to where the result should be saved
  //    user_id   - the user id that made the request
  //    email     - the email address that should be notified on zip completed
  //    sequence  - the sequence number within this request chain

  const skipJob = job.data.bypassjobs && (job.data.bypassjobs.indexOf('stitch') >= 0);
  if (!skipJob) {
    // 1. for each folder in save_path, create an empty csv file with the same name
    const dir = job.data.serials[0];
    let extension = 'csv';
    if (job.data.stitch_format) {
      switch (job.data.stitch_format) {
        case 'influx':
          extension = 'json';
          break;
      }
    }

    fs.closeSync(fs.openSync(`${job.data.save_path}/${dir}.${extension}`, 'w'));

    // 2. for each folder in save_path, analyze file all the "n.json" to infer the type of Egg
    //    model, generate an appropriate header row and append it to the csv file, and
    //    save the egg model in a variable for later dependencies, and establish the
    //    time base of the data in the file. In order to determine the time base,
    //    analyze the time differences between adjacent messages on the temperature
    //    topic (which all Eggs have)

    //// **********
    let uniqueTopics = {};
    let modelType = null;
    let temperatureUnits = null;
    let hasPressure = false;
    let hasBattery = false;
    const temperatureItems = []; // for the benefit of determineTimebase
    let rowsWritten = 0;
    let totalMessages = 0;
    let messagesProcessed = 0;
    let currentRecord = [];
    let timeBase = 0;
    let currentTemperatureUnits = 'degC';
    let firstPassAllFiles = fs.readdirSync(`${job.data.save_path}/${dir}/`)
      .sort((a, b) => {
        return fs.statSync(`${job.data.save_path}/${dir}/${a}`).mtime.getTime() -
          fs.statSync(`${job.data.save_path}/${dir}/${b}`).mtime.getTime();
      });
    let allFiles = firstPassAllFiles.slice();

    console.log(`Listed files`, allFiles);

    let stop_working = false;

    return promiseDoWhilst(() => {
      // do this
      return new Promise((resolve, reject) => {
        const currentFile = firstPassAllFiles.shift();
        let serialNumber = dir.split("_");
        serialNumber = serialNumber[serialNumber.length - 1]; // the last part of the dirname

        // console.log(`currentFile: ${currentFile}, serialNumber: ${serialNumber}`);

        if (currentFile) {
          // operate on the current file
          let items = null;
          try {
            items = require(`${job.data.save_path}/${dir}/${currentFile}`);
          }
          catch (error) {

            if (extension === 'csv') {
              fs.appendFileSync(`${job.data.save_path}/${dir}.${extension}`, `No data found for ${serialNumber}. Please check that the Serial Number is accurate`);
            }
            else if (extension === 'json') {
              fs.appendFileSync(`${job.data.save_path}/${dir}.${extension}`, '[]'); // nothing to see here
            }

            stop_working = true;
            generateNextJob(job);
            done();
            allFiles = [];
            firstPassAllFiles = [];
            resolve();
            return;
          }

          // if 1.json has zero records, but it exists, that's also a problem we should not continue within
          let sn = "";
          if (currentFile === '1.json' && (!items || (items.length === 0))) {
            sn = dir.split("_");
            sn = sn[sn.length - 1]; // the last part of the dirname

            if (extension === 'csv') {
              fs.appendFileSync(`${job.data.save_path}/${dir}.${extension}`, `No data found for ${sn}. Please check the time period you requested is accurate`);
            }
            else if (extension === 'json') {
              fs.appendFileSync(`${job.data.save_path}/${dir}.${extension}`, '[]'); // nothing to see here
            }

            stop_working = true;
            generateNextJob(job);
            done();
            resolve();
            allFiles = [];
            firstPassAllFiles = [];
            return;
          }

          totalMessages += items.length;
          items.forEach((item) => {
            uniqueTopics[item.topic] = 1;
            if (item.topic.indexOf("temperature") >= 0) {
              temperatureItems.push(item);
            }
            if (item.topic.indexOf("pressure") >= 0) {
              hasPressure = true;
            }
            if (item.topic.indexOf("battery") >= 0) {
              hasBattery = true;
            }            
            uniqueTopics[item.topic] = 1;
            temperatureUnits = temperatureUnits || getTemperatureUnits([item]);
          });
        }
        else {
          if (extension === 'csv') {
            fs.appendFileSync(`${job.data.save_path}/${dir}.${extension}`, `No data found for ${sn}. Please check that the Serial Number is accurate`);
          }
          else if (extension === 'json') {
            fs.appendFileSync(`${job.data.save_path}/${dir}.${extension}`, '[]'); // nothing to see here
          }
        }
        resolve();
      });
    }, () => {
      // repeate as long as this is true
      return firstPassAllFiles.length > 0;
    }).then(() => {
      if (stop_working) {
        return;
      }

      return new Promise((resolve, reject) => {
        uniqueTopics = Object.keys(uniqueTopics);
        console.log(`Total messages: ${totalMessages}`);
        job.log(`uniqueTopics: `, uniqueTopics);
        modelType = getEggModelType(dir, uniqueTopics);
        console.log(`Egg Serial Number ${dir} is ${modelType} type`);
        job.log(`Egg Serial Number ${dir} is ${modelType} type`);

        timeBase = determineTimebase(dir, temperatureItems, uniqueTopics);
        job.log(`Egg Serial Number ${dir} has timebase of ${timeBase} ms`);
        resolve();
      });
    }).then(() => {
      if (stop_working) {
        return;
      }

      if (allFiles.length > 0) {
        console.log("Starting main loop for Job");
        return promiseDoWhilst(() => {
          // do this action...
          const filename = allFiles.shift();

          const fullPathToFile = `${job.data.save_path}/${dir}/${filename}`;
          const data = require(fullPathToFile);
          let index = 0;

          modelType = refineModelType(modelType, data);
          console.log(`Egg Serial Number ${dir} is ${modelType} type (refined)`);
          job.log(`Egg Serial Number ${dir} is ${modelType} type (refined)`);

          if (extension === 'csv') {
            appendHeaderRow(modelType, `${job.data.save_path}/${dir}.csv`, temperatureUnits, hasPressure, hasBattery);
          }
          else if (extension === 'json' && (totalMessages > 0)) {
            fs.appendFileSync(`${job.data.save_path}/${dir}.json`, '['); // it's going to be an array of objects
          }

          if (data.length > 0) {
            return promiseDoWhilst(() => {
              // do this action...
              return new Promise((resolve, reject) => {
                const res = resolve;
                const rej = reject;
                setTimeout(() => {
                  try {
                    const datum = data.shift();
                    // console.log(datum, index, currentRecord, modelType);

                    if (index === 0) {
                      // special case, use this timestamp
                      const natural_topic = datum.topic.replace(`/${datum['serial-number']}`, '');
                      if (known_topic_prefixes.indexOf(natural_topic) >= 0 && (currentRecord[0] === undefined)) {
                        currentRecord[0] = datum.timestamp;
                      }
                    }

                    const timeToPreviousMessage = moment(datum.timestamp).diff(currentRecord[0], "milliseconds");

                    // if datum falls within current record, then just add it
                    if (timeToPreviousMessage < timeBase / 2) {
                      if (datum.topic.indexOf("temperature") >= 0) {
                        currentTemperatureUnits = datum["converted-units"];
                      }
                      addMessageToRecord(datum, modelType, job.data.compensated, job.data.instantaneous, currentRecord, hasPressure, hasBattery);
                      // console.log(datum, currentRecord);
                    }
                    // if it doesn't, then append the stringified current record to the csv file
                    // then reset the current record
                    // and add this datum to it, and use it's timestamp
                    else {
                      // if the record is non-trivial, add it
                      if (extension === 'csv') {
                        fs.appendFileSync(`${job.data.save_path}/${dir}.csv`, convertRecordToString(currentRecord, modelType, hasPressure, hasBattery, job.data.utcOffset));
                        // console.log(currentRecord, convertRecordToString(currentRecord, modelType, job.data.utcOffset));
                      }
                      else if (job.data.stitch_format === 'influx') {
                        fs.appendFileSync(`${job.data.save_path}/${dir}.json`, convertRecordToString(currentRecord, modelType, hasPressure, hasBattery, job.data.utcOffset, currentTemperatureUnits, 'influx', rowsWritten, job.data.serials[0]));
                      }
                      rowsWritten++;

                      currentRecord = [];
                      currentRecord[0] = datum.timestamp;
                      addMessageToRecord(datum, modelType, job.data.compensated, job.data.instantaneous, currentRecord, hasPressure, hasBattery);
                      // console.log(datum, currentRecord);
                    }
                    messagesProcessed++;
                    job.progress(messagesProcessed, totalMessages);
                    console.log(messagesProcessed, totalMessages);
                    index++;
                    // console.log("Resolving promise...");
                    res();
                  }
                  catch (err) {
                    console.log(err.message, err.stack);
                    rej(err);
                  }
                }, 0);
              });
            }, () => {
              // ... while this condition is true
              return data.length > 0;
            });
          }
          else {
            return;
          }

        }, () => {
          // ... while this condition is true
          return allFiles.length > 0;
        }).then(() => {
          job.log(`Finished processing all JSON files, committing last record`);
          // make sure to commit the last record to file in whatever state it's in
          if (extension === 'csv') {
            fs.appendFileSync(`${job.data.save_path}/${dir}.csv`, convertRecordToString(currentRecord, modelType, hasPressure, hasBattery, job.data.utcOffset));
          }
          else if (job.data.stitch_format === 'influx') {
            if (totalMessages > 0) {
              fs.appendFileSync(`${job.data.save_path}/${dir}.json`, convertRecordToString(currentRecord, modelType, hasPressure, hasBattery, job.data.utcOffset, temperatureUnits, 'influx', rowsWritten, job.data.serials[0]));
              fs.appendFileSync(`${job.data.save_path}/${dir}.json`, ']'); // end the array
            }
          }
          rowsWritten++;
        }).then(() => { // job is complete
          job.log(`Generating next job after doing work...`);
          stop_working = true;
          generateNextJob(job);
          job.log(`About to call done.`);
          done();
          console.log(`Completing main loop.`);
        }).catch((err) => { // something went badly wrong
          job.log(`Error occured... ${err.message}`);
          console.log(err.message, err.stack);
          stop_working = true;
          generateNextJob(job);
          done(err);
        });
      }
      else { // no files
        job.log(`Generating next job because of no work to do...`);
        stop_working = true;
        generateNextJob(job);
        done();
      }
    })
      .catch((err) => {
        job.log(`Error occured... ${err.message}`);
        console.log(err.message, err.stack);
        stop_working = true;
        generateNextJob(job);
        done(err);
      });
  }
  else { // skip job
    job.log(`Generating next job because skipped...`);
    stop_working = true;
    generateNextJob(job);
    done();
  }
});

const generateNextJob = (job) => {
  const serials = job.data.serials.slice(1);
  if (serials.length > 0) {
    const job2 = queue.create('stitch', {
      title: 'stitching data for ' + serials[0],
      save_path: job.data.save_path,
      original_serials: job.data.original_serials.slice(),
      original_url: job.data.original_url,
      serials: serials,
      user_id: job.data.user_id,
      email: job.data.email,
      compensated: job.data.compensated,
      instantaneous: job.data.instantaneous,
      utcOffset: job.data.utcOffset,
      zipfilename: job.data.zipfilename,
      bypassjobs: job.data.bypassjobs ? job.data.bypassjobs.slice() : []
    })
      .priority('high')
      .attempts(1)
      .save();
  }
  else {
    // before declaring we are done, create a job to zip the results up
    const job2 = queue.create('zip', {
      title: 'zipping folder ' + job.data.save_path,
      serials: serials,
      original_serials: job.data.original_serials.slice(),
      original_url: job.data.original_url,
      save_path: job.data.save_path,
      user_id: job.data.user_id,
      email: job.data.email,
      zipfilename: job.data.zipfilename,
      bypassjobs: job.data.bypassjobs ? job.data.bypassjobs.slice() : []
    })
      .priority('high')
      .attempts(1)
      .save();
  }
};

process.once('uncaughtException', function (err) {
  console.error('Something bad happened: ', err.message, err.stack);
  queue.shutdown(1000, function (err2) {
    console.error('Kue shutdown result: ', err2 || 'OK');
    process.exit(0);
  });
});
