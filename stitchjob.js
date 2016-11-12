var kue = require('kue')
  , queue = kue.createQueue();
var fs = require('fs');
path = require('path');

let getDirectories = (srcpath) => {
  return fs.readdirSync(srcpath).filter( (file) => {
    return fs.statSync(path.join(srcpath, file)).isDirectory();
  });
}


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

  // 2. for each folder in save_path, analyze file 1.json to infer the type of Egg
  //    model, generate an appropriate header row and append it to the csv file, 
  //    and create a map of topic string to CSV index based on the determined model
  

  // 3. for each folder in save_path, list the files in that folder
  //    load them into memory one at a time, and process records in each file
  //    generating one csv record at a time and appending to the csv file
  //    progressively as you go
  

  done();
});

process.once( 'uncaughtException', function(err){
  console.error( 'Something bad happened: ', err );
  queue.shutdown( 1000, function(err2){
    console.error( 'Kue shutdown result: ', err2 || 'OK' );
    process.exit( 0 );
  });
});
