var fs = require('fs');
var path = require('path');        
var kue = require('kue')
  , queue = kue.createQueue();

let job = {
  data: {
    original_serials: ["egg00802ebeec080120AH"],
    original_url: "blah",
    save_path : "/home/mitesh/dev/KueStitchJob/tmp",
    user_id: "blah",
    email: "blah",
    utcOffset: -4,
    compensated: true,
    instantaneous: false,
    zipfilename: "blah",
    bypassjobs: [],
    stitch_format: "csv"
  }
};
                   
        let getDirectories = (srcpath) => {
          return fs.readdirSync(srcpath).filter( (file) => {
            return fs.statSync(path.join(srcpath, file)).isDirectory();
          });
        }

        let directories = getDirectories(job.data.save_path);

        let job2 = queue.create('stitch', {
            title: 'stitching data for ' + job.data.original_serials[0]
          , save_path: job.data.save_path
          , original_serials: job.data.original_serials.slice()
          , original_url: job.data.original_url
          , serials: directories
          , user_id: job.data.user_id
          , email: job.data.email
          , compensated: job.data.compensated
          , instantaneous: job.data.instantaneous
          , utcOffset: job.data.utcOffset
          , zipfilename: job.data.zipfilename
          , bypassjobs: job.data.bypassjobs ? job.data.bypassjobs.slice() : []
          , stitch_format: job.data.stitch_format
        })
        .priority('high')
        .attempts(1)
        .save();

process.once( 'uncaughtException', function(err){
  console.error( 'Something bad happened: ', err );
  queue.shutdown( 1000, function(err2){
    console.error( 'Kue shutdown result: ', err2 || 'OK' );
    process.exit( 0 );
  });
});
