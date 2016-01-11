var fs = require('fs');

var FoldersHdfs = new require('../folders-hdfs');

var prefix = '/http_window.io_0:webhdfs/'
var url = "http://45.55.223.28/webhdfs/v1/data/";
var hdfs = new FoldersHdfs(prefix,{
	baseurl:url,
	username:'hdfs'
});

// test ls the root path
hdfs.ls('/', function cb(error, files) {
	if (error) {
		console.log("error in ls directory/files");
		console.log(error);
		return;
	}
	
	console.log("hdfs result for ls /, files length: ",files.length);
	//console.log(files);
});

// hdfs.meta(url, function cb(files) {
// console.log("hdfs result for ls / ");
// console.log(files);
// });

// test write file
var stream = fs.createReadStream('dat/test.txt');
hdfs.write(
//	{
//	uri : "/test.txt",
//	shareId : "test-share-id",
//	streamId : "test-stream-id",
//	data : stream}
		"/test.txt", stream, function cb(error, result) {
	if (error) {
		console.log("error in write file");
		console.log(error);
		return;
	}

	console.log("hdfs result for write / ");
	console.log(result);
});

//test cat file
hdfs.cat(
		"/test.txt"
//		{
//	shareId : "test-share-id",
//	data : {
//		fileId : url + "data/test.txt"
//	}
//}
	, function cb(error, results) {
	if (error) {
		console.log("error in cat file");
		console.log(error);
		return;
	}

	//console.log("\nresults,", results.name, results.size);
	var stream = results.stream;
	stream.on('readable', function() {
		var chunk;
		var decoder = new StringDecoder('utf8');
		while (null !== (chunk = stream.read())) {
			console.log('\ngot %d bytes of data', chunk.length);
			// var strdata = decoder.write(chunk);
			// console.log('data:\n+' + strdata);
		}
		done();
	});
});