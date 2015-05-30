var fs = require('fs');

var FoldersHdfs = new require('../folders-hdfs');

var url = "http://45.55.223.28/webhdfs/v1/data/";
var hdfs = new FoldersHdfs('folders-hdfs1',{
	baseurl:url,
	username:'hdfs'
});

// test ls the root path
hdfs.ls('/', function cb(files,error) {
	if (error) {
		console.log("error in ls directory/files");
		console.log(error);
		return;
	}
	
	console.log("hdfs result for ls / ");
	console.log(files);
});

// hdfs.meta(url, function cb(files) {
// console.log("hdfs result for ls / ");
// console.log(files);
// });

// test write file
var stream = fs.createReadStream('dat/test.txt');
console.log("input stream");
console.log(stream);
hdfs.write({
	uri : url + "data/test.txt",
	shareId : "test-share-id",
	streamId : "test-stream-id",
	data : stream
}, function cb(result, error) {
	if (error) {
		console.log("error in write file");
		console.log(error);
		return;
	}

	console.log("hdfs result for write / ");
	console.log(result);
});

// test cat file
hdfs.cat({
	shareId : "test-share-id",
	data : {
		fileId : url + "data/test.txt"
	}
}, function cb(results, error) {
	if (error) {
		console.log("error in cat file");
		console.log(error);
		return;
	}

	console.log("\ncat result:");
	console.log(results);
	console.log("\nfile data");
	var stream = results.data;
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