var fs = require('fs');

// A test case show ls/create/cat/delete file.
module.exports = function testFoldersHdfs(hdfs, testFolder, testFile, callback) {
    var testFilePath = testFolder + testFile;
    // Step 1: test ls the root path
    console.log("step 1: ls ,", testFolder);
    hdfs.ls(testFolder, function cb(error, files) {
	if (error) {
	    console.log("error in ls directory/files");
	    console.log(error);
	    return callback(error);
	}

	console.log("hdfs result for ls /, ", files, '\n');

	// Step 2: test write file
	console.log("step 2: write, ", testFilePath);
	var stream = fs.createReadStream('dat/test.txt');
	hdfs.write(testFilePath, stream, function cb(error, result) {
	    if (error) {
		console.log("error in write file");
		console.log(error);
		return callback(error);
	    }

	    console.log("result for write, ", result, '\n');

	    // Step 3: test cat file
	    console.log("step 3: cat, ", testFilePath);
	    hdfs.cat(testFilePath, function cb(error, results) {
		if (error) {
		    console.log("error in cat file");
		    console.log(error);
		    return callback(error);
		}

		console.log("results for cat,", results.name, results.size,
			'\n');
		// var stream = results.stream;
		// stream.on('readable', function() {
		// var chunk;
		// var decoder = new StringDecoder('utf8');
		// while (null !== (chunk = stream.read())) {
		// console.log('\ngot %d bytes of data', chunk.length);
		// // var strdata = decoder.write(chunk);
		// // console.log('data:\n+' + strdata);
		// }
		// done();
		// });

		// Step 4 : test delete/unlinke files
		console.log("step 4: unlink,", testFilePath);
		hdfs.unlink(testFilePath, function cb(error, result) {
		    if (error) {
			console.log("error in unlink directory/files");
			console.log(error);
			return callback(error);
		    }

		    console.log("result for unlink,", result, '\n');
		    // console.log(files);
		    return callback();
		});
	    });

	});

    });
};
