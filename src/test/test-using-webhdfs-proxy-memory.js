var fs = require('fs');
var FoldersHdfs = new require('../folders-hdfs');
var testFoldersHdfs = new require('./test-folders-hdfs');

var WebHDFSProxy = require('webhdfs-proxy');
var memoryStorageHandler = require('webhdfs-proxy-memory');

var prefix = '/http_window.io_0:webhdfs/';
var PORT = 40050;
var url = "http://localhost:" + PORT + "/webhdfs/v1/";
var hdfs = new FoldersHdfs(prefix, {
    baseurl : url
});

WebHDFSProxy.createServer({
    path : '/webhdfs/v1',
    validate : true,

    http : {
	port : PORT
    }
// ,https: {
// port: 443,
// key: '/path/to/key',
// cert: '/path/to/cert'
// }
}, memoryStorageHandler, function onServerCreate(err, servers) {
    if (err) {
	console.log('WebHDFS proxy server was not created: ' + err.message);
	return;
    }
    console.log('WebHDFS proxy server was created success. ');

    // begin to show test case.
    testFoldersHdfs(hdfs, '/', 'test.txt', function(error) {
	if (error) {
	    console.warning('test hdfs for folder error, ', '/');
	    return;
	}

	console.log("test success");
    });
});
