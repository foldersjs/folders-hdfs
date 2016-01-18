var FoldersHdfs = new require('../folders-hdfs');
var testFoldersHdfs = new require('./test-folders-hdfs');

var prefix = '/http_window.io_0:webhdfs/'
var url = "http://45.55.223.28/webhdfs/v1/data/";
var hdfs = new FoldersHdfs(prefix, {
    baseurl : url,
    username : 'hdfs'
});

var testFilePath = '/test.txt';
testFoldersHdfs(hdfs, '/', 'test.txt', function(error) {
    if (error) {
	console.warning('test hdfs for folder error, ', '/');
	return;
    }

    console.log('test sub folders, ', '/folder1/');
    testFoldersHdfs(hdfs, '/folder1/', 'test.txt', function(error) {
	if (error) {
	    console.warning('test hdfs for folder error, ', '/');
	    return;
	}

	console.log("test success");
    });

})