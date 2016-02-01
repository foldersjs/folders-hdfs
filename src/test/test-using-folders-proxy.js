var fs = require('fs');
var FoldersHdfs = new require('../folders-hdfs');
var testFoldersHdfs = new require('./test-folders-hdfs');

var WebHDFSProxy = require('webhdfs-proxy');
var FoldersStorageHandler = require('../embedded-folders-based-proxy.js');
var backendPrefix = '/http_window.io_0:webhdfs/';
var backendUrl = "http://45.55.223.28/webhdfs/v1/data/";
var backendFolder = new FoldersHdfs(backendPrefix, {
  baseurl : backendUrl,
  username : 'hdfs'
});
var hdfsStorageHandler = new FoldersStorageHandler(backendFolder);
console.log('hdfsStorageHandler,', hdfsStorageHandler);

var prefix = '/http_window.io_0:webhdfs/';
var PORT = 40050;

WebHDFSProxy.createServer({
  path : '/webhdfs/v1',
  validate : true,

  http : {
    port : PORT
  }
}, hdfsStorageHandler.storageHandler(), function onServerCreate(err, servers) {
  if (err) {
    console.log('WebHDFS proxy server was not created: ' + err.message);
    return;
  }
  console.log('WebHDFS proxy server was created success. ');

  var url = "http://localhost:" + PORT + "/webhdfs/v1/";
  var hdfs = new FoldersHdfs(prefix, {
    baseurl : url
  });
  console.log('hdfs,', hdfs);
  console.log('backendFolder,', backendFolder);

  // begin to show test case.
  testFoldersHdfs(hdfs, '/', 'test.txt', function(error) {
    if (error) {
      console.warning('test hdfs for folder error, ', '/');
      return;
    }

    console.log("test success");
  });
});
