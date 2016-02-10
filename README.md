Folders
=============

This node.js package implements the folders.io synthetic file system.

This Folders Module is based on a web hdfs,
Module can be installed via "npm install folders-hdfs".


### Installation 


To install 'folders-hdfs' 

Installation (use --save to save to package.json)

```sh
npm install folders-hdfs
```


Basic Usage


### Constructor

Constructor, could pass the special option/param in the config param.

```js

var FoldersHdfs = require('folders-hdfs');

var prefix = '/http_window.io_0:webhdfs/';
var config = {
    // the base url address for hdfs instance
    baseurl : "http://webhdfs.node/webhdfs/v1/data/",

    // the username to access the hdfs instances
    username : 'hdfs'
};

var hdfs = new FoldersHdfs(prefix, config);

```


###ls
list folders/files

```js
/**
 * @param uri or query param, 
 * the uri on hdfs to ls
 *    eg, '/', '/sub-folder1', 
 *    or dir start with {prefix}, '/http_window.io_0:webhdfs/sub-folder1'
 * the param object
 *   {
 *      path:  uri,
 *      offset: 0, 
 *      length: 10
 *   }
 * @param cb, callback function(err, result) function. 
 *    result will be a file info array. [{}, ... {}]
 *    a example file information
 *   { 
 *      name: 'emptysubfolder',
 *      fullPath: 'emptysubfolder',
 *      uri: '/http_window.io_0:webhdfs/emptysubfolder',
 *      size: 510,
 *      extension: '+folder',
 *      type: '',
 *      modificationTime: 1452527809825 
 *    }
 * 
 * ls(uri,cb)
 */
 
hdfs.ls('.', function(err, result) {
      
      if (err){
        // err handling
      }
      
      console.log("Folder listing", result);
});
```


###cat
Read a file

```js

/**
 * @param uri, the file uri to cat 
 * @param cb, callback  function(err, result) function.
 *    example for result.
 *    {
 *      stream: .., // a readable 'request' stream
 *      size : .. , // file size
 *      name: path
 *    }
 *
 * cat(uri,cb) 
 */

hdfs.cat('path/to/file', function(err,result) {
      
      if (err){
        // err handling
      }
      
      console.log("Read Stream ", result.stream);
});
```

### write
Write a file

```js

/**
 * @param path, string, the path 
 * @param data, the input data, 'stream.Readable' or 'Buffer'
 * @param cb, the callback function
 * write(path,data,cb)
 */

var writeData = getWriteStreamSomeHow('some_movie.mp4');

hdfs.write('path/to/file',writeData, function(err,result) {
      
      if (err){
        // err handling
      }
      
      console.log("Write status ", result);
});
```


###unlink
Delete a File

```js

/**
 * @param uri, the file uri to delete 
 * @param cb, callback  function(err, result) function.
 *    example for result. {"boolean":true}
 *
 * unlink(uri,cb) 
 */

hdfs.cat('path/to/file', function(err,result) {
      
      if (err){
        // err handling
      }
      
      console.log("Read Stream ", result.stream);
});
```
