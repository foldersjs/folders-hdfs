Folders
=============

This node.js package implements the folders.io synthetic file system.

This Folders Module is based on a web hdfs,
Module can be installed via "npm install folders-hdfs".


### Installation 


To install 'folders-ftp' 

Installation (use --save to save to package.json)

```sh
npm install folders-ftp
```


Basic Usage


### Constructor

Constructor, could pass the special option/param in the config param.

```js

var FoldersHdfs = require('folders-hdfs');


var config = {
    // the base url address for hdfs instance
    baseurl : "http://webhdfs.node/webhdfs/v1/data/",

    // the username to access the hdfs instances
    username : 'hdfs'
};

var hdfs = new FoldersHdfs("localhost-hdfs", config);

```


###ls

```js
/**
 * @param uri, the uri on ftp server to ls
 * @param cb, callback function. 
 * ls(uri,cb)
 */
 
hdfs.ls('.', function(err,data) {
        console.log("Folder listing", data);
});
```


###cat


```js

/**
 * @param uri, the file uri to cat 
 * @param cb, callback function.
 * cat(uri,cb) 
 */

hdfs.cat('path/to/file', function(err,result) {
        console.log("Read Stream ", result.stream);
});
```

### write

```js

/**
 * @param path, string, the path 
 * @param data, the input data, 'stream.Readable' or 'Buffer'
 * @param cb, the callback function
 * write(path,data,cb)
 */

var writeData = getWriteStreamSomeHow('some_movie.mp4');

hdfs.write('path/to/file',writeData, function(err,result) {
        console.log("Write status ", result);
});
```