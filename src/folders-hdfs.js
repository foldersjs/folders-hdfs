// Folders.io connector to WebHDFS
var request = require('request');
var uriParse = require('url');
var assert = require('assert');

var baseurl;
//TODO we may want to pass the host, port, username as the param of inin
var FoldersHdfs = function(prefix, options) {

	assert.equal(typeof (options), 'object', 
			"argument 'options' must be a object");

	this.prefix = prefix || "/http_window.io_0:hdfs/";
	this.configure(options);
};

// The web hdfs operation support
var WebHdfsOp = {
	LIST:"LISTSTATUS",
	DIRECTORY_SUMMARY:"GETCONTENTSUMMARY",
	CREATE:"CREATE",
	READ:"OPEN",
	GET_FILE_STATUS:"GETFILESTATUS"
};

module.exports = FoldersHdfs;

FoldersHdfs.prototype.configure = function(options) {
	// var
	baseurl = options.baseurl;

	if (baseurl.length && baseurl.substr(-1) != "/")
		baseurl = baseurl + "/";

	this.username = options.username;
	this.baseurl = baseurl;

	console.log("inin foldersHdfs,", baseurl, this.username);
}

FoldersHdfs.prototype.features = FoldersHdfs.features = {
	cat : true,
	ls : true,
	write : true,
	server : false
};

FoldersHdfs.isConfigValid = function(config, cb) {
	assert.equal(typeof (config), 'object',
			"argument 'config' must be a object");

	assert.equal(typeof (cb), 'function', "argument 'cb' must be a function");

	var baseurl = config.baseurl;
	var username = config.username;
	var checkConfig = config.checkConfig;

	if (checkConfig == false) {
		return cb(null, config);
	}

	// TODO check access credentials and test conn if needed.

	return cb(null, config);
}

FoldersHdfs.prototype.ls = function(path,cb){
	ls(path, cb);
};

//Temporary comment meta, have to fixed the 'viewfs' first 
//FoldersHdfs.prototype.meta = function(path,files,cb){
//	lsMounts(path, cb);
//};

FoldersHdfs.prototype.write = function(uri, data, cb) {
	
	write(uri, data, function(result,error) {
		if (error){
			cb(null, error);
			return;
		}
		
		cb(result);

	});

};

FoldersHdfs.prototype.cat = function(data, cb) {
	var path = data;	

	cat(path, function(result, error) {

		if (error){
			cb(null, error);
			return;
		}
		
		cb(result);
		
	//		var headers = {
	//			"Content-Length" : result.size,
	//			"Content-Type" : "application/octet-stream",
	//			"X-File-Type" : "application/octet-stream",
	//			"X-File-Size" : result.size,
	//			"X-File-Name" : result.name
	//		};
	//
	//		cb({
	//			streamId : o.streamId,
	//			data : result.stream,
	//			headers : headers,
	//			shareId : data.shareId
	//		});
	});
};

var op = function(path, op) {
	// //FIXME view_op ??
	// var parts = view_op(path, viewfs);
	// var url = parts.base + parts.path + "?op="+op+"&user.name=hdfs";
	
	//delete the '/' of path
	if (path == null || typeof(path)=='undefined' || path=="/"){
		path = "";
	}else if (path.length &&  path.substr(0, 1) == "/"){
		path = path.substr(1);
	}
	
	var url = uriParse.resolve(baseurl, path + "?op=" + op + "&user.name=hdfs");
	console.log("out: " + url);
	return url;
};

//http redirect status code
var isRedirect = function(res){
	return [301, 307].indexOf(res.statusCode) !== -1 && res.headers.hasOwnProperty('location');
};

// http success status code
var isSuccess = function(res){
	return [200, 201].indexOf(res.statusCode) !== -1;
};

// http error status code
var isError = function(res){
	return [400,401,402,403,404,500].indexOf(res.statusCode) !== -1;
};

//check the exception in response body
var parseError = function (body){
	var error = null;
	
	if (typeof body === 'string'){
		try{
			body = JSON.parse(body);
		}catch(err){
			body = null;
		}
	}
	
	//remoteException;
	if (body && body.hasOwnProperty('RemoteException')){
		error = body.RemoteException;
	}
	
	return error;
};

//cat file 
var cat = function(path, cb) {

	// step 1: list file status
	var listUrl = op(path, WebHdfsOp.GET_FILE_STATUS);
	request.get({
		url : listUrl,
		json : true
	}, function(error, response, body) {

		if (error){
			console.error(error);
			return cb(null, error);
		}

		if (isError(response)) {
			console.error(response);
			return cb(null, parseError(body));
		}

		console.log("response in cat,",body);
		if (typeof body === 'string'){
			try{
				body = JSON.parse(body);
			}catch(err){
				body = null;
			}
		}
		
		// get the json of FileStatus
		var fileStatus = body.FileStatus;
		// check if is file, don't support cat Directoy
		if (fileStatus.type == null || fileStatus.type == 'DIRECTORY') {
			console.error("refused to cat directory");
			return cb(null, "refused to cat directory");
		}

		// step 2: get the redirect url for reading the data
		var readUrl = op(path, WebHdfsOp.READ);
		request.get(readUrl, function(error, response, body) {
			if (error){
				console.error(error);
				return cb(null, error);
			}

			if (isError(response)) {
				console.error(response);
				return cb(null, parseError(body));
			}
			
			// check if there is a redirect url for reading here.
			if (!isRedirect(response)){
				var errMsg = "expecting redirect 307, return un-expected status code, statusCode:"
					+ response.statusCode;
				console.error(errMsg);
				console.error(response);
				return callback(null, errMsg);
			}
			
			var redirectUrl = res.headers.hasOwnProperty('location');
			console.log("get data from redirect uri, ",redirectedUri);
			
			// step 3: read the file data from the redirected url.
			request.get(redirectUrl,function(error, response, body){
				if (error){
					console.error(error);
					return cb(null, error);
				}

				if (isError(response)){ 
					console.error(response);
					return cb(null, parseError(body));
				}
				
				cb({
					 // TODO check how to compatible with stream here.
					stream : body,
					size : fileStatus.length,
				// TODO check name here
					name : fileStatus.pathSuffix
					
				});
			});
		});

	});
};

// write file
var write = function(uri, data, cb) {

	// curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
	var path = op(uri, WebHdfsOp.CREATE);
	console.log("step 1, expect the redirect tmp path for file write , url:" + path);
	
	//step 1: get the redirected url for writing data
	request.put(path, function(error, response, body) {
		// forward request error
		if (error){
			console.error(error);
			return cb(null, error);
		}

		if (isError(response)) {
			console.error(response);
			return cb(null, parseError(body));
		}
		
		// check for the expected redirect,307 TEMPORARY_REDIRECT
		// will be redirected to a datanode where the file data is to be written
		if (!isRedirect(response)){
			var errMsg = "expecting redirect 307, return un-expected status code, statusCode:"
				+ response.statusCode;
			console.error(errMsg);
			console.error(response);
			return callback(null, errMsg);
		}
		
		console.log("return redirect uri for step 1,");
		//console.log(response);

		var redirectedUri = response.headers.location;
		console.log("send data to redirect uri, ",redirectedUri);

		if (data instanceof Buffer){
		
			request.put({
				uri : redirectedUri,
				// NOTES we should use the form upload instead??
				body : data
			}, function(error, response, body) {
				if (error){
					console.error(error);
					return cb(null, error);
				}
				
				if (isError(response)){
					console.error(response);
					return cb(null, parseError(body));
				}
				
				else if (isSuccess(response))
					return cb("created success");
				else
					return cb(null, "unkowned response, " + response.body);
			});
		}else{
			
			var errHandle = function(e){
				console.error("error in pipe write to folder-hdfs,",e)
				cb(null,e.message);
			};

			//stream source input, use pipe
			data.on('error',errHandle)
				.pipe(request.put(redirectedUri).on('error',errHandle)
						.on('end', function() {
							console.log("write finished");
							cb("write uri success");}));
			
		}
	});
}

// Providers:
var asHdfsMounts = function() {
    var mounts = [];
    
    //FIXME how to maintain the viewfs??
    for(var path in viewfs) {
     var folder = {
        "pathSuffix": path,
        "type":"DIRECTORY"
     };
     mounts.push(folder);
    }
    var f = {"FileStatuses":{"FileStatus": mounts }};
    return f;
};

var asHdfsFolders = function(dir, files) {
	var out = [];
	for (var i = 0; i < files.length; i++) {
		var file = files[i];
		var o = {
			name : file.pathSuffix
		};
		o.fullPath = dir + file.pathSuffix;
		if (!o.meta)
			o.meta = {};
		var cols = [ 'permission', 'owner', 'group', 'fileId' ];
		for ( var meta in cols)
			o.meta[cols[meta]] = file[cols[meta]];
		o.uri = "#/http_window.io_0:webhdfs/" + o.fullPath;
		o.size = 0;
		o.extension = "txt";
		o.type = "text/plain";
		if (file.type == 'DIRECTORY') {
			o.extension = '+folder';
			o.type = "";
		}
		o.modificationTime = file.modificationTime ? +new Date(
				file.modificationTime) : 0;
		out.push(o);
	}
	return out;
};


// so meta.
// META:
var lsMounts = function(path, cb) {
	processListResponse(path, asHdfsMounts(), cb);
};

var ls = function(path, cb) {

	request(op(path, WebHdfsOp.LIST), function(err, response, content) {
		if (err) {
			console.error("Could not connect", err);
			return cb(null, err);
		}
		try {
			// console.log("LISTSTATUS result:");
			// console.log(content);

			var fileObj = JSON.parse(content);
			files = fileObj.FileStatuses.FileStatus;
		} catch (e) {
			console.error("No luck parsing, path: ", path);
			console.error(fileObj);
			console.error(content);
			return cb(null, content);
		}
		processListResponse(path, fileObj, cb);
	});
};


var lsdu = function(path, cb) {
    var out = [];
    if(path == "" || path.substr(0,1) != "/") path = "/" + path;
    if(path == "" || path.substr(-1) != "/") path = path + "/";
    if(path == "/") {
      lsMounts(cb, path);
        processListResponse(path, asHdfsMounts(), function(hdfs) {
            var ftps = asFtpFolders(path, asFtpMounts())
            var mounts = ftps.concat(hdfs);
            cb(hdfs);
        });
    }
    else {
        console.log("real grab at: ", path);
        ls(path, cb);
    }
};

var processListResponse = function(path, content, cb) {
      var relPath = path;
      var files = content.FileStatuses.FileStatus;
      if(path.substr(0,1) == "/") relPath = path.substr(1);
      var results = asHdfsFolders(relPath, files);
      var latch = files.length;
      for(var i = 0; i < files.length; i++) {
        (function(i) {
            console.log("subrequest: ", path + files[i].pathSuffix);
        request(op(path + files[i].pathSuffix, WebHdfsOp.DIRECTORY_SUMMARY),
          function(err, response, statsResponse) {
            if(err) {
                console.log("failed: " + files[i].pathSuffix);
                console.log(err);
                latch--;
                return cb(null, err);
            }
            
            //FIXME check how node handle the share variable between different thread.
            latch--;
						try {
							stats = JSON.parse(statsResponse);
							if (stats.RemoteException) {
								console.log("RemoteException", stats);
								return cb(null, stats.RemoteException);
							}
							stats = stats.ContentSummary;
						} catch (e) {
							console.error("Parse GETCONTENTSUMMARY json response error,",statsResponse);
						}
            
            results[i].size = stats.length;
            var cols = ['directoryCount','fileCount','spaceConsumed','spaceQuota'];
            if(!results[i].meta) results[i].meta = {};
            for(var meta in cols) results[i].meta[cols[meta]] = stats[cols[meta]];
            if(latch == 0) {
              console.log("fin", results);
              cb(results);
            }
        })})(i);
      }
};

// FIXME: No upper bounds on this cache.
var bigCache = {};
proxyListRequest = function(data) {
    console.log(data);
    var path = data.data.path;
    if(path && path.indexOf('@')>-1) {
        var prefix = "/http_window.io_0:webhdfs/";
        path = path.substr(path.indexOf('@')+1).substr(prefix.length);
    }

    if(path in bigCache) {
        postEvent(data.data.streamId, bigCache[path], directoryStubMime, data.shareId);
    }
    lsdu(path, function(results) {
        bigCache[path] = JSON.stringify(results);
        postEvent(data.data.streamId, bigCache[path], directoryStubMime, data.shareId);
        // NOTES: "undefined" will break some tees.
        // if(teeListResponse) teeListResponse(results);
    });
    // postEvent(data.data.streamId, JSON.stringify(obj), directoryStubMime, data.shareId);
    return;
};
