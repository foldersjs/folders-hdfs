// Folders.io connector to WebHDFS


var op = function(path, op) {
    var parts = view_op(path, viewfs);
    var url = parts.base + parts.path + "?op="+op+"&user.name=hdfs";
    console.log("out: " + url);
    return url;
};

// Providers:
var asHdfsMounts = function() {
    var mounts = [];
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
  for(var i = 0; i < files.length; i ++) {
    var file = files[i];
    var o = { name: file.pathSuffix };
    o.fullPath = dir + file.pathSuffix;
    if(!o.meta) o.meta = {};
    var cols = ['permission','owner','group','fileId'];
    for(var meta in cols) o.meta[cols[meta]] = file[cols[meta]];
    o.uri = "#/http_window.io_0:webhdfs/" + o.fullPath;
    o.size = 0;
    o.extension = "txt";
    o.type = "text/plain";
    if(file.type == 'DIRECTORY') {
        o.extension = '+folder';
        o.type = "";
    }
    o.modificationTime = file.modificationTime ? +new Date(file.modificationTime) : 0;
    out.push(o);
  }
  return out;
};


// so meta.
// META:
var lsMounts = function(path, cb) {
  processListResponse(asHdfsMounts(), cb);
};

var ls = function(path, cb) {
  request(op(path, "LISTSTATUS"), function(err,response, content) {
      if(err) {
          console.log("Could not connect");
          return;
      }
      try {
          var fileObj = JSON.parse(content);
          files = fileObj.FileStatuses.FileStatus;
      } catch(e) {
          console.log("No luck parsing");
          console.log("path: " + path);
          console.log(fileObj);
          console.log(content);
          return;
      }
      processListResponse(fileObj, cb);
  });
};


var lsdu = function(path, cb) {
    var out = [];
    if(path == "" || path.substr(0,1) != "/") path = "/" + path;
    if(path == "" || path.substr(-1) != "/") path = path + "/";
    if(path == "/") {
      lsMounts(cb, path);
        processListResponse(asHdfsMounts(), function(hdfs) {
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

var processListResponse = function(content, cb) {
      var relPath = path;
      var files = content.FileStatuses.FileStatus;
      if(path.substr(0,1) == "/") relPath = path.substr(1);
      var results = asHdfsFolders(relPath, files);
      var latch = files.length;
      for(var i = 0; i < files.length; i++) {
        (function(i) {
            console.log("subrequest: ", path + files[i].pathSuffix);
        request(op(path + files[i].pathSuffix, "GETCONTENTSUMMARY"),
          function(err, response, statsResponse) {
            if(err) {
                console.log("failed: " + files[i].pathSuffix);
                console.log(err);
                latch--;
                return;
            }
            latch--;
            try {
            stats = JSON.parse(statsResponse);
            if(stats.RemoteException) {
              console.log("oops", stats);
              return;
            }
              stats = stats.ContentSummary;
            } catch(e) {
              console.log("oh");
              console.log(statsResponse);
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
