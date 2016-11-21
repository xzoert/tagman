const Q=require('q');
const URL=require('url');

/*
The rest api processes an http request object, if the url prefix matches the prefix given.

following calls are at disposal:

	POST suggestions
	data:
		limit
		offset
		prefix
		exclude
	



	
*/

function Handler(tagman,prefix) {
	if (!prefix) {
		prefix='';
	} else {
		prefix=prefix.replace(/^\/+|\/+$/gm,'');
	}
	this.prefix=prefix;
	this.tagman=tagman.q;
}

exports.get=function(tagman) {
	var d=Q.defer();
	var h=new Handler(tagman);
	d.resolve(h);
	return d.promise;
}

Handler.prototype.handleRequest= function ( req, resp, callback ) {
	var rdata;
	var self=this;
	if (req.url.length<self.prefix.length || req.url.substr(0,self.prefix.length)!==self.prefix) {
		var msg='URL prefix does not match';
		if (callback) callback( msg );
		else throw msg;
		return;
	}
	
	var url=req.url.substr(self.prefix.length);
	var purl=URL.parse(url,true);
	var url=purl.pathname.replace(/^\/+|\/+$/gm,'');
	var urlParams=purl.query;
	
	var method=req.method;
	
	
	var body='';
	req.on('data', function (data) {
		body += data;

		// Too much POST data, kill the connection!
		// 1e6 === 1 * Math.pow(10, 6) === 1 * 1000000 ~~~ 1MB
		if (body.length > 1e6) {
			req.connection.destroy();
			throw 'Too many data';
		}
	});

	req.on('end', function () {
		if (method==='POST') {
			if (body) {
				try {
					data=JSON.parse(body);
				} catch( err )  {
					callback({code: 500, msg: "Bad JSON format"});
					return;
				}
			} else {
				data={};
			}
		}
		
		if (method==='GET') {
			var rtags=url.split(/\/+\s*/g);
			var tags=[];
			for (var i in rtags) {
				var name=decodeURIComponent(rtags[i]).trim();
				if (!name) continue;
				tags.push(name);
			}
			urlParams.tags=tags;
			self.getResources(urlParams,callback);
			return;
		} else if (method==='POST') {
			var firstFrag='';
			var remainder=url;
			var pos=url.indexOf('/');
			if (pos==-1) {
				firstFrag=url;
				remainder=null;
			} else {
				firstFrag=url.substr(0,pos);
				remainder=url.substr(pos+1);
			}
			switch( firstFrag ) {
				case 'find':
					return self.getResources(data,callback);
				case 'resource':
					return self.getResource(data,callback);
				case 'urls':
					return self.getUrlList(data,callback);
				case 'update':
					return self.updateResource(data,callback);
				case 'rename':
					return self.renameResource(data,callback);
				case 'suggestions':
					return self.getSuggestions(data,callback);
				case 'load':
					return self.load(data,callback);
				case 'clear':
					return self.clear(callback);
				case 'remove':
					return self.remove(data,callback);
				case 'removeList':
					return self.removeList(data,callback);
					
				default:
					callback({code: 404, msg: 'Not found'});
					return;
			}
		}
		
	});
	
	
	
}


Handler.prototype.getResources=function(data,callback) {
	var self=this;
	var limit=data.limit||null;
	var offset=data.offset||null;
	var orderBy=data.orderBy||null;
	var tagCloud=data.tagCloud||null;
	var tagCloudUseOr=data.tagCloudUseOr||null;
	var resourceList=data.resourceList||true;
	if (typeof tagCloud === 'string' ) {
		if (tagCloud && tagCloud.toLowerCase()=='true') tagCloud=true;
		else tagCloud=false;
	}
	var tagCloudLimit=data.tagCloudLimit||null;
	var tags=data.tags||{};
	if (resourceList) {
		self.tagman.findResources(tags,orderBy,limit,offset)
		.then( (result) => {
			if (tagCloud) {
				var list=result;
				self.tagman.tagCloud(tags,tagCloudLimit,tagCloudUseOr)
				.then( (cloud) => {
					callback( null, {'resources':list,'tagCloud':cloud} );
				}, (err) => {
					callback({code: 500, msg: err});
				});
			} else {
				callback(null,{'resources':result});
			}
		}, (err) => {
			callback({code: 500, msg: err});
		});
	} else if (tagCloud) {
		self.tagman.tagCloud(tags,tagCloudLimit,tagCloudUseOr)
		.then( (cloud) => {
			callback( null, {'tagCloud':cloud} );
		}, (err) => {
			callback({code: 500, msg: err});
		});
	} else {
		callback( null, {} );
	}
}

Handler.prototype.getSuggestions=function(data,callback) {
	var self=this;
	var limit=data.limit||null;
	var offset=data.offset||null;
	var prefix=data.prefix||'';
	var exclude=data.exclude||[];
	var minWeight=data.minWeight||0;
	self.tagman.getSuggestions(prefix,exclude,limit,offset,minWeight)
	.then( (result) => {
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}

Handler.prototype.getResource=function(data,callback) {
	var self=this;
	var url=data.url||'';
	self.tagman.getResource(url)
	.then( (result) => {
		if (!result) result=null;
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}

Handler.prototype.getUrlList=function(data,callback) {
	var self=this;
	var urlList=data||[];
	self.tagman.getBulk(urlList)
	.then( (result) => {
		if (!result) result=null;
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}


Handler.prototype.remove=function(data,callback) {
	var self=this;
	var url=data.url||'';
	self.tagman.remove(url)
	.then( (result) => {
		if (!result) result=null;
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}

Handler.prototype.removeList=function(data,callback) {
	var self=this;
	var urlList=data||[];
	self.tagman.removeBulk(urlList)
	.then( (result) => {
		if (!result) result=null;
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}

Handler.prototype.updateResource=function(data,callback) {
	var self=this;
	var url=data.url||'';
	if (!url) {
		callback({code: 500, msg: 'No url'});
		return;
	}
	var tags=data.tags||{};
	var data=data.data||{};
	self.tagman.update(url,tags,data)
	.then( (result) => {
		if (!result) result=null;
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}

Handler.prototype.renameResource=function(data,callback) {
	var self=this;
	var url=data.url||'';
	if (!url) {                                                                    
		callback({code: 500, msg: 'No url'});
		return;
	}
	var newUrl=data.newUrl||'';
	if (!newUrl) {
		callback({code: 500, msg: 'No new url'});
		return;
	}
	self.tagman.rename(url,newUrl,data.renameDescendants)
	.then( (result) => {
		if (!result) result=null;
		self.tagman.getResource(newUrl)
		.then( (result) => {
			if (!result) result=null;
			callback(null,result);
		}, (err) => {
			callback({code: 500, msg: err});
		});
	}, (err) => {
		callback({code: 500, msg: err});
	});
}

Handler.prototype.load=function(data,callback) {
	var self=this;
	if (!data) {
		callback({code: 500, msg: 'No data'});
		return;
	}
	self.tagman.loadBulk(data)
	.then( (result) => {
		if (!result) result=null;
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}

Handler.prototype.clear=function(callback) {
	var self=this;
	self.tagman.clear()
	.then( (result) => {
		if (!result) result=null;
		callback(null,result);
	}, (err) => {
		callback({code: 500, msg: err});
	});
}


