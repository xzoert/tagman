const sqlite3=require('sqlite3').verbose();
const TransactionDatabase = require("sqlite3-transactions").TransactionDatabase;
const Q=require('q');


/*
the field types you can use as second argument in in define() and which are returned by getFieldType().
NB: for datetimes you should use timestamps and store them as integers.
*/
exports.Text=1;
exports.Int=2;
exports.Float=3;



/*
the engine constructor.
the callback will get err, engine.
*/
function Engine(sqlfilepath,callback) {                                                      
	this._resourceFields={};
	this._tags={};
	this._db=new TransactionDatabase( new sqlite3.Database(sqlfilepath) );

	var self=this;
	self._db.run("CREATE TABLE tag (name TEXT PRIMARY KEY, resource_count INTEGER, last_used_at INTEGER)",[], (err) => {
		self._db.serialize( () => {
			if (!err) {
				/* 
				if no error, the table did not exist, so let's create the remaining tables since this db is not yet initialized
				*/
				
				/*
				following line is commented since i get foreign key errors although i shouldn't. 
				there must be some subtility i miss in parallel operations, although they appear to run
				all in the correct order. i finally don't care too much, since the code ensures consistency 
				pretty much by its own.
				*/
				//self._db.run("PRAGMA foreign_keys = '1'");
				self._db.run("CREATE TABLE resource (_url TEXT, _created_at INTEGER, _modified_at INTEGER, _path TEXT)");
				self._db.run("CREATE UNIQUE INDEX resource_path on resource (_path)");
				self._db.run("CREATE TABLE tree_idx (desc_id INTEGER, anc_id INTEGER, FOREIGN KEY (anc_id) REFERENCES resource (rowid), FOREIGN KEY (desc_id) REFERENCES resource (rowid))");
				self._db.run("CREATE UNIQUE INDEX tree_descendant on tree_idx (desc_id,anc_id)");
				self._db.run("CREATE INDEX tree_ancestor on tree_idx (anc_id)");
				self._db.run("CREATE TABLE resource_field (name TEXT, type INTEGER)");
				self._db.run("CREATE TABLE resource_tag (resource_id INTEGER, tag_id INTEGER, tag_path TEXT, comment TEXT, FOREIGN KEY (resource_id) REFERENCES resource (rowid), FOREIGN KEY (tag_id) REFERENCES tag (rowid))");
				self._db.run("CREATE UNIQUE INDEX resource_tag_relation on resource_tag (resource_id,tag_id)");
				self._db.run("CREATE INDEX resource_tag_by_tag on resource_tag (tag_id)");
				self._db.run("CREATE INDEX resource_tag_by_tagpath on resource_tag (tag_path)");
				self._db.run("CREATE TABLE meta (key TEXT, value TEXT)");
				self._db.run("INSERT INTO meta (key,value) VALUES ('format_version','1')");
			}
			/*
			load all resource fields, which are cached.
			*/
			self._db.each("SELECT name,type FROM resource_field", (err, row) => {
				self._resourceFields[row.name]=row.type;
			}, (err) => {
				/*
				load all tags, which are cached as well.
				*/
				self._db.each("SELECT rowid,name,resource_count,last_used_at FROM tag", (err, row) => {
					self._tags[row.name]=row;
				}, (err) => {
					if (callback) callback(err,self);
				});
			});
		});
	});
	
	/*
	Qed version of the engine API: all public methods of the engine are replicated on the engine.q object, but
	returning a promise istead of calling a callback.
	*/
	this.q={
		cb: self,
		define: (fieldName,fieldType) => {
			var d=Q.defer();
			self.define(fieldName,fieldType, (err) => {
				if (err) d.reject(err);
				else d.resolve();
			});
			return d.promise;
		},
		loadBulk: (data) => {
			var d=Q.defer();
			self.loadBulk(data, (err) => {
				if (err) d.reject(err);
				else d.resolve();
			});
			return d.promise;
		},
		update: (url,tags,data) => {
			var d=Q.defer();
			self.update(url,tags,data, (err,id) => {
				if (err) d.reject(err);
				else d.resolve(id);
			});
			return d.promise;
		},
		remove: (url) => {
			var d=Q.defer();
			self.remove(url, (err,id) => {
				if (err) d.reject(err);
				else d.resolve(id);
			});
			return d.promise;
		},                                   
		removeBulk: (urlList) => {
			var d=Q.defer();
			self.removeBulk(urlList, (err,id) => {
				if (err) d.reject(err);
				else d.resolve(id);
			});
			return d.promise;
		},                                   
		rename: (url,newUrl,renameDescendants) => {
			var d=Q.defer();
			self.rename(url, newUrl, renameDescendants, (err,id) => {
				if (err) d.reject(err);
				else d.resolve(id);
			});
			return d.promise;
		},
		findResources: (tags,orderBy,limit,offset) => {
			var d=Q.defer();
			self.findResources(tags,orderBy,limit,offset, (err,res) => {
				if (err) d.reject(err);
				else d.resolve(res);
			});
			return d.promise;
		},
		getResource: (url) => {
			var d=Q.defer();
			self.getResource(url, (err,res) => {
				if (err) d.reject(err);
				else d.resolve(res);
			});
			return d.promise;
		},
		getBulk: (urlList) => {
			var d=Q.defer();
			self.getBulk(urlList, (err,res) => {
				if (err) d.reject(err);
				else d.resolve(res);
			});
			return d.promise;
		},
		tagCloud: (tags,limit,useOr) => {
			var d=Q.defer();
			self.tagCloud(tags,limit,useOr, (err,res) => {
				if (err) d.reject(err);
				else d.resolve(res);
			});
			return d.promise;
		},
		getSuggestions: (prefix,exclude,limit,offset,minWeight) => {
			var d=Q.defer();
			self.getSuggestions(prefix,exclude,limit,offset,minWeight, (err,res) => {
				if (err) d.reject(err);
				else d.resolve(res);
			});
			return d.promise;
		},
		clear: () => {
			var d=Q.defer();
			self.clear( (err,res) => {
				if (err) d.reject(err);
				else d.resolve(res);
			});
			return d.promise;
		}
	};
	
	/*
	ensure that whatever API you are using (the callback API or the Qed one), you will
	always be able to do:
		engine.cb.someMethod()
	and:
		engine.q.someMethod()
	getting explicitely the desired version of the method.
	*/
	this.q.q=this.q;
	this.cb=this;
	
}

/*
returns the type of a user defined field, or undefined if the field does not exist.
*/
Engine.prototype.getFieldType=function(fieldName) {
	return this._resourceFields[fieldName];
}

/*
allows the user to define custom data fields on the stored resources.
if the field already exists, nothing will happen unless the existing field has a
type different than fieldType, in which case an error will be thrown.
field names must begin with a latin letter and can contain only latin letters, numbers 
and the underscore character. 
field names are case insensitive.
*/
Engine.prototype.define=function(fieldName, fieldType,callback) {
	if (!fieldName || typeof fieldName !== 'string') throw 'Invalid field name';
	fieldName=fieldName.trim().toLowerCase();
	if (fieldName in this._resourceFields) {
		if (fieldType!=this._resourceFields[fieldName]) throw 'Field '+fieldName+' already exists with a different type';
		if (callback) callback();
		return;
	}
	if (!fieldName.match(/^[a-zA-Z][a-zA-Z\_\-0-9]*$/)) throw 'Invalid field name';
	var type;
	switch( fieldType ) {
		case exports.Int:
			type='INTEGER';
			break;
		case exports.Text:
			type='TEXT';
			break;
		case exports.Float:
			type='REAL';
			break;
		default:
			throw 'Unknown field type: '+fieldType;
	}
	var self=this;
	this._resourceFields[fieldName]=fieldType;
	this._db.run("ALTER TABLE resource ADD COLUMN "+fieldName+" "+type, (err) => {
		if (err) {
			delete self._resourceFields[fieldName];
			if (callback) callback(err);
			else throw err;
			return;
		}
		self._db.run("INSERT INTO resource_field (name,type) values( ?, ? )",[fieldName,fieldType], (err) => {
			if (err) {
				delete self._resourceFields[fieldName];
				this._db.run("ALTER TABLE resource DROP COLUMN "+fieldName,null, (err) => {});
				if (callback) callback(err);
				else throw err;
				return;
			}
			if (callback) callback();
		});
	});
}

/*
error handling utility. 
it checks if there is an error, in which case it will:
	- rollback the transaction, if any
	- pass the error to the callback, if any, else throw it
	- return 1 in case there is an error and 0 if there isn't
*/
Engine.prototype._error=function(err,callback,transaction) {
	if (err) {
		if (transaction) {
			transaction.rollback( (err) => {
				if (callback) callback(err);
				throw err;
			});
		} else {
			if (callback) callback(err);
			throw err;
		}
		return 1;
	} else {
		return 0;
	}
}

var count=0;

Engine.prototype.clear=function(callback) {
	var self=this;
	self._db.serialize( () => {
		self._db.run("DELETE FROM resource_tag");
		self._db.run("DELETE FROM tag");
		self._db.run("DELETE FROM tree_idx");
		self._db.run("DELETE FROM resource",function (err) {
			callback(err);
		});
	});
	this._tags={};
}

Engine.prototype.loadBulk=function(data,callback) {
	var self=this;
	self._db.beginTransaction(function(err,db) {
		if (self._error(err,callback,db)) return;
		var datai=new AsyncIterator(data);
		datai.seq(function (i,next) {
			var entry=data[i];
			self._update(db,entry.url,entry.tags,entry.data,function (err,id) {
				next(err);
			});
		},function (err) {
			if (err) {
				db.rollback( () => {
					if (callback) callback(err);
					else throw err;
				});
			} else {
				db.commit( (err) => {
					if (callback) callback(err);
					else if (err) throw err;
				});
			}
		});
	});
	
}

Engine.prototype.removeBulk=function(data,callback) {
	var self=this;
	self._db.beginTransaction(function(err,db) {
		if (self._error(err,callback,db)) return;
		var datai=new AsyncIterator(data);
		datai.seq(function (i,next) {
			var url=data[i];
			self._remove(db,url,function (err,id) {
				next(err);
			});
		},function (err) {
			if (err) {
				db.rollback( () => {
					if (callback) callback(err);
					else throw err;
				});
			} else {
				db.commit( (err) => {
					if (callback) callback(err);
					else if (err) throw err;
				});
			}
		});
	});
}


Engine.prototype.getBulk=function(urlList,callback) {
	var self=this;
	var listi=new AsyncIterator(urlList);
	result=[]
	listi.seq(function (i,next) {
		var url=urlList[i];
		self.getResource(url,function (err,res) {
			if (res) result.push(res)
			next(err);
		});
	},function (err) {
		if (err) {
			db.rollback( () => {
				if (callback) callback(err);
				else throw err;
			});
		} else {
			if (callback) callback(null,result);
		}
	});
}


/*
updates (or creates if it does not exist) an entry.
the tags can be given as an array or as a string with comma separated tags.
the data are an object containing the data fields of the entry. only the data fields which have been 
defined previously will be entered into the database.
the callback will get err, id (the rowid of the resource, which might be used for debugging).
the whole operation runs in a transaction and will either succeed or fail atomically.
*/
Engine.prototype.update=function(url,tags,data,callback) {
	var self=this;
	self._db.beginTransaction(function(err,db) {
		if (self._error(err,callback,db)) return;
		self._update(db,url,tags,data,function (err,id) {
			if (err) {
				db.rollback( () => {
					if (callback) callback(err,null);
					else throw err;
				});
			} else {
				db.commit( (err) => {
					if (callback) callback(err,id);
					else if (err) throw err;
				});
			}
		});
	});
}

Engine.prototype._update=function(db,url,tags,data,callback) {
	tags=this._normalizeTagList(tags);
	var self=this;
	var path=self._pathFromUrl(url);
	db.get("SELECT rowid,* FROM resource WHERE _path=?",[path], (err, row) => {
		if (self._error(err,callback)) return;
		if (!row) {

			// new entry
			var fields=['_url','_created_at','_modified_at','_path'];
			var now=Date.now();
			var values=[url,now,now,path];
			var plholders='?,?,?,?';
			for (var name in self._resourceFields) {
				if (name in data) {
					fields.push(name);
					values.push(data[name]);
					plholders+=',?';
				}
			}
			var id=null;
			db.run("INSERT INTO resource ("+fields.join(',')+") VALUES ("+plholders+")",values,function(err) {
				if (self._error(err,callback,db)) return;
				var id=this.lastID;
				var iTags=tags;
				var iterator=new AsyncIterator(iTags);
				iterator.run( (name,next) => {
					self._tagUsed(db,name,iTags[name],id,next);
				}, (err) => {
					if (self._error(err,callback,db)) return;
					db.run("INSERT INTO tree_idx (desc_id,anc_id) SELECT rowid, ? FROM resource WHERE _path like ?",[id,path+'%'],function(err) {
						if (self._error(err,callback,db)) return;
						db.run("INSERT INTO tree_idx (desc_id,anc_id) SELECT ?, rowid from resource where ? like _path || '%' and rowid!=?",[id,path,id],function(err) {
							if (self._error(err,callback,db)) return;
							callback(err,id);
						});
					});
				});
			});
		} else {
			// update existing
			var resource=row;
			var fields='SET _modified_at=?';
			var values=[Date.now()];
			for (var name in self._resourceFields) {
				if (name in data) {
					fields+=','+name+'=?';
					values.push(data[name]);
				}
			}
			if (self._error(err,callback,db)) return;
			db.run("UPDATE resource "+fields+" WHERE rowid="+resource.rowid,values, (err) => {
				if (self._error(err,callback,db)) return;
				var rTags={};
				var iTags=tags;
				var pTags={};
				db.each("SELECT rt.comment as comment, tag.name as name FROM resource_tag rt, tag WHERE rt.tag_id=tag.rowid and resource_id=?",[resource.rowid],function (err, row ) {
					if (self._error(err,callback,db)) return;
					//console.log('PTAGS',pTags);
					pTags[row.name]=row.comment;
				},function(err) {
					if (self._error(err,callback,db)) return;
					for (name in pTags) {
						if (name in iTags) {
							if (iTags[name]==pTags[name]) delete iTags[name];
							else rTags[name]=1;
						} else {
							rTags[name]=1;
						}
					}
					var rIterator=new AsyncIterator(rTags);
					rIterator.run( (name,next) => {
						self._tagUnused(db,name,resource.rowid,next);
					}, (err) => {
						if (self._error(err,callback,db)) return;
						var iIterator=new AsyncIterator(iTags);
						iIterator.run( (name,next) => {
							self._tagUsed(db,name,iTags[name],resource.rowid,next);
						}, (err) => {
							if (self._error(err,callback,db)) return;
							if (callback) callback(err,resource.rowid);
						});
					});
				});
			});
		}
	});
	
}

/*
removes an entry with a given url
*/
Engine.prototype.remove=function(url,callback) {
	var self=this;
	self._db.beginTransaction(function(err,db) {
		if (self._error(err,callback,db)) return;
		self._remove(db,url,function (err,id) {
			if (err) {
				db.rollback( () => {
					if (callback) callback(err,null);
					else throw err;
				});
			} else {
				db.commit( (err) => {
					if (callback) callback(err,id);
					else if (err) throw err;
				});
			}
		});
	});
}

Engine.prototype._remove=function (db,url,callback) {
	var self=this;
	db.get("SELECT rowid FROM resource WHERE _path=?",[self._pathFromUrl(url)], (err, resource) => {
		if (self._error(err,callback)) return;
		if (resource) {
			var rTags={};
			db.each("SELECT tag.name from resource_tag, tag where resource_tag.tag_id=tag.rowid and resource_tag.resource_id="+resource.rowid, (err,row) => {
				if (self._error(err,callback,db)) return;
				rTags[row.name]=1;
			}, (err) => {
				if (self._error(err,callback,db)) return;
				var rIterator=new AsyncIterator(rTags);
				rIterator.run( (name,next) => {
					self._tagUnused(db,name,resource.rowid,next);
				}, (err) => {
					db.run("DELETE FROM tree_idx WHERE anc_id=? or desc_id=?",[resource.rowid,resource.rowid], (err) => {
						if (self._error(err,callback,db)) return;
						db.run("DELETE FROM resource_tag WHERE resource_id=?",[resource.rowid], (err) => {
							if (self._error(err,callback,db)) return;
							db.run("DELETE FROM resource WHERE rowid=?",[resource.rowid], (err) => {
								if (callback) callback(err,resource.rowid);
								else if (err) throw err;
							});
						});
					});
				});
			});
		} else {
			if (callback) callback();
		}
	});
}


/* renames a resource, i.e. changes the URL (and tree position consequently) */
Engine.prototype.rename=function (url,newUrl,renameDescendants,callback) {
	var self=this;
	var path=self._pathFromUrl(url);
	var newPath=self._pathFromUrl(newUrl);
	if (newPath===path) return;
	
	if (typeof renameDescendants==='undefined') renameDescendants=1;
	this._db.get("SELECT rowid FROM resource WHERE _path=?",[path], (err, resource) => {
		if (self._error(err,callback)) return;
		if (!resource) return;
		var id=resource.rowid;
		self._db.beginTransaction(function(err,db) {
			if (self._error(err,callback,db)) return;
			if (renameDescendants) {
				if (newPath.length>path.length && newPath.substr(0,path.length)==path) {
					// RENAMED AS A DESCENDANT OF ITSELF...
					// UPDATES THE PATH AND URL OF THE RESOURCE AND ITS DESCENDANTS, NOTHING ELSE CHANGES
					var sql="UPDATE resource SET _path=? || SUBSTR(_path,?+1), _url=? || SUBSTR(_url,?+1) WHERE _path LIKE ? || '%'";
					var params=[newPath,path.length,newUrl,url.length,path];
					db.run(sql,params,function(err) {
						if (self._error(err,callback,db)) return;
						db.commit( (err) => {
							if (callback) self.getResource(newUrl,callback);
							else if (err) throw err;
						});
					});
				} else if (path.length>newPath.length && path.substr(0,newPath.length)==newPath) {
					// RENAMED AS AN ANCESTOR OF ITSELS.... DO NOT ALLOW BY NOW
					if (callback) callback("Can't rename as an ancestor of itself.");
					return;
				} else {
					// INSERTS NEW ANCESTORS FOR THE RESOURCE AND ITS CURRENT DESCENDANTS
					var sql="INSERT INTO tree_idx (desc_id,anc_id) SELECT t.desc_id, r.rowid FROM resource r, tree_idx t WHERE ? LIKE _path || '%' AND ? NOT LIKE _path || '%' AND t.anc_id=?";
					var params=[newPath,path,id];
					db.run(sql,params,function(err) {
						// INSERTS NEW DESCENDATS 
						db.run("INSERT INTO tree_idx (desc_id,anc_id) SELECT rowid, ? FROM resource WHERE _path LIKE ? AND _path NOT LIKE ? AND rowid!=?",[id,newPath+'%',path+'%',id],function(err) {
						if (self._error(err,callback,db)) return;
							// DELETES OLD ANCESTORS FOR THE RESOURCE AND ITS DESCENDANTS
							var sql="DELETE FROM tree_idx WHERE EXISTS (SELECT rowid FROM tree_idx t2 WHERE t2.anc_id=? AND t2.desc_id=tree_idx.desc_id) AND desc_id!=anc_id AND anc_id!=? AND NOT EXISTS (SELECT rowid FROM resource r WHERE rowid=tree_idx.anc_id AND ? LIKE r._path || '%')";
							var params=[id,id,newPath];
							db.run(sql,params,function(err) {
								// UPDATES THE PATH AND URL OF THE RESOURCE AND ITS DESCENDANTS
								db.run("UPDATE resource SET _path=? || SUBSTR(_path,?+1), _url=? || SUBSTR(_url,?+1) WHERE _path LIKE ? || '%';",[newPath,path.length,newUrl,url.length,path],function(err) {
									if (self._error(err,callback,db)) return;
									db.commit( (err) => {
										if (callback) self.getResource(newUrl,callback);
										else if (err) throw err;
									});
								});
							});
						});
					});
				}
			} else {
				// INSERT NEW DESCENDATS
				var sql="INSERT INTO tree_idx (desc_id,anc_id) SELECT rowid, ? FROM resource WHERE _path LIKE ? AND _path NOT LIKE ? AND rowid!=?";
				var params=[id,newPath+'%',path+'%',id];
				db.run(sql,params,function(err) {
					// INSERTS NEW ANCESTORS	
					var sql="INSERT INTO tree_idx (desc_id,anc_id) SELECT ?, rowid FROM resource WHERE ? LIKE _path || '%' AND ? NOT LIKE _path || '%' AND rowid!=?";
					var params=[id,newPath,path,id];
					db.run(sql,params,function(err) {
						if (self._error(err,callback,db)) return;
							// DELETES OLD ANCESTORS
							var sql="DELETE FROM tree_idx WHERE desc_id=? AND desc_id!=anc_id AND NOT EXISTS (SELECT rowid FROM resource WHERE rowid=anc_id AND ? LIKE _path || '%')";
							var params=[id,newPath];
							db.run(sql,params,function(err) {
								if (self._error(err,callback,db)) return;
								// DELETES OLD DESCENDANTS
								var sql="DELETE FROM tree_idx WHERE anc_id=? AND desc_id!=anc_id AND NOT EXISTS (SELECT rowid FROM resource WHERE rowid=anc_id AND _path LIKE ?)";
								var params=[id,newPath+'%'];
								db.run(sql,params,function(err) {
									// UPDATES THE PATH AND URL
									var sql="UPDATE resource SET _url=?, _path=? WHERE rowid=?";
									var params=[newUrl,newPath,id];
									db.run(sql,params,function(err) {
										if (self._error(err,callback,db)) return;
										db.commit( (err) => {
											if (callback) callback(err,resource.rowid);
											else if (err) throw err;
										});
									});
								});
							});
						
					});
				});
			}
		});
	});
}


/*
find entries tagged by a list of tags, optionally ordered by a given field and optionally 
specifying the offset and limit of the returned result.
tags can be provided as an array or as a comma separated string.
if no tags are given, all entries will be returned (in which case you probably want to set some limit).
the result will contain a list of objects in the form:

	{
		_url: 'some://url',
		_created_at: 142672938,
		_modified_at: 1426762374,
		data_field_1: 'value',
		data_field_2: 'other value'
		...
	}
*/

Engine.prototype.findResources=function(tags,orderBy,limit,offset,callback) {
	var params=[];
	var self=this;
	tags=this._normalizeTagList(tags);
	var sql="SELECT r.* FROM resource r";
	var i=1;
	for (var name in tags) {
		if (name in self._tags) {
			var tagId=self._tags[name].rowid;
			if (i==1) sql+=" WHERE ";
			else sql+=" AND ";
			if (tags[name]==-1) sql+='NOT ';
			sql+="EXISTS ( SELECT rt"+i+".rowid FROM tree_idx i"+i+" INNER JOIN resource_tag rt"+i+" ON  rt"+i+".resource_id=i"+i+".anc_id AND rt"+i+".tag_path LIKE ? WHERE i"+i+".desc_id=r.rowid)";
			i++;
			params.push(name+'-%');
		} else {
			// tag not in database, no document matches!
			console.log('TAG '+name+' NOT IN DATABASE');
			if (callback) callback(null,[]);
			return;
		}
	}
	/*
	var sql="SELECT r.* FROM resource r";
	var i=1;
	for (var name in tags) {
		if (name in self._tags) {
			var tagId=self._tags[name].rowid;
			sql+=" INNER JOIN tree_idx i"+i+" ON i"+i+".desc_id=r.rowid";
			sql+=" INNER JOIN resource_tag rt"+i+" ON rt"+i+".resource_id=i"+i+".anc_id AND rt"+i+".tag_id=?";
			i++;
			params.push(tagId);
		} else {
			// tag not in database, no document matches!
			console.log('TAG '+name+' NOT IN DATABASE');
			if (callback) callback(null,[]);
			return;
		}
	}
	*/
	if (orderBy) {
		orderBy=orderBy.trim().toLowerCase();
		if (orderBy in self._resourceFields) {
			sql+=' ORDER BY '+orderBy+' COLLATE NOCASE';
		} else {
			var msg='No such field: "'+orderBy+'".';
			if (callback) callback(msg);
			else throw msg;
			return;
		}
		//var field=self._resourceFields
	} 
	if (!offset) offset=0;
	if (limit) {
		sql+=' LIMIT ?,?';
		params.push(offset);
		params.push(limit);
	}
	var result=[];
	self._db.each(sql,params, (err,row) => {
		if (self._error(err,callback)) return;
		result.push(row);
	}, (err) => {
		if (self._error(err,callback)) return;
		if (callback) callback(null,result);
	});
}

Engine.prototype._getTags=function (row,callback) {
	var self=this;
	row._tags=[];
	var sql="SELECT tag.name AS name, rt.comment AS comment, anc._url AS inheritedFrom FROM resource_tag rt, tag, tree_idx, resource as anc WHERE rt.resource_id=tree_idx.anc_id AND tag.rowid=rt.tag_id AND anc.rowid=tree_idx.anc_id AND tree_idx.desc_id=? order by tag.name";
	self._db.each(sql,[row.rowid],function (err,tagrow) {
		if (self._error(err,callback)) return;
		row._tags.push(tagrow);
	},function (err) {
		delete row.rowid;
		delete row._path;
		if (callback) callback(null,row);
	});
}


Engine.prototype.getResource=function (url,callback) {
	var self=this;
	var sql='SELECT rowid,* FROM resource WHERE _path=?';
	var path=self._pathFromUrl(url);
	self._db.get(sql,[path], (err,row) => {
		if (self._error(err,callback)) return;
		if (!row) {
			// FIND THE CLOSEST ANCESTOR
			var sql="SELECT rowid,* FROM resource WHERE ? LIKE _path || '%' ORDER BY _path DESC LIMIT 1";
			self._db.get(sql,[path], (err,row) => {
				if (self._error(err,callback)) return;
				if (!row) {
					if (callback) callback(null,{_url:url,_template:1});
					return;
				}
				self._getTags(row, (err,row) => {
					if (self._error(err,callback)) return;
					row._url=url;
					delete row._created_at;
					delete row._modified_at;
					delete row.label;
					delete row.description;
					row._template=1
					if (callback) callback(null,row);
				});
			});
		} else {
			self._getTags(row, (err,row) => {
				if (self._error(err,callback)) return;
				if (callback) callback(null,row);
			});
		}
	});
}


/*
generates the data for a tag cloud of all entries matching a given query (i.e. a given
list of tags).
tags can be an array as well as a comma separated string.
the result is an array of entries in the form:

	{
		name: 'tag name',
		weight: 37
	}

ordered in descending weight order. the weight corresponds to the number of matching entries 
which are tagged by the tag.
the tags provided as query will not be listed in the tag cloud.
if you specify a limit of N, the top N in terms of weight will be returned.
if no tags are specified, the tagcloud of the whole database will be generated (in which case a limit 
is highly recommended!)
*/
Engine.prototype.tagCloud=function(tags,limit,useOr,callback) {

	var params=[];
	var self=this;
	tags=this._normalizeTagList(tags);
	/*
	var sql="SELECT r.rowid as id FROM resource r";
	var where=null;
	var i=1;
	for (var name in tags) {
		if (name in self._tags) {
			var tagId=self._tags[name].rowid;
			sql+=" INNER JOIN tree_idx i"+i+" ON i"+i+".desc_id=r.rowid";
			sql+=" INNER JOIN resource_tag rt"+i+" ON rt"+i+".resource_id=i"+i+".anc_id AND rt"+i+".tag_id=?";
			if (!where) where=' WHERE rt.tag_id!=?';
			else where+=' AND rt.tag_id!=?';
			i++;
			params.push(tagId);
		} else {
			// tag not in database, no document matches!
			console.log('TAG '+name+' NOT IN DATABASE');
			if (callback) callback(null,[]);
			return;
		}
	}
	*/
	var where=null;
	var whereParams=[]
	var sql="SELECT r.rowid,r.* FROM resource r";
	var i=1;
	for (var name in tags) {
		if (name in self._tags) {
			var tagId=self._tags[name].rowid;
			if (i==1) {
				sql+=" WHERE ";
			} else if (useOr) {
				sql+=" OR ";
			} else {
				sql+=" AND ";
			}
			if (tags[name]==-1) sql+='NOT ';
			sql+="EXISTS ( SELECT rt"+i+".rowid FROM tree_idx i"+i+" INNER JOIN resource_tag rt"+i+" ON  rt"+i+".resource_id=i"+i+".anc_id AND rt"+i+".tag_path LIKE ? WHERE i"+i+".desc_id=r.rowid)";
			params.push(name+'-%');
			if (!where) where=' WHERE rt.tag_id!=?';
			else where+=' AND rt.tag_id!=?';
			whereParams.push(tagId);
			i++;
		} else {
			// tag not in database, no document matches!
			console.log('TAG '+name+' NOT IN DATABASE');
			if (callback) callback(null,[]);
			return;
		}
	}
	sql="SELECT res.name as name, COUNT(res.desc_id) as weight FROM ( SELECT DISTINCT i.desc_id as desc_id,tag.name as name,tag.rowid as rowid FROM ( "+sql+" ) t INNER JOIN tree_idx i ON i.desc_id=t.rowid INNER JOIN resource_tag rt ON rt.resource_id=i.anc_id INNER JOIN tag ON tag.rowid=rt.tag_id ";
	if (where) {
		sql+=where;
		params=params.concat(whereParams);
	}
	sql+=") res";
	/*
	var params=[];
	var self=this;
	tags=this._normalizeTagList(tags);
	//var sql="SELECT tag.name, COUNT(rt.rowid) as weight FROM resource_tag rt INNER JOIN tag ON rt.tag_id=tag.rowid";
	var sql="SELECT tag.name, COUNT(i.desc_id) as weight FROM tree_idx i INNER JOIN resource_tag rt ON rt.resource_id=i.anc_id INNER JOIN tag ON rt.tag_id=tag.rowid";
	var where=null;
	var i=1;
	for (var name in tags) {
		if (!name) continue;
		if (name in self._tags) {
			var tagId=self._tags[name].rowid;
			sql+=" INNER JOIN tree_idx i"+i+" ON i.desc_id=i"+i+".desc_id";
			sql+=" INNER JOIN resource_tag rt"+i+" ON rt"+i+".resource_id=i"+i+".anc_id AND rt"+i+".tag_id=?";
			if (!where) where=' WHERE rt.tag_id!=?';
			else where+=' AND rt.tag_id!=?';
			params.push(tagId);
			i++;
		} else {
			// tag not in database
			if (callback) callback(null,[]);
			return;
		}
	}
	if (where) {
		sql+=where;
		params=params.concat(params);
	}
	*/
	sql+=' GROUP BY res.rowid ORDER BY weight DESC';
	if (limit) {
		sql+=' LIMIT 0,?';
		params.push(limit);
	}
	var result=[];
	self._db.each(sql,params, (err,row) => {
		if (self._error(err,callback)) return;
		result.push(row);
	}, (err) => {
		if (self._error(err,callback)) return;
		if (callback) callback(null,result);
	});
}

Engine.prototype.getSuggestions=function (prefix,exclude,limit,offset,minWeight,callback) {
	var self=this;
	exclude=this._normalizeTagList(exclude);
	var params=[''+prefix+'%'];
	var sql='SELECT name, resource_count FROM tag WHERE name LIKE ?';
	if (exclude) {
		for (var name in exclude) {
			sql+=' AND NAME!=?';
			params.push(name);
		}
	}
	if (minWeight) {
		sql+=' AND resource_count>=?';
		params.push(minWeight);
	}
	sql+=' ORDER BY name COLLATE NOCASE';
	if (!offset) offset=0;
	if (limit) {
		sql+=' LIMIT ?,?';
		params.push(offset);
		params.push(limit);
	}
	var result=[];
	self._db.each(sql,params, (err,row) => {
		if (self._error(err,callback)) return;
		result.push([row.name,row.resource_count]);
	}, (err) => {
		if (self._error(err,callback)) return;
		if (callback) callback(null,result);
	});
}

/*
inserts the record in the resource_tag table and updates the tag resource_count and last_used.
if the tag not yet exists, it will be created.
*/
Engine.prototype._tagUsed=function(db,tag,comment,resourceId,callback) {
	if (!tag) {
		if (callback) callback();
		return;
	}
	var self=this;
	if (tag in self._tags) {
		var now=Date.now();
		var to=self._tags[tag];
		db.run("UPDATE tag SET last_used_at=?, resource_count=? where name=?",[now,to.resource_count+1,tag], (err) => {
			if (self._error(err,callback,db)) return;
			to.resource_count+=1;
			to.last_used_at=now;
			db.run("INSERT INTO resource_tag (resource_id, tag_id, tag_path, comment) VALUES (?,?,?,?)",[resourceId,to.rowid,to.name+'-',comment], (err) => {
				if (self._error(err,callback,db)) return;
				if (callback) callback(null,to);
			});
		});
	} else {
		var now=Date.now();
		var to={name:tag,last_used_at:now,resource_count:1};
		db.run("INSERT INTO tag (name,last_used_at,resource_count) VALUES (?,?,?)",[tag,now,1], function (err) {
			if (self._error(err,callback,db)) return;
			to.rowid=this.lastID;
			self._tags[tag]=to;
			db.run("INSERT INTO resource_tag (resource_id, tag_id, tag_path, comment) VALUES (?,?,?,?)",[resourceId,to.rowid,to.name+'-',comment], (err) => {
				if (self._error(err,callback,db)) return;
				if (callback) callback(null,to);
			});
		});
	}
}

/*
removes the record from the resource_tag table and decrements the tag resource_count.
*/
Engine.prototype._tagUnused=function(db,tag,resourceId,callback) {
	if (!tag) {
		if (callback) callback();
		return;
	}
	var self=this;
	if (tag in self._tags) {
		var to=self._tags[tag];
		db.run("DELETE FROM resource_tag where tag_id=? AND resource_id=?",[to.rowid,resourceId], (err) => {
			if (self._error(err,callback,db)) return;
			if (to.resource_count>1) {
				db.run("UPDATE tag SET resource_count=? WHERE rowid=?",[to.resource_count-1,to.rowid], (err) => {
					if (err) {
						if (callback) callback(err);
						else throw err;
						return;
					}
				});
			} else {
				db.run("DELETE FROM tag WHERE rowid=?",[to.rowid], (err) => {
					if (err) {
						if (callback) callback(err);
						else throw err;
						return;
					}
				});
			}
			to.resource_count-=1;
			callback(null,to);
		});
	}
}

/*
converts a tag list (an array of tags) or a string (a list of comma separated tags) into an object
having the tags as properties mapped to an 1. 
tags will be trimmed. empty tags will be ignored. 
this format is convenient since it allows fast lookup and ensures there are no duplicates.
*/
Engine.prototype._normalizeTagList=function (tags) {
	if (!tags) return {}
	switch(tags.constructor) {
		case String:
			return exports.parseTags(tags);
		case Array:
			var a={}
			for (i in tags) {
				var tag=tags[i]
				switch( tag.constructor ) {
					case String:
						var name=tag.trim()
						if (name) a[name]='';
						break;
					case Array:
						if (tag.length==2) {
							var name=tag[0].trim();
							var comment=tag[1];
							if (name) a[name]=comment;
							break;
						}
					default:
						throw 'Only string lists or lists of name/comment pairs are accepted.';
				}
			}
			return a;
		case Object:
			a={};
			for (name in tags) {
				nname=name.trim();
				if (!nname) continue;
				a[nname]=tags[name];
			}
			return a;
		default:
			throw 'Tags canb be only a string, an array or an object.';
	}
}

Engine.prototype._pathFromUrl=function (url) {
	if (url.charAt(url.length-1)=='/') return url;
	else return url+'/';
}

Engine.prototype._serializeTagList=function (tags) {
	var data=[];
	for (var name in tags) {
		data.push([name,tags[name]]);
	}
	return JSON.stringify(data);
}




/*
function for creating a new Engine.
*/
exports.create=function (sqlfilepath,callback) {
	return new Engine(sqlfilepath,callback);
}

/*
utility function for parsing a comma separated string getting back an array of tags.
*/
exports.parseTags=function(text) {
	var res={};
	var re=/([^\(,]+)(\(([^\)]*)\))?\s*(,|$|\n)/g;
	
	while ((match = re.exec(text)) !== null) {
		var comment=match[3];
		if (!comment) comment='';
		res[match[1].trim()]=comment;
	}
	return res;
}


/*
check if an object is empty
*/

function isEmpty(obj) {
	for (p in obj) return false;
	return true;
}



/*
simple home made iterator of asynchronous operations done on an array or on an object.
usage:
	var list=[1,2,3];
	var i=AsyncIterator(list);
	i.run(function (i,next) {
		someAsynchronousFunctionWithCallback(list[i],function (err,whatever) {
			if (err) {
				// this will not cause the loop to stop
				next(err);
				return;
			}
			// process 'whatever' if you have to
			next();
		});
	},function (errors) {
		if (errors) {
			// handle them: it is a list since it will collect all errors
		} 
		// done
	});
*/
function AsyncIterator(list) {
	if (typeof list !== 'object' ) {
		throw 'Lists must be either arrays or objects.';
	}
	if (Array.isArray(list)) {
		this._len=list.length;
	} else {
		this._len=Object.keys(list).length;
	}
	this._list=list;
}

exports.AsyncIterator=AsyncIterator;

AsyncIterator.prototype.run=function(step,end) {
	count=0;
	if (this._len==0) {
		if (end) end();
		return;
	}
	var self=this;
	var errors=[]
	for (var i in this._list) {
		step(i, (err) => {
			if (err) {
				errors.push(err);
			} 
			count++;
			if (count==self._len) {
				if (end) {
					if (errors.length) end(errors);
					else end();
				}
			}
		});
	}
}

AsyncIterator.prototype.seq=function(step,end) {
	if (this._len==0) {
		if (end) end();
		return;
	}
	var state={errors:[],count:0,end:end,step:step};
	this._seqStep(state);
}


AsyncIterator.prototype._seqStep=function(state) {
	var self=this;
	state.step(state.count, (err) => {
		if (err) {
			state.errors.push(err);
		} 
		state.count++;
		if (state.count==self._len) {
			if (state.end) {
				if (state.errors.length) state.end(state.errors);
				else state.end();
			}
		} else {
			self._seqStep(state);
		}
	});
}



