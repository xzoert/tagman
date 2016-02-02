var sqlite3=require('sqlite3').verbose();
var TransactionDatabase = require("sqlite3-transactions").TransactionDatabase;
var Q=require('q');


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
	self._db.run("CREATE TABLE tag (name TEXT PRIMARY KEY, resource_count INTEGER, last_used_at INTEGER)",[],function(err) {
		self._db.serialize(function () {
			if (!err) {
				/* 
				if no error, the table did not exist, so let's create the remaining tables since this db is not yet initialized
				*/
				
				/*
				following line is commented since i get foreign key errors although i shouldn't. 
				there must be some subtility i miss in parallel operations, although they appear to run
				all in the correct order. i finally don't care too much, the code ensures consistency pretty much
				on its own.
				*/
				//self._db.run("PRAGMA foreign_keys = '1'");
				self._db.run("CREATE TABLE resource (_url TEXT, _created_at INTEGER, _modified_at INTEGER)");
				self._db.run("CREATE UNIQUE INDEX resource_url on resource (_url)");
				self._db.run("CREATE TABLE resource_field (name TEXT, type INTEGER)");
				self._db.run("CREATE TABLE resource_tag (resource_id INTEGER, tag_id INTEGER, FOREIGN KEY (resource_id) REFERENCES resource (rowid), FOREIGN KEY (tag_id) REFERENCES tag (rowid))");
				self._db.run("CREATE UNIQUE INDEX resource_tag_relation on resource_tag (resource_id,tag_id)");
				self._db.run("CREATE INDEX resource_tag_by_tag on resource_tag (tag_id)");
			}
			/*
			load all resource fields, which are cached.
			*/
			self._db.each("SELECT name,type FROM resource_field", function(err, row) {
				self._resourceFields[row.name]=row.type;
			},function (err) {
				/*
				load all tags, which are cached as well.
				*/
				self._db.each("SELECT rowid,name,resource_count,last_used_at FROM tag", function(err, row) {
					self._tags[row.name]=row;
				},function (err) {
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
		define: function(fieldName,fieldType) {
			var d=Q.defer();
			self.define(fieldName,fieldType,function(err) {
				if (err) d.reject(err);
				else d.resolve();
			});
			return d.promise;
		},
		update: function(url,tags,data) {
			var d=Q.defer();
			self.update(url,tags,data,function(err,id) {
				if (err) d.reject(err);
				else d.resolve(id);
			});
			return d.promise;
		},
		remove: function(url) {
			var d=Q.defer();
			self.remove(url,function(err,id) {
				if (err) d.reject(err);
				else d.resolve(id);
			});
			return d.promise;
		},
		findEntries: function (tags,orderBy,limit,offset) {
			var d=Q.defer();
			self.findEntries(tags,orderBy,limit,offset,function(err,res) {
				if (err) d.reject(err);
				else d.resolve(res);
			});
			return d.promise;
		},
		tagCloud: function (tags,limit) {
			var d=Q.defer();
			self.tagCloud(tags,limit,function(err,res) {
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
	this._db.run("ALTER TABLE resource ADD COLUMN "+fieldName+" "+type,function (err) {
		if (err) {
			delete self._resourceFields[fieldName];
			if (callback) callback(err);
			else throw err;
			return;
		}
		self._db.run("INSERT INTO resource_field (name,type) values( ?, ? )",[fieldName,fieldType],function (err) {
			if (err) {
				delete self._resourceFields[fieldName];
				this._db.run("ALTER TABLE resource DROP COLUMN "+fieldName,null,function (err) {});
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
			transaction.rollback(function (err) {
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

/*
updates (or creates if it does not exist) an entry.
the tags can be given as an array or as a string with comma separated tags.
the data are an object containing the data fields of the entry. only the data fields which have been 
defined previously will be entered into the database.
the callback will get err, id (the rowid of the resource, which might be used for debugging).
the whole operation runs in a transaction and will either succeed or fail atomically.
*/
Engine.prototype.update=function(url,tags,data,callback) {
	if (typeof tags==='string') tags=exports.parseTags(tags);
	var self=this;
	this._db.get("SELECT rowid,* FROM resource WHERE _url=?",[url],function (err, row) {
		if (self._error(err,callback)) return;
		if (!row) {
			// new entry
			var fields=['_url','_created_at','_modified_at'];
			var now=Date.now();
			var values=[url,now,now];
			var plholders='?,?,?';
			for (var name in self._resourceFields) {
				if (name in data) {
					fields.push(name);
					values.push(data[name]);
					plholders+=',?';
				}
			}
			var id=null;
			self._db.beginTransaction(function(err,db) {
				if (self._error(err,callback,db)) return;
				db.run("INSERT INTO resource ("+fields.join(',')+") VALUES ("+plholders+")",values,function(err) {
					if (self._error(err,callback,db)) return;
					var id=this.lastID;
					var iTags={};
					for (var i in tags) { 
						iTags[tags[i].trim().toLowerCase()]=1;
					}
					var iterator=new AsyncIterator(iTags);
					iterator.run(function (name,next) {
						self._tagUsed(db,name,id,next);
					},function(errors) {
						if (errors) {
							db.rollback(function(){
								if (callback) callback(errors,null);
								else throw errors;
							});
						} else {
							db.commit(function(err) {
								if (callback) callback(err,id);
								else if (err) throw err;
							});
						}
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
			self._db.beginTransaction(function (err,db) {
				if (self._error(err,callback,db)) return;
				db.run("UPDATE resource "+fields+" WHERE rowid="+resource.rowid,values,function(err) {
					if (self._error(err,callback,db)) return;
					var rTags={};
					var iTags={};
					for (var i in tags) { 
						iTags[tags[i].trim().toLowerCase()]=1;
					}
					db.each("SELECT tag.name from resource_tag, tag where resource_tag.tag_id=tag.rowid and resource_tag.resource_id="+resource.rowid,function (err,row) {
						if (self._error(err,callback,db)) return;
						if (row.name in iTags) {
							delete iTags[row.name];
						} else {
							rTags[row.name]=1;
						}
					},function (err) {
						if (self._error(err,callback,db)) return;
						var rIterator=new AsyncIterator(rTags);
						rIterator.run(function (name,next) {
							self._tagUnused(db,name,resource.rowid,next);
						},function (err) {
							if (self._error(err,callback,db)) return;
							var iIterator=new AsyncIterator(iTags);
							iIterator.run(function (name,next) {
								self._tagUsed(db,name,resource.rowid,next);
							},function(err) {
								if (self._error(err,callback,db)) return;
								db.commit(function(err) {
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
}

/*
removes an entry with a given url
*/
Engine.prototype.remove=function (url,callback) {
	var self=this;
	this._db.get("SELECT rowid FROM resource WHERE _url=?",[url],function (err, resource) {
		if (self._error(err,callback)) return;
		if (resource) {
			self._db.beginTransaction(function (err,db) {
				var rTags={};
				db.each("SELECT tag.name from resource_tag, tag where resource_tag.tag_id=tag.rowid and resource_tag.resource_id="+resource.rowid,function (err,row) {
					if (self._error(err,callback,db)) return;
					rTags[row.name]=1;
				},function (err) {
					if (self._error(err,callback,db)) return;
					var rIterator=new AsyncIterator(rTags);
					rIterator.run(function (name,next) {
						self._tagUnused(db,name,resource.rowid,next);
					},function (err) {
						db.run("DELETE FROM resource_tag WHERE resource_id=?",[resource.rowid],function (err) {
							if (self._error(err,callback,db)) return;
							db.run("DELETE FROM resource WHERE rowid=?",[resource.rowid],function(err) {
								if (self._error(err,callback,db)) return;
								db.commit(function(err) {
									if (callback) callback(err,resource.rowid);
									else if (err) throw err;
								});
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


/*
find entries tagged by a list of tags, optionally ordered by a given field and optionally 
specifying the offset and limit of the returned result.
tags can be provided as an array or as a comma separated string.
if no tags are given, all entries will be returned.
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
Engine.prototype.findEntries=function(tags,orderBy,limit,offset,callback) {
	var params=[];
	var self=this;
	var sql="SELECT r.* FROM resource r";
	if (tags) {
		if (typeof tags==='string') tags=exports.parseTags(tags);
		for (var i in tags) {
			var name=tags[i];
			if (name in self._tags) {
				var tagId=self._tags[name].rowid;
				sql+=" INNER JOIN resource_tag rt"+i+" ON rt"+i+".resource_id=r.rowid AND rt"+i+".tag_id=?";
				params.push(tagId);
			} else {
				// tag not in database, no document matches!
				console.log('TAG '+name+' NOT IN DATABASE');
				if (callback) callback(null,[]);
				return;
			}
		}
	}
	if (orderBy) {
		orderBy=orderBy.trim().toLowerCase();
		if (orderBy in self._resourceFields) {
			sql+=' ORDER BY '+orderBy;
		} else {
			var msg='No such field: "'+orderBy+'".';
			if (callback) callback(msg);
			else throw msg;
			return;
		}
		var field=self._resourceFields
	}
	if (!offset) offset=0;
	if (limit) {
		sql+=' LIMIT ?,?';
		params.push(offset);
		params.push(limit);
	}
	var result=[];
	self._db.each(sql,params,function (err,row) {
		if (self._error(err,callback)) return;
		result.push(row);
	}, function (err) {
		if (self._error(err,callback)) return;
		if (callback) callback(null,result);
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

ordered in descending weight order. if you specify a limit of N, the top N in terms of
weight will be returned.
*/
Engine.prototype.tagCloud=function(tags,limit,callback) {
	var params=[];
	var self=this;
	var sql="SELECT tag.name, COUNT(rt.rowid) as weight FROM resource_tag rt INNER JOIN tag ON rt.tag_id=tag.rowid";
	var where=null;
	if (tags) {
		if (typeof tags==='string') tags=exports.parseTags(tags);
		for (var i in tags) {
			var name=tags[i];
			if (name in self._tags) {
				var tagId=self._tags[name].rowid;
				sql+=" INNER JOIN resource_tag rt"+i+" ON rt"+i+".resource_id=rt.resource_id AND rt"+i+".tag_id=?";
				if (!where) where=' WHERE rt.tag_id!=?';
				else where+=' AND rt.tag_id!=?';
				params.push(tagId);
			} else {
				// tag not in database
				if (callback) callback(null,[]);
				return;
			}
		}
	}
	if (where) {
		sql+=where;
		params=params.concat(params);
	}
	sql+=' GROUP BY rt.tag_id ORDER BY weight DESC';
	if (limit) {
		sql+=' LIMIT 0,?';
		params.push(limit);
	}
	var result=[];
	self._db.each(sql,params,function (err,row) {
		if (self._error(err,callback)) return;
		result.push(row);
	}, function (err) {
		if (self._error(err,callback)) return;
		if (callback) callback(null,result);
	});
	
}

/*
inserts the record in the resource_tag table and updates the tag resource_count and last_used.
if the tag not yet exists, it will be created.
*/
Engine.prototype._tagUsed=function(db,tag,resourceId,callback) {
	var self=this;
	if (tag in self._tags) {
		var now=Date.now();
		var to=self._tags[tag];
		db.run("UPDATE tag SET last_used_at=?, resource_count=? where name=?",[now,to.resource_count+1,tag],function(err) {
			if (self._error(err,callback,db)) return;
			to.resource_count+=1;
			to.last_used_at=now;
			db.run("INSERT INTO resource_tag (resource_id, tag_id) VALUES (?,?)",[resourceId,to.rowid],function(err) {
				if (self._error(err,callback,db)) return;
				if (callback) callback(null,to);
			});
		});
	} else {
		var now=Date.now();
		var to={name:tag,last_used_at:now,resource_count:1};
		db.run("INSERT INTO tag (name,last_used_at,resource_count) VALUES (?,?,?)",[tag,now,1],function(err) {
			if (self._error(err,callback,db)) return;
			to.rowid=this.lastID;
			self._tags[tag]=to;
			db.run("INSERT INTO resource_tag (resource_id, tag_id) VALUES (?,?)",[resourceId,to.rowid],function(err) {
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
	var self=this;
	if (tag in self._tags) {
		var to=self._tags[tag];
		db.run("UPDATE tag SET resource_count=? where name=?",[to.resource_count-1,tag],function(err) {
			if (self._error(err,callback,db)) return;
			db.run("DELETE FROM resource_tag where tag_id=? and resource_id=?",[to.rowid,resourceId],function(err) {
				if (err) {
					if (callback) callback(err);
					else throw err;
					return;
				}
			});
			to.resource_count-=1;
			callback(null,to);
		});
	}
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
	if (!text) return [];
	return text.split(/\s*,\s*/g);
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
		step(i,function(err) {
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




