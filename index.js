const path=require('path');
const fs=require('fs');
const Q=require('q');
const engine=require('./engine');
const server=require('./server');
const rest=require('./rest');


/*
the engine pool: one engine per database file path.
*/
var pool={};

/*
function to get the user home directory cross platform.
*/
function getUserHome() {
  return process.env[(process.platform == 'win32') ? 'USERPROFILE' : 'HOME'];
}


/*
expose the engine field types.
*/
exports.Text=engine.Text;
exports.Int=engine.Int;
exports.Float=engine.Float;

/*
returns to the callback a tagman engine connected to the given sqlite3 file. 
if no file is provided, it will use the default file in:
	USER_HOME/.tagman/default.sql
two arguments will be passed to the callback: err, for the error, and tagman, for the engine.
*/
exports.get=function(sqlfilepath,callback) {
	if (!callback && typeof sqlfilepath === 'function' ) {
		callback=sqlfilepath;
		sqlfilepath=null;
	}
	if (!sqlfilepath) {
		var tagmandir=path.resolve(getUserHome(),'.tagman');
		if (!fs.existsSync(tagmandir)) {
			fs.mkdirSync(tagmandir);
		}
		sqlfilepath=path.resolve(tagmandir,'default.sql');
	}
	if (sqlfilepath in pool) {
		var t=pool[sqlfilepath];
		if (callback) callback(null,t);
		return t;
	} else {
		var t=engine.create(sqlfilepath,callback);
		pool[sqlfilepath]=t;
		return t;
	}
}

/*
Qed version of get: returns a promise instead of using a callback.
*/
exports.q={
	get: (sqlfilepath) => {
		var d=Q.defer();
		exports.get(sqlfilepath, (err,tagman) => {
			if (err) d.reject(err);
			else d.resolve(tagman.q);
		});
		return d.promise;
	},
	getRest: (tagman) => {
		return rest.get(tagman);
	}
}


exports.runServer=function (port) {
	server.run(port);
}

exports.getRest=function (tagman,callback) {
	rest.get(tagman).
	then(function (rest) {
		callback(null,rest);
	}, function (err) {
		callback(err,null);
	});
}

