const http = require('http');
const rest = require('./rest');
const Tagman=require('tagman');

function handleError(res,err,data) {
	if (err) {
		if (typeof err === 'object' && 'code' in err && 'msg' in err) {
			res.writeHead(err.code);
			res.end(err.msg);
		} else {
			res.writeHead(500);
			if (typeof err === 'string') res.end(err);
			else res.end();
		}
		return 1;
	} else if (typeof data==='undefined') {
		res.writeHead(500);
		res.end();
		return 1;
	} 
	return 0;
}

exports.run=function(port) {
	
	process.on('uncaughtException', (err) => {
		console.log('ERROR',err)
	});
	
	var restHandler;
	
	Tagman.q.get()
	.then( (tagman) => {
		return rest.get(tagman);
	}, (reason) => {
		throw reason;
	})
	.then( (rh) => {
		restHandler=rh;
	}, (reason) => {
		throw reason;
	})
	.done();

	if (typeof port==='undefined') port=8000; 
	http.createServer( (req, res) => {
		if (!restHandler) {
			res.writeHead(500);
			res.end();
		} else {
			restHandler.handleRequest(req,res, (err,data) => {
				if (handleError(res,err,data)) return;
				res.writeHead(200, {'Content-Type': 'application/json'});
				res.end(JSON.stringify(data));
			});
		}
	}).listen(port);
	
}



