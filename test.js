Tagman=require('tagman');
Q=require('q');
/*
tagman=Tagman.get(function(err,tagman) {
	tagman.define(' label ',Tagman.Text);
	tagman.define('description',Tagman.Text);
	//console.log('TAGS',tagman._tags);
	tagman.update(
		'http://www.google.com',
		//{'internet':1,'search-engine':2},
		//'#internet ##search-engine',
		'internettauta*5, search-engine*2 abbi chiari propositi nella vita',
		{label: 'Google Inc.', description: 'Internet search engine'},
		function (err, id) {
			console.log('UPDATED',err,id);
		}
	);
});
*/


var testData=[
	{url: 'http://www.google.com', tags: 'internet, search engine', data: { label: 'Google' }},
	{url: 'http://www.xzoert.org', tags: 'musica, genio, internet', data: { label: 'Xzoert' }},
	{url: 'http://www.freesounds.net', tags: 'musica, suoni', data: { label: 'Freesounds' }},
	{url: 'dedalus', tags: 'search engine, genio, internet', data: { label: 'Dedalus' }}
];


var tagman;
Tagman.getq()
.then(function(t) {
	tagman=t;
	return tagman.define(' label ',Tagman.Text);
})
.then(function() {
	return tagman.define('description',Tagman.Text);
})
.then(function() {
	var test=testData[0];
	return tagman.update(test.url,test.tags,test.data)
})
.then(function() {
	var test=testData[1];
	return tagman.update(test.url,test.tags,test.data)
})
.then(function() {
	var test=testData[2];
	return tagman.update(test.url,test.tags,test.data)
})
.then(function() {
	var test=testData[3];
	return tagman.update(test.url,test.tags,test.data)
})
.then(function() {
	return tagman.findEntries('internet')
	.then(function(result) {
		console.log('RESULT',result);
	});
})
.then(function() {
	return tagman.tagCloud('musica')
	.then(function(result) {
		console.log('TAG CLOUD',result);
	});
})
.done(function() {
	console.log('DONE');	
});




//tagman.update('http://www.google.com',{label: 'Google', description: 'Internet search engine', tags:['internet','search-engine']});

