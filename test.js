Tagman=require('tagman');
Q=require('q');

var testData=[
	{url: 'http://www.google.com', tags: 'internet, search engine', data: { label: 'Google' }},
	{url: 'http://www.xzoert.org', tags: 'musica, genio, internet', data: { label: 'Xzoert' }},
	{url: 'http://www.freesounds.net', tags: 'musica, suoni', data: { label: 'Freesounds' }},
	{url: 'dedalus', tags: 'search engine, genio, INTERNET', data: { label: 'Dedalus' }}
];


var tagman;
Tagman.q.get()
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





