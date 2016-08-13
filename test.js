const Tagman=require('tagman');
const Q=require('q');



function rndStr(n) {
	if (!n) n=8;
	var i=n;
	var r='';
	while (i>0) {
		var x=Math.floor(Math.random()*36);
		if (x<10) r+=String.fromCharCode(48+x);
		else r+=String.fromCharCode(55+x);
		--i;
	}
	return r;
}

function rndPath(n) {
	var steps=Math.floor(Math.random()*10);
	var path=null;
	while (steps>0) {
		if (!path) path=rndStr(n);
		else path+="/"+rndStr(n);
		--steps;
	}
	return path;
}

function parseTags(text) {
	var res={};
	var re=/([^\(,]+)(\(([^\)]*)\))?\s*(,|$)/g;
	
	while ((match = re.exec(text)) !== null) {
		console.log('TAG',match);
		var comment=match[3];
		if (!comment) comment='';
		res[match[1].trim()]=comment;
	}
	return res;
	
}

var text='Tag 1 (for this reason), Tag 2 (for another reason), Tag 3, Tag 4';
console.log(parseTags(text));


var testData=[
	{url: 'http://www.google.com', tags: 'internet (because this)\n search engine', data: { label: 'Google' }},
	{url: 'http://www.xzoert.org', tags: 'musica, genio (because that), internet', data: { label: 'Xzoert' }},
	{url: 'http://www.freesounds.net', tags: 'musica, suoni', data: { label: 'Freesounds' }},
	{url: 'dedalus', tags: 'search engine, genio, internet, semola, segovia, segatura, segale', data: { label: 'Dedalus' }}
];

var testTags=[];
for (var i=0; i<400; ++i) {
	testTags.push(rndStr(10));
}


for (var i=0; i<100000; ++i) {
	var tags=[];
	for (var j=0; j<Math.floor(Math.random()*12)+3; ++j) {
		var idx1=Math.floor(Math.random()*testTags.length/4);
		var idx2=Math.floor(Math.random()*testTags.length/4);
		var idx3=Math.floor(Math.random()*testTags.length/4);
		var idx4=Math.floor(Math.random()*testTags.length/4);
		tags.push(testTags[idx1+idx2+idx3+idx4]);
	}
	var entry={'url': 'http://'+rndPath(2),tags:tags,data:{label:rndStr(10),description:rndStr(10)}}
	testData.push(entry);
}



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
	return tagman.loadBulk(testData);
})
.then(function() {
	return tagman.findResources('internet')
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





