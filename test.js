Tagman=require('tagman');

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



//tagman.update('http://www.google.com',{label: 'Google', description: 'Internet search engine', tags:['internet','search-engine']});

