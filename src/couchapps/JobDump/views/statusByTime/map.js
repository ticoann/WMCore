function(doc) {
  if (doc['states']) {
    for (var i in doc['states']){
        var state = doc['states'][i];
        emit([state['newstate'], state['timestamp']], doc['workflow']);
    }
  }
};