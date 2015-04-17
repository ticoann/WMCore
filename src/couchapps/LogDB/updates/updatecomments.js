function (doc, req) {
    // create doc if it is not exist. 
    temp_doc = JSON.parse(req.query.doc);
    if (!doc) {
        // _id needs to be specified
        if (!doc._id) {
            return [null, "Error"];
        }
        doc = temp_doc;
        doc['msg'] = Array();
        doc['msg'].push(temp_doc['msg']);
        return [doc, 'OK'];   
    } else {
        //update the message field and 
       for (key in temp_doc) {
       	   if (key === 'msg') {
       	   		doc[key].push(temp_doc[key]);
       	   } else if (key === 'ts') {
       	   		doc[key] = temp_doc[key]
       	   }
       } 
    } 
} 
