function(doc) {
  emit([doc.RequestStatus, doc.RequestType], doc.RequestName);
}