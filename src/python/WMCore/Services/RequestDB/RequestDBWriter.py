from WMCore.Wrappers.JsonWrapper import JSONEncoder
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader

class RequestDBWriter(RequestDBReader):

    def __init__(self, couchURL, dbName = None):
        # set the connection for local couchDB call
        # inherited from WMStatsReader
        self._commonInit(couchURL, dbName)


    def insertGenericRequest(self, doc):
        result = self.couchDB.updateDocument(doc['_id'], self.couchapp,
                                    'insertrequest',
                                    fields={'doc': JSONEncoder().encode(doc)})
        self.updateRequestStatus(doc['_id'], "new")
        return result

    def updateRequestStatus(self, request, status):
        status = {'RequsestStatus': status}
        return self.couchDB.updateDocument(request, self.couchapp, 'updatestatus',
                    status)
