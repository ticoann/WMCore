"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

import WMCore.Lexicon
from WMCore.Database.CMSCouch import CouchError
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WMSpec.StdSpecs.StdBase import WMSpecFactoryException
from WMCore.WMSpec.WMWorkloadTools import loadSpecByType
from WMCore.Wrappers import JsonWrapper

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Auth import authz_match
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str, validate_strlist

import WMCore.ReqMgr.Service.RegExp as rx

from WMCore.ReqMgr.DataStructs.Request import RequestDataError
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATUS_LIST
from WMCore.ReqMgr.DataStructs.RequestStatus import REQUEST_STATUS_TRANSITION
from WMCore.ReqMgr.DataStructs.RequestType import REQUEST_TYPES
from WMCore.ReqMgr.DataStructs.Request import Request as RequestData



class Request(RESTEntity):
    def __init__(self, app, api, config, mount):
        # main CouchDB database where requests/workloads are stored
        RESTEntity.__init__(self, app, api, config, mount)
        self.reqmgr_db = api.db_handler.get_db(config.couch_reqmgr_db)
        # this need for the post validtiaon 
        self.reqmgr_aux_db = api.db_handler.get_db(config.couch_reqmgr_aux_db)
    
    def validate(self, apiobj, method, api, param, safe):
        # to make validate successful
        # move the validated argument to safe
        # make param empty
        # other wise raise the error 
        
        if method in ['GET']:
            for prop in param.kwargs:
                safe.kwargs[prop] = param.kwargs[prop]
            
            for prop in safe.kwargs:
                del param.kwargs[prop]
#             
#             permittedParams = ["statusList", "names", "type", "prepID", "inputDataset", 
#                                "outputDataset", "dateRange", "campaign", "workqueue", "team"]
#             validate_strlist("statusList", param, safe, '*')
#             validate_strlist("names", param, safe, rx.RX_REQUEST_NAME)
#             validate_str("type", param, safe, "*", optional=True)
#             validate_str("prepID", param, safe, "*", optional=True)
#             validate_str("inputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
#             validate_str("outputDataset", param, safe, rx.RX_REQUEST_NAME, optional=True)
#             validate_strlist("dateRagne", param, safe, rx.RX_REQUEST_NAME)
#             validate_str("campaign", param, safe, "*", optional=True)
#             validate_str("workqueue", param, safe, "*", optional=True)
#             validate_str("team", param, safe, "*", optional=True)
        if method == 'PUT':
            # need to validate the post arguemtn and pass as argument
            self.validate_request_update_args(param, safe)
        
        if method == 'POST':
            # need to validate the post arguemtn and pass as argument
            self.validate_request_create_args(safe)
   
    def validate_request_update_args(self, param, safe):
        """
        validate post request
        1. read data from body
        2. validate the permission
        3. validate state transition
        2. validate using workload validation
        3. convert data from body to arguments (spec instance, argument with default setting) 
        """
        #convert request.body to json (python dict)
        request_args = JsonWrapper.loads(cherrypy.request.body.read())
        
        print param
        
        couchurl =  '%s/%s' % (self.config.couch_host, self.config.couch_reqmgr_db)
        helper = WMWorkloadHelper()
        helper.loadSpecFromCouch(couchurl, param.args[0])
        
        #TODO need to validates kwargs (add WorkloadHelper validation)
        #helper.updateAssignment(request_args)
        safe.args.append(param.args[0])
        param.args.pop()
        safe.kwargs['RequestStatus'] = request_args['RequestStatus']
            

    def validate_request_create_args(self, safe):
        """
        validate post request
        1. read data from body
        2. validate using spec validation
        3. convert data from body to arguments (spec instance, argument with default setting) 
        """
        request_args = JsonWrapper.loads(cherrypy.request.body.read())
        
        #TODO: this fuction need to be update in validateSchema in each Spec
        #request.validate_automatic_args_empty()
        self.request_initialize(request_args)
        
        # get the spec type and validate arguments
        spec = loadSpecByType(request_args["RequestType"])
        workload = spec.factoryWorkloadConstruction(request_args["RequestName"], 
                                                    request_args)
        safe.kwargs['workload'] = workload
        safe.kwargs['schema'] = request_args
        
        
    @restcall
    def get(self, **kwargs):
        """
        Returns request info depending on the conditions set by kwargs
        Currently defined kwargs are following.
        statusList, requestNames, requestType, prepID, inputDataset, outputDataset, dateRange
        If jobInfo is True, returns jobInfomation about the request as well.
            
        TODO:
        stuff like this has to filtered out from result of this call:
            _attachments: {u'spec': {u'stub': True, u'length': 51712, u'revpos': 2, u'content_type': u'application/json'}}
            _id: maxa_RequestString-OVERRIDE-ME_130621_174227_9225
            _rev: 4-c6ceb2737793aaeac3f1cdf591593da4        

        """
        # list of status
        status = kwargs.get("status", False)
        # list of request names
        name = kwargs.get("name", False)
        type = kwargs.get("type", False)
        prep_id = kwargs.get("prep_id", False)
        inputdataset = kwargs.get("inputdataset", False)
        outputdataset = kwargs.get("outputdataset", False)
        date_range = kwargs.get("date_range", False)
        campaign = kwargs.get("campaign", False)
        workqueue = kwargs.get("workqueue", False)
        team = kwargs.get("team", False)
        # eventhing should be stale view. this only needs for test
        _nostale = kwargs.get("_nostale", False)
        option = {}
        if not _nostale:
            option['stale'] = "update_after"
            
        request_info =[]
        
        if status and not team:
            request_info.append(self.get_reqmgr_view("bystatus" , option, status, "list"))
        if status and team:
            request_info.append(self.get_reqmgr_view("byteamandstatus", option, team, "list"))
        if name:
            request_info.append(self._get_request_by_name(name))
        if prep_id:
            request_info.append(self.get_reqmgr_view("byprepid", option, prep_id, "list"))
        if inputdataset:
            request_info.append(self.get_reqmgr_view("byinputdataset", option, inputdataset, "list"))
        if outputdataset:
            request_info.append(self.get_reqmgr_view("byoutputdataset", option, outputdataset, "list"))
        if date_range:
            request_info.append(self.get_reqmgr_view("bydate", option, date_range, "list"))
        if campaign:
            request_info.append( self.get_reqmgr_view("bycampaign", option, campaign, "list"))
        if workqueue:
            request_info.append(self.get_reqmgr_view("byworkqueue", option, workqueue, "list"))
        
        #get interaction of the request
        return self._intersection_of_request_info(request_info);
        
    def _intersection_of_request_info(self, request_info):
        return request_info[0]    
        
    def _get_couch_view(self, couchdb, couchapp, view, options, keys, format):
        
        if not options:
            options = {}
        options.setdefault("include_docs", True)
        if type(keys) == str:
            keys = [keys]
        result = couchdb.loadView(couchapp, view, options, keys)
        
        if format == "dict":
            request_info = {}
            for item in result["rows"]:
                request_info[item["id"]] = None
            return request_info
        else:
            request_info = []
            for item in result["rows"]:
                request_info.append(item["id"])
            return request_info
        
    
    def get_reqmgr_view(self, view, options, keys, format):
        return self._get_couch_view(self.reqmgr_db, "ReqMgr", view,
                                    options, keys, format)
    
    
    def get_wmstats_view(self, view, options, keys, format):
        return self._get_couch_view(self.wmstatsCouch, "WMStats", view,
                                    options, keys, format)
    
    def _get_request_by_name(self, name, stale="update_after"):
        """
        TODO: names can be regular expression or list of names
        """
        request_doc = self.reqmgr_db.document(name)
        return rows([request_doc])
        
    def _combine_request(self, request_info, requestAgentUrl, cache):
        keys = {}
        requestAgentUrlList = []
        for row in requestAgentUrl["rows"]:
            request = row["key"][0]
            if not keys[request]: 
                keys[request] = []
            keys[request].append(row["key"][1])

        for request in request_info: 
            for agentUrl in keys[request]: 
                requestAgentUrlList.append([request, agentUrl]);

        return requestAgentUrlList;

    @restcall
    def put(self, name, **kwargs):
        cherrypy.log("INFO:  '%s  -- %s' ..." % (name, kwargs))
        return self.reqmgr_db.updateDocument(name, "ReqMgr", "updaterequest",
                                             fields=kwargs)
        
    
    @restcall
    def delete(self, request_name):
        cherrypy.log("INFO: Deleting request document '%s' ..." % request_name)
        try:
            self.reqmgr_db.delete_doc(request_name)
        except CouchError, ex:
            msg = "ERROR: Delete failed."
            cherrypy.log(msg + " Reason: %s" % ex)
            raise cherrypy.HTTPError(404, msg)        
        # TODO
        # delete should also happen on WMStats
        cherrypy.log("INFO: Delete '%s' done." % request_name)
        
    
    @restcall
    def post(self, workload, schema):
        """
        Create and update couchDB with  a new request. 
        request argument is passed from validation 
        (validation convert cherrypy.request.body data to argument)
                        
        TODO:
        this method will have some parts factored out so that e.g. clone call
        can share functionality.
        
        NOTES:
        1) do not strip spaces, #4705 will fails upon injection with spaces ; 
            currently the chain relies on a number of things coming in #4705
        
        2) reqInputArgs = Utilities.unidecode(JsonWrapper.loads(body))
            (from ReqMgrRESTModel.putRequest)
                
        """
        cherrypy.log("INFO: Create request, input args: %s ..." % schema)
        
        # storing the request document into Couch

        workload.saveCouch(schema["CouchURL"], schema["CouchWorkloadDBName"],
                           metadata=schema)
        
        #TODO should return something else instead on whole schema
        return schema
        

    def request_validate(self, request):
        """
        Validate input request arguments.
        Upon call of this method, all automatic request arguments are
        already figured out.
        
        TODO:
        Some of these validations will be removed once #4705 is in, in
        favour of validation done in specs during instantiation.
        
        NOTE:
        Checking user/group membership? probably impossible, groups is nothing
        that would be SiteDB ... (and there is no internal user management here)
        
        """
        for identifier in ["ScramArch", "RequestName", "Group", "Requestor",
                           "RequestName", "Campaign", "ConfigCacheID"]:
            request.lexicon(identifier, WMCore.Lexicon.identifier)
        request.lexicon("CMSSWVersion", WMCore.Lexicon.cmsswversion)
        for dataset in ["InputDataset", "OutputDataset"]:
            request.lexicon(dataset, WMCore.Lexicon.dataset)
        if request["Scenario"] and request["ConfigCacheID"]:
            msg = "ERROR: Scenario and ConfigCacheID are mutually exclusive."
            raise RequestDataError(msg)
        if request["RequestType"] not in REQUEST_TYPES:
            msg = "ERROR: Request/Workload type '%s' not known." % request["RequestType"]
            raise RequestDataError(msg)
        
        # check that newly created RequestName does not exist in Couch
        # database or requests already, by any chance.
        try:
            doc = self.reqmgr_db.document(request["RequestName"])
            msg = ("ERROR: Request '%s' already exists in the database: %s." %
                   (request["RequestName"], doc))
            raise RequestDataError(msg)            
        except CouchError:
            # this is what we want here to happen - document does not exist
            pass
        
        # check that specified ScramArch, CMSSWVersion, SoftwareVersions all
        # exist and match
        sw = self.reqmgr_aux_db.document("software")
        if request["ScramArch"] not in sw.keys():
            msg = ("Specified ScramArch '%s not present in ReqMgr database "
                   "(data is taken from TC, available ScramArch: %s)." %
                   (request["ScramArch"], sw.keys()))
            raise RequestDataError(msg)
        # from previously called request_initialize(), SoftwareVersions contains
        # the value from CMSSWVersion, it's enough to validate only SoftwareVersions        
        for version in request.get("SoftwareVersions", []):
            if version not in sw[request["ScramArch"]]:
                msg = ("Specified software version '%s' not found for "
                       "ScramArch '%s'. Supported versions: %s." %
                       (version, request["ScramArch"], sw[request["ScramArch"]])) 
                raise RequestDataError(msg)
    

    def request_initialize(self, request):
        """
        Request data class request is a dictionary representing
        a being injected / created request. This method initializes
        various request fields. This should be the ONLY method to
        manipulate request arguments upon injection so that various
        levels or arguments manipulation does not occur accros several
        modules and across about 7 various methods like in ReqMgr1.
        
        request is changed here.
        
        """ 
        #update the information from config
        request["CouchURL"] = self.config.couch_host
        request["CouchWorkloadDBName"] = self.config.couch_reqmgr_db
        request["CouchDBName"] = self.config.couch_config_cache_db
        
        #user information for cert. (which is converted to cherry py log in)
        request["Requestor"] = cherrypy.request.user["login"]
        request["RequestorDN"] = cherrypy.request.user.get("dn", "unknown")
        # assign first starting status, should be 'new'
        request["RequestStatus"] = REQUEST_STATUS_LIST[0] 
        request["RequestTransition"] = [{"Status": request["RequestStatus"], "UpdateTime": int(time.time())}]
        
        #TODO: generate this automatically from the spec
        # generate request name using request
        self._generateRequestName(request)
        
        request["RequestDate"] = list(time.gmtime()[:6])
        
        request.setdefault("SoftwareVersions", [])
        if request["CMSSWVersion"] and (request["CMSSWVersion"] not in request["SoftwareVersions"]):
            request["SoftwareVersions"].append(request["CMSSWVersion"])
            
        # TODO
        # do we need InputDataset and InputDatasets? when one is just a list
        # containing the other? ; could be related to #3743 problem
        if request.has_key("InputDataset"):
            request["InputDatasets"] = [request["InputDataset"]]
    
    def _generateRequestName(self, request):
        
        current_time = time.strftime('%y%m%d_%H%M%S', time.localtime(time.time()))
        seconds = int(10000 * (time.time() % 1.0))
        request_string = request.get("RequestString", "")
        if request_string != "":
            request["RequestName"] = "%s_%s" % (request["Requestor"], request_string)
        else:
            request["RequestName"] = request["Requestor"]
        request["RequestName"] += "_%s_%s" % (current_time, seconds)       

class RequestStatus(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)


    def validate(self, apiobj, method, api, param, safe):
        validate_str("transition", param, safe, rx.RX_BOOL_FLAG, optional=True)
    
    
    @restcall
    def get(self, transition):
        """
        Return list of allowed request status.
        If transition, return exhaustive list with all request status
        and their defined transitions.
        
        """
        if transition == "true":
            return rows(REQUEST_STATUS_TRANSITION)
        else:
            return rows(REQUEST_STATUS_LIST)
    
    
    
class RequestType(RESTEntity):
    def __init__(self, app, api, config, mount):
        RESTEntity.__init__(self, app, api, config, mount)
    
    
    def validate(self, apiobj, method, api, param, safe):
        pass
    
    
    @restcall
    def get(self):
        return rows(REQUEST_TYPES)
