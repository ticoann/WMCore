#!/usr/bin/env python

"""
Request Manager service (ReqMgr) injection script.

This script, with appropriate input, replaces a few Ops scripts:
vocms23:/data/cmst1/CMSSW_4_1_8_patch1/src/mc_test/testbed/
    make_mc_gen_request.py
    make_rereco_skim.py
    make_redigi_request2.py
    make_mc_lhe_request.py
 
The script shall have no WMCore libraries dependency.

Command line interface: --help

There are mandatory command line arguments (e.g. URL of the Request Manager)

Production ConfigCache: https://cmsweb.cern.ch/couchdb/reqmgr_config_cache/

-----------------------------------------------------------------------------
Notes to request arguments JSON file:

Parameters with values "*-OVERRIDE-ME" are supposed to be defined (overridden)
by a user on the command line, whichever other argument can be overridden too.

// TODO
// this is passing into a webpage checkbox HTML element (hence "checked" value")
// needs to be replaced by a proper boolean argument in the REST API
// "Team"+team: "checked" (the same for "checkbox"+workflow: "checked")

"""


import os
import sys
import httplib
import urllib
import logging
from optparse import OptionParser, TitledHelpFormatter
import json
import copy


class ReqMgrClient(object):
    """
    Client REST interface to Request Manager service (ReqMgr).
    
    Actions: queryRequests, deleteRequests, createRequest, assignRequests,
             cloneRequest, allTest, userGroup', team,
               

    """
    def __init__(self, reqMgrUrl, certFile, keyFile):
        logging.info("Identity files:\n\tcert file: '%s'\n\tkey file:  '%s' " %
                     (certFile, keyFile))
        self.textHeaders  =  {"Content-type": "application/x-www-form-urlencoded",
                              "Accept": "text/plain"}
        if reqMgrUrl.startswith("https://"):
            reqMgrUrl = reqMgrUrl.replace("https://", '')
        self.conn = httplib.HTTPSConnection(reqMgrUrl, key_file = keyFile,
                                            cert_file = certFile)
        
        
    def _httpRequest(self, verb, uri, data=None, headers=None):
        logging.info("Request: %s %s ..." % (verb, uri))
        if headers:
            self.conn.request(verb, uri, data, headers)
        else:
            self.conn.request(verb, uri, data)
        resp = self.conn.getresponse()
        data = resp.read()
        logging.debug("Status: %s" % resp.status)
        logging.debug("Reason: %s" % resp.reason)
        return resp.status, data
        

    def _createRequestViaRest(self, requestArgs):
        """
        Talks to the REST ReqMgr API.
        
        """
        logging.info("Injecting a request for arguments (REST API):\n%s ..." % requestArgs["createRequest"])
        jsonArgs = json.dumps(requestArgs["createRequest"])
        status, data = self._httpRequest("PUT", "/reqmgr/reqMgr/request", data=jsonArgs)        
        if status > 216:
            logging.error("Error occurred, exit.")
            print data
            sys.exit(1)
        data = json.loads(data)
        # ReqMgr returns dictionary with key: 'WMCore.RequestManager.DataStructs.Request.Request'
        # print data
        requestName = data.values()[0]["RequestName"] 
        logging.info("Create request '%s' succeeded." % requestName)
        return requestName
                

    def _createRequestViaWebPage(self, requestArgs):
        """
        Talks to the ReqMgr webpage, as if the request came from the web browser.
        
        """
        encodedParams = urllib.urlencode(requestArgs["createRequest"])
        logging.info("Injecting a request for arguments (webpage):\n%s ..." % requestArgs["createRequest"])
        # the response is now be an HTML webpage
        status, data = self._httpRequest("POST", "/reqmgr/create/makeSchema",
                                         data=encodedParams, headers=self.textHeaders)        
        if status > 216 and status != 303:
            logging.error("Error occurred, exit.")
            print data
            sys.exit(1)
        # this is a call to a webpage/webform and the response here is HTML page
        # retrieve the request name from the returned HTML page       
        requestName = data.split("'")[1].split('/')[-1]
        logging.info("Create request '%s' succeeded." % requestName)
        return requestName
    
    
    def createRequest(self, config, restApi = True):
        """
        requestArgs - arguments for both creation and assignment
        restApi - call REST API at ReqMgr or request creating webpage
        
        """
        if restApi:
            requestName = self._createRequestViaRest(config.requestArgs)
        else:
            requestName = self._createRequestViaWebPage(config.requestArgs)
        self.approveRequest(requestName)
        if config.assignRequests:
            # if --assignRequests at the same time, it will be checking requestNames
            config.requestNames = [requestName]
        return requestName
        

    def approveRequest(self, requestName):
        """
        Set request status assignment-approved of the requestName request.
        Once ReqMgr provides proper API for status settings, esp. for assignment,
        a single method setStates shall handle all request status changes.
        
        """
        params = {"requestName": requestName,
                  "status": "assignment-approved"}
        encodedParams = urllib.urlencode(params)
        logging.info("Approving request '%s' ..." % requestName)
        status, data = self._httpRequest("PUT", "/reqmgr/reqMgr/request",
                                         data=encodedParams, headers=self.textHeaders)
        if status != 200:
            logging.error("Approve did not succeed.")
            print data
            sys.exit(1)
        logging.info("Approve succeeded.")
            
            
    def assignRequests(self, config):
        """
        It seems that the assignment doens't have proper REST API.
        Do via web page (as in the original script).
        This is why the items
            "action": "Assign"
            "Team"+team: "checked"
            "checkbox"+workflow: "checked"
            have to be hacked here, as if they were ticked on a web form.
            This hack is the reason why the requestArgs have to get
            to this method deep-copied if subsequent request assignment happens.
            
        """
        def doAssignRequest(assignArgs, requestName):
            assignArgs["action"] = "Assign"        
            team = assignArgs["Team"]
            assignArgs["Team" + team] = "checked"
            assignArgs["checkbox" + requestName] = "checked"
            # have to remove this one, otherwise it will get confused with "Team+team"
            # TODO this needs to be put right with proper REST interface
            del assignArgs["Team"]
            encodedParams = urllib.urlencode(assignArgs, True)
            logging.info("Assigning request '%s' ..." % requestName)
            status, data = self._httpRequest("POST", "/reqmgr/assign/handleAssignmentPage",
                                             data=encodedParams, headers=self.textHeaders)
            if status != 200:
                logging.error("Assign did not succeed.")
                print data
                sys.exit(1)
            logging.info("Assign succeeded.")

        for requestName in config.requestNames:
            assignArgs = copy.deepcopy(config.requestArgs["assignRequest"])
            doAssignRequest(assignArgs, requestName)
        
                                
    def userGroup(self, _):
        """
        List all groups and users registered with Request Manager.
        
        """
        logging.info("Querying registered groups ...")
        status, data = self._httpRequest("GET", "/reqmgr/reqMgr/group")
        groups = json.loads(data)
        logging.info(data)
        logging.info("Querying registered users ...")
        status, data = self._httpRequest("GET", "/reqmgr/reqMgr/user")
        logging.info(data)
        logging.info("Querying groups membership ...")
        for group in groups:
            status, data = self._httpRequest("GET", "/reqmgr/reqMgr/group/%s" % group)
            logging.info("Group: '%s': %s" % (group, data))
            
    
    def team(self, _):
        logging.info("Querying registered teams ...")
        status, data = self._httpRequest("GET", "/reqmgr/reqMgr/team")
        groups = json.loads(data)
        logging.info(data)
            
            
    def queryRequests(self, config):
        requestsData = []
        if config.requestNames:
            for requestName in config.requestNames:
                logging.info("Querying '%s' request ..." % requestName)
                status, data = self._httpRequest("GET", "/reqmgr/reqMgr/request/%s" % requestName)
                if status != 200:
                    print data
                    sys.exit(1)           
                request = json.loads(data)
                for k, v in sorted(request.items()):
                    print "\t%s: %s" % (k, v)
                requestsData.append(request)
            # returns data on requests in the same order as in the config.requestNames
            return requestsData
        else:
            logging.info("Querying all requests ...")
            status, data = self._httpRequest("GET", "/reqmgr/reqMgr/requestnames")
            if status != 200:
                print data
                sys.exit(1)
            requests = json.loads(data)
            for request in requests:
                print request
            logging.info("%s requests in the system." % len(requests))
            return requests
            

    def deleteRequests(self, config):
        for requestName in config.requestNames:
            logging.info("Deleting '%s' request ..." % requestName)
            status, data = self._httpRequest("DELETE", "/reqmgr/reqMgr/request/%s" % requestName)
            if status != 200:
                print data
                sys.exit(1)
            logging.info("Done.")           

    
    def cloneRequest(self, config):
        requestName = config.cloneRequest
        logging.info("Cloning request '%s' ..." % requestName)
        headers = {"Content-Length": 0}
        status, data = self._httpRequest("PUT", "/reqmgr/reqMgr/clone/%s" % requestName, 
                                         headers=headers)
        if status > 216:
            logging.error("Error occurred, exit.")
            print data  
            sys.exit(1)
        data = json.loads(data)
        # ReqMgr returns dictionary with key: 'WMCore.RequestManager.DataStructs.Request.Request'
        # print data
        newRequestName = data.values()[0]["RequestName"] 
        logging.info("Clone request succeeded: original request name: '%s' "
                     "new request name: '%s'" % (requestName, newRequestName))
        return newRequestName
    
    
    def changePriority(self, requestName, priority):
        """
        Test changing request priority.
        It's not exposed to the command line usage, it's used only in allTests()
        
        """
        logging.info("Changing request priority: %s for %s ..." % (priority, requestName))
        # this approach should also be possible:
        # jsonSender.put("request/%s?priority=%s" % (requestName, priority))
        # "requestName": requestName can probably be specified here as well
        params = {"priority": "%s" % priority}
        encodedParams = urllib.urlencode(params)
        status, data = self._httpRequest("PUT", "/reqmgr/reqMgr/request/%s" % requestName,
                                         data=encodedParams, headers=self.textHeaders)        
        if status > 200:
            logging.error("Error occurred, exit.")
            print data
            sys.exit(1)
        
    
    def allTests(self, config):
        """
        Call all methods above. Tests everything.
        Checks that the ReqMgr instance has the same state before 
        and after this script.
                
        """
        self.userGroup(None) # argument has no meaning
        self.team(None) # argument has no meaning
        currentRequests = self.queryRequests(config)
        requestNames = []
        config.assignRequests = True # createRequest will subsequently also assignRequests
        requestNames.append(self.createRequest(config, restApi = True))
        requestNames.append(self.createRequest(config, restApi = False))
        config.requestNames = requestNames        
        self.queryRequests(config)
        # test priority changing (final priority will be sum of the current
        # and new, so have to first find out the current)
        # config.requestNames must be set
        requests = self.queryRequests(config)
        currPriority = requests[0]["RequestPriority"]
        newPriority = 10
        totalPriority = currPriority + newPriority
        self.changePriority(requestNames[0], newPriority)
        requests = self.queryRequests(config)
        assert requests[0]["RequestPriority"] == totalPriority, "New RequestPriority does not match!"
        # test clone
        config.cloneRequest = requestNames[0] # clone the first request in the list
        clonedRequestName = self.cloneRequest(config)
        requestNames.append(clonedRequestName)
        config.requestNames = requestNames
        # now test that the cloned request has correct priority
        requests = self.queryRequests(config)
        # last from the returned result is the cloned request
        clonedRequest = requests[-1]
        msg = ("Priorities don't match: original request: %s cloned request: %s" %
               (totalPriority, clonedRequest["RequestPriority"]))
        assert totalPriority == clonedRequest["RequestPriority"], msg
        self.deleteRequests(config)
        logging.info("%s requests in the system before this test." % len(currentRequests))
        config.requestNames = None # this means queryRequests will check all requests
        afterRequests = self.queryRequests(config)
        logging.info("%s requests in the system before this test." % len(afterRequests))
        assert currentRequests == afterRequests, "Requests in ReqMgr before and after this test not matching!"
        logging.info("Running --allTests succeeded.")
                
    
    def __del__(self):
        self.conn.close()
        del self.conn
    

# ---------------------------------------------------------------------------    


def processCmdLine(args):
    def errExit(msg, parser):
        print('\n')
        parser.print_help()
        print("\n\n%s" % msg)
        sys.exit(1)
        
    form = TitledHelpFormatter(width=78)
    parser = OptionParser(usage="usage: %prog options", formatter=form, add_help_option=None)
    actions = defineCmdLineOptions(parser)
    # opts - new processed options
    # args - remainder of the input array
    opts, args = parser.parse_args(args=args)
    # check command line arguments validity
    if not opts.reqMgrUrl:
        errExit("Missing mandatory --reqMgrUrl.", parser)
    if opts.createRequest and not opts.configFile:
        errExit("When --createRequest, --configFile is necessary.", parser)
    if opts.assignRequests and not opts.createRequest and not opts.configFile:
        errExit("Without --createRequest, --configFile must be specified for --assignRequests.", parser)
    if opts.assignRequests and not opts.createRequest and not opts.requestNames:
        errExit("Without --createRequest, --requestNames must be supplied to --assignRequests.", parser)
    if not opts.requestNames and (opts.queryRequests or opts.deleteRequests or \
                                  (opts.assignRequests and not opts.createRequest)):
        errExit("--requestNames must be supplied.", parser)
    if opts.createRequest and opts.requestNames:
        errExit("--requestNames can't be provided with --createRequest", parser)
    if opts.allTests and not opts.configFile:
        errExit("When --allTests, --configFile is necessary", parser)
    if (opts.json and not opts.createRequest) and (opts.json and not opts.allTests) \
        and (opts.json and not opts.assignRequests):
        errExit("--json only with --createRequest, --allTests, --assignRequest", parser)
    for action in filter(lambda name: getattr(opts, name), actions):
        if opts.allTests and action and action != "allTests":
            errExit("Arguments --allTests and --%s mutually exclusive." % action, parser)
    if opts.requestNames:
        # make it a list here
        opts.requestNames = opts.requestNames.split(',')
    return opts, actions


def defineCmdLineOptions(parser):
    actions = []
    # "-h" ------------------------------------------------------------------
    help = "Display this help"
    parser.add_option("-h", "--help", help=help, action='help')
    # "-c" ------------------------------------------------------------------
    help = ("User cert file (or cert proxy file). "
            "If not defined, tries X509_USER_CERT then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_option("-c", "--cert", help=help)    
    # "-k" ------------------------------------------------------------------
    help = ("User key file (or cert proxy file). "
            "If not defined, tries X509_USER_KEY then X509_USER_PROXY env. "
            "variables. And lastly /tmp/x509up_uUID.")
    parser.add_option("-k", "--key", help=help)
    # -u --------------------------------------------------------------------
    help = ("Request Manager service address (if not options is supplied, "
            "returns a list of the requests in ReqMgr) "
            "e.g.: https://maxareqmgr01.cern.ch")
    parser.add_option("-u", "--reqMgrUrl", help=help)
    # -f --------------------------------------------------------------------
    help = "Request create and/or assign arguments config file."
    parser.add_option("-f", "--configFile", help=help)
    # -j --------------------------------------------------------------------
    help = ("JSON string to override values from --configFile. "
            "e.g. --json=\'{\"createRequest\": {\"Requestor\": \"efajardo\"}, "
            "\"assignRequest\": {\"FirstLumi\": 1}}\' "
            "e.g. --json=`\"cat alan.json\"`")
    parser.add_option("-j", "--json", help=help)
    # -r --------------------------------------------------------------------
    help = ("Request name or list of comma-separated names to perform "
            "actions upon.")
    parser.add_option("-r", "--requestNames", help=help)
    # -q --------------------------------------------------------------------
    help = "Action: Query request(s) specified by --requestNames."
    action = "queryRequests"
    actions.append(action)
    parser.add_option("-q", "--" + action, action="store_true", help=help)
    # -d --------------------------------------------------------------------
    help = "Action: Delete request(s) specified by --requestNames."
    action = "deleteRequests"
    actions.append(action)
    parser.add_option("-d", "--" + action, action="store_true", help=help)
    # -i --------------------------------------------------------------------
    help = ("Action: Create and inject a request. Whichever from the config "
            "file defined arguments can be overridden from "
            "command line and a few have to be so (*-OVERRIDE-ME ending). "
            "Depends on --configFile. "
            "This request can be 'approved' and 'assigned' if --assignRequests.")
    action = "createRequest"
    actions.append(action)  
    parser.add_option("-i", "--" + action, action="store_true", help=help)
    # TODO
    # once ReqMgr has proper REST API for assign, then implement --setStates
    # taking a list of states to route requests through
    # -s --------------------------------------------------------------------
    help = ("Action: Approve and assign request(s) specified by --requestNames "
            "or a new request when used with --createRequest. "
            "Depends on --requestNames and --configFile when used without "
            "--createRequest")
    action = "assignRequests"
    actions.append(action)
    parser.add_option("-g", "--" + action, action="store_true", help=help)
    # -l --------------------------------------------------------------------
    help = "Action: Clone request, the request name is mandatory argument."
    action = "cloneRequest"
    actions.append(action)
    parser.add_option("-l", "--" + action, help=help)
    # -a --------------------------------------------------------------------
    help = ("Action: Perform all operations this script allows. "
            "--configFile and possibly --json must be present for initial "
            "request injection and assignment.")
    action = "allTests"
    actions.append(action)
    parser.add_option("-a", "--" + action, action="store_true", help=help)
    # -s --------------------------------------------------------------------
    # TODO
    # this will be removed once ReqMgr takes this internal user management
    # information from SiteDB, only teams will remain
    help = "Action: List groups and users registered with ReqMgr instance."
    action = "userGroup"
    actions.append(action)
    parser.add_option("-s", "--" + action,  action="store_true", help=help)
    # -t --------------------------------------------------------------------
    help = "Action: List teams registered with a Request Manager instance."
    action = "team"
    actions.append(action)
    parser.add_option("-t", "--" + action,  action="store_true", help=help)
    # -v ---------------------------------------------------------------------\
    help = "Verbose console output."
    parser.add_option("-v", "--verbose",  action="store_true", help=help)    
    return actions
    
    
def processRequestArgs(intputConfigFile, commandLineJson):
    """
    Load request arguments from a file, blend with JSON from command line.
    
    """
    logging.info("Loading file '%s' ..." % intputConfigFile)
    try:
        requestArgs = json.load(open(intputConfigFile, 'r'))
    except IOError as ex:
        logging.fatal("Reading arguments file '%s' failed, "
                      "reason: %s." % (intputConfigFile, ex))
        sys.exit(1)
    if commandLineJson:
        logging.info("Parsing request arguments on the command line ...")
        cliJson = json.loads(commandLineJson)
        # if a key exists in cliJson, update values in the main requestArgs dict
        for k in requestArgs.keys():
            if cliJson.has_key(k):
                requestArgs[k].update(cliJson[k])            
    else:
        logging.warn("No request arguments to override (--json)? Some values will be wrong.")
        
    # iterate over all items recursively and warn about those ending with 
    # OVERRIDE-ME, hence not overridden
    def check(items):
        for k, v in items:
            if isinstance(v, dict):
                check(v.items())
            if isinstance(v, unicode) and v.endswith("OVERRIDE-ME"):
                logging.warn("Not properly set: %s: %s" % (k, v))
    check(requestArgs.items())
    return requestArgs
        
    
def initialization(commandLineArgs):
    print("Processing command line arguments: '%s' ..." % commandLineArgs)
    config, actions = processCmdLine(commandLineArgs)
    logging.basicConfig(level=logging.DEBUG if config.verbose else logging.INFO)
    logging.debug("Set verbose console output.")
    logging.info("Getting user identity files ...")
    proxyFile = "/tmp/x509up_u%s" % os.getuid()
    if not os.path.exists(proxyFile):
        proxyFile = "UNDEFINED" 
    certFile = config.cert or os.getenv("X509_USER_CERT", os.getenv("X509_USER_PROXY", proxyFile)) 
    keyFile = config.key or os.getenv("X509_USER_KEY", os.getenv("X509_USER_PROXY", proxyFile)) 
    reqMgrClient = ReqMgrClient(config.reqMgrUrl, certFile, keyFile)
    if config.createRequest or config.assignRequests or config.allTests:
        # process request arguments and store them
        config.requestArgs = processRequestArgs(config.configFile, config.json)
    return reqMgrClient, config, actions
    

def main():
    reqMgrClient, config, definedActions = initialization(sys.argv)
    # definedAction are all actions as defined for CLI
    # there is now gonna be usually 1 action to perform, but could be more
    # filter out those where config.ACTION is None
    # config is all options for this script but also request creation parameters
    actions = filter(lambda name: getattr(config, name), definedActions)
    logging.info("Actions to perform: %s" % actions) 
    for action in actions:
        logging.info("Performing '%s' ..." % action)
        # some methods need to modify config (e.g. add a request name),
        # pass them entire configuration
        reqMgrClient.__getattribute__(action)(config)
    if not actions:
        reqMgrClient.queryRequests(config)
        
    
if __name__ == "__main__":
    main()