"""
Perform cleanup actions
"""
__all__ = []



import threading
import logging
import time
import traceback
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Database.CMSCouch import CouchMonitor
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueService
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMComponent.AnalyticsDataCollector.DataCollectAPI import LocalCouchDBData, \
     WMAgentDBData, combineAnalyticsData, convertToRequestCouchDoc, \
     convertToAgentCouchDoc, isDrainMode
from WMCore.WMFactory import WMFactory

class AgentStatusPoller(BaseWorkerThread):
    """
    Gether the summary data for request (workflow) from local queue,
    local job couchdb, wmbs/boss air and populate summary db for monitoring
    """
    def __init__(self, config):
        """
        initialize properties specified from config
        """
        BaseWorkerThread.__init__(self)
        # set the workqueue service for REST call
        self.config = config
        self.agentInfo = {}
        self.agentInfo['agent_team'] = config.Agent.teamName
        self.agentInfo['agent'] = config.Agent.agentName
        # temporarly add port for the split test
        self.agentInfo['agent_url'] = ("%s:%s" % (config.Agent.hostName, config.WMBSService.Webtools.port))
        # need to get campaign, user, owner info
        self.agentDocID = "agent+hostname"
        
            
    def setup(self, parameters):
        """
        set db connection(couchdb, wmbs) to prepare to gather information
        """

        # interface to WMBS/BossAir db
        myThread = threading.currentThread()
        # set wmagent db data
        self.wmagentDB = WMAgentDBData(self.summaryLevel, myThread.dbi, myThread.logger)
       
        self.localCouchServer = CouchMonitor(self.config.JobStateMachine.couchurl)
        

    def algorithm(self, parameters):
        """
        get information from wmbs, workqueue and local couch
        """
        try:
            logging.info("Getting Agent info ...")
            agentInfo = self.collectAgentInfo()
            
            #set the uploadTime - should be the same for all docs
            uploadTime = int(time.time())
            
            self.uploadAgentInfoToCentralWMStats(agentInfo, uploadTime)
            
            logging.info("Agent status update success\n %s agents \nsleep for next cycle" % len(agentInfo))

        except Exception, ex:
            logging.error("Error occurred, will retry later:")
            logging.error(str(ex))
            logging.error("Trace back: \n%s" % traceback.format_exc())
    
    def collectAgentInfo(self):
        #TODO: agent info (need to include job Slots for the sites)
        # always checks couch first
        source = self.config.JobStateMachine.jobSummaryDBName
        target = self.config.AnalyticsDataCollector.centralWMStatsURL
        couchInfo = self.localCouchServer.recoverReplicationErrors(source, target)
        logging.info("getting couchdb replication status: %s" % couchInfo)
        
        agentInfo = self.wmagentDB.getComponentStatus(self.config)
        agentInfo.update(self.agentInfo)
        
        if (couchInfo['status'] != 'ok'):
            agentInfo['down_components'].append("CouchServer")
            agentInfo['status'] = couchInfo['status']
            couchInfo['name'] = "CouchServer"
            agentInfo['down_component_detail'].append(couchInfo)
        
        if isDrainMode():
            logging.info("Agent is in DrainMode")
            agentInfo['drain_mode'] = True
            agentInfo['status'] = "warning"
        else:
            agentInfo['drain_mode'] = False
            
        return agentInfo

    def uploadAgentInfoToCentralWMStats(self, agentInfo, uploadTime):
        #direct data upload to the remote to prevent data conflict when agent is cleaned up and redeployed
        agentDocs = convertToAgentCouchDoc(agentInfo, self.config.ACDC, uploadTime)
        self.centralWMStatsCouchDB.updateAgentInfo(agentDocs)
        logging.info("Agent data direct upload success\n %s request" % len(agentDocs))

