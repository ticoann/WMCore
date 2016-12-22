from __future__ import (division, print_function)

from Utils.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
from WMCore.WorkQueue.WorkQueueReqMgrInterface import WorkQueueReqMgrInterface

class ReqMgrInteractionTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.interactWithReqmgr, 'duration': config.interactDuration}]

    def interactWithReqmgr(self, config):
        """
        
        1. pull new work
        2. add the new element from running-open request
        3. report element status to reqmgr (need to be removed and set as reqmgr task)
        4. record this activity
        """
        
        globalQ = queueFromConfig(config)
        reqMgrConfig = config.WorkQueueManager.reqMgrConfig
        reqMgrInt = WorkQueueReqMgrInterface(**reqMgrConfig)
        reqMgrInt(globalQ)
                  
        return