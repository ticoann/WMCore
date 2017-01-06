from __future__ import (division, print_function)

from Utils.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig

class CleanUpTask(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.cleanUpAndSyncCanceledElements, 'duration': config.cleanUpDuration}]

    def cleanUpAndSyncCanceledElements(self, config):
        """
        
        1. deleted the wqe in end states
        2. synchronize cancelled elements.
        We can also make this in the separate thread
        """
        
        globalQ = queueFromConfig(config)
        globalQ.performQueueCleanupActions(skipWMBS=True)
                  
        return