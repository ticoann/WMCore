from __future__ import (division, print_function)

from Utils.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig

class LocationUpdateTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.updateDataLocation, 'duration': config.locationUpdateDuration}]

    def updateDataLocation(self, config):
        """
        gather active data statistics
        """
        
        globalQ = queueFromConfig(config)
        globalQ.updateLocationInfo()
                  
        return
