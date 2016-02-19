from __future__ import (division, print_function)

from WMCore.Services.WMStats.WMStatsReader import WMStatsReader
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.WMStats.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask

class CleanUpTask(CherryPyPeriodicTask):
    """
    This class is used for both T0WMStats and WMStats
    controlled by config.reqdb_couch_app value
    """

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)
        self.wmstatsDB = WMStatsWriter(config.wmstats_url, reqdbURL=config.reqmgrdb_url, 
                              reqdbCouchApp=config.reqdb_couch_app)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.cleanUpOldRequests, 'duration': (config.DataKeepDays * 24 * 60 * 60)},
                                {'func': self.cleanUpArchivedRequests, 'duration': config.archivedCleanUpDuration}]

    def cleanUpOldRequests(self, config):
        """
        clean up wmstats data older then given days
        """
        self.logger.info("deleting %s hours old docs" % (config.DataKeepDays * 24))
        result = self.wmstatsDB.deleteOldDocs(config.DataKeepDays)
        self.logger.info("%s old doc deleted" % result)
        return
    
    def cleanUpArchivedRequests(self, config):
        """
        loop through the workflows in couchdb, if archived delete all the data in couchdb
        """
        
        requestNames = self.wmstatsDB.getArchivedRequests()
        for req in requestNames:
            self.logger.info("deleting %s data" % req)
            result = self.wmstatsDB.deleteDocsByWorkflow(req)
            self.logger.info("%s deleted" % result)
        return