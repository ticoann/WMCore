#!/usr/bin/python

"""
This is the Dashboard API Module for the Worker Node
"""

from WMCore.Services.Dashboard import apmon
from types import DictType, ListType

#
# Methods for manipulating the apmon instance
#

# Internal attributes
apmonInstance = None
apmonInit = False

# Monalisa configuration
apmonConf = ["cms-wmagent-job.cern.ch"]

# private function converting unitcode to str
def __checkAndConvertToStr(value):
    if not isinstance(value, basestring) :
        return 'unknown'
    try:
        return str(value)
    except UnicodeEncodeError:
        #This contains some unicode outside ascii range
        return 'unknown'

#
# Method to create a single apmon instance at a time
#
def getApmonInstance( logr, apmonServer ):
    global apmonInstance
    global apmonInit
    if apmonInstance is None and not apmonInit :
        apmonInit = True
        if apmonInstance is None :
            try :
                if not apmonServer:
                    apmonInstance = apmon.ApMon(apmonConf, logr) #apmonLoggingLevel)
                else:
                    apmonInstance = apmon.ApMon(apmonServer, logr)
            except Exception as e :
                pass
    return apmonInstance

#
# Method to free the apmon instance
#
def apmonFree() :
    global apmonInstance
    global apmonInit
    if apmonInstance is not None :
        try :
            apmonInstance.free()
        except Exception as e :
            pass
        apmonInstance = None
    apmonInit = False

#
# Method to send params to Monalisa service
#
def apmonSend(taskid, jobid, params, logr, apmonServer) :
    apm = getApmonInstance( logr, apmonServer )
    if apm is not None :
        if not isinstance(params, DictType) and not isinstance(params, ListType) :
            params = {'unknown' : '0'}
        taskid = __checkAndConvertToStr(taskid)
        jobid = __checkAndConvertToStr(jobid)
        try :
            apm.sendParameters(taskid, jobid, params)
            return 0
        except Exception as e:
            pass
    return 1
