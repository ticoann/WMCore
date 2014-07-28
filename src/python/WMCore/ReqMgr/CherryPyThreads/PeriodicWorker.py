'''
Created on Jul 31, 2014

@author: sryu
'''
import cherrypy
from threading import Thread, Condition

class PeriodicWorker(Thread):
    
    def __init__(self, func, config):
        # use default RLock from condition
        # Lock wan't be shared between the instance used  only for wait
        # func : function or callable object pointer
        self.wakeUp = Condition()
        self.stopFlag = False
        self.taskFunc = func
        self.config = config
        self.duration = 5 or config.duration
        try: 
            name = func.__class__.__name__
            print name
        except:
            name = func.__name__
            print name
        Thread.__init__(self, name=name)
        cherrypy.engine.subscribe('start', self.start, priority = 100)
        cherrypy.engine.subscribe('stop', self.stop, priority = 100)
    
        
    def stop(self):
        self.wakeUp.acquire()
        self.stopFlag = True
        self.wakeUp.notifyAll()
        self.wakeUp.release()
    
    def isStopFlagOn(self):
        # this function can be used if the work needs to be gracefully 
        # shut down by setting the several stopping point in the self.taskFunc
        return self.stopFlag
    
    def run(self):
        
        while not self.stopFlag:
            self.wakeUp.acquire()
            self.taskFunc(self.config, self.isStopFlagOn)
            self.wakeUp.wait(self.duration)
            self.wakeUp.release()
         
import logging
    
class SequentialTaskBase(object):
    
    def __init__(self):
        self.setCallSequence()
        
    def __call__(self, config, stopFlagFunc):
        for call in self._callSequence:
            if stopFlagFunc():
                return
            try:
                call(config)
            except Exception, ex:
                #log the excpeiotn and break. 
                #SequencialTasks are interconnected between functions  
                print (str(ex))
                logging.error(str(ex))
                break
            
    def setCallSequence(self):
        """
        set the list of function call with out args on self.callSequence
        
        i.e.
        self.callSequence = [self.do_something1, self.do_something1]
        """
        raise NotImplementedError("need to implement setCallSequence assign self._callSequence")

   
#this is the sckeleton of request data collector
class DataUploadTask(SequentialTaskBase):
    
    def setCallSequence(self):
        self._callSequence = [self.getData, self.convertData, self.putData]
    
    def getData(self, config):
        # self.data = getData(self.sourceUrl)
        pass
    
    def convertData(self, config):
        # self.data = convertData(self.data)
        pass
    
    def putData(self, config):
        # putData(self.destUrl)
        pass
    
class Hello(SequentialTaskBase):
    
    
    def setCallSequence(self):
        self._callSequence = [self.printHello, self.printThere, self.printA]
    
    def printHello(self, config):
        print "Hello"
        
    def printThere(self, config):
        print "there"
    
    def printA(self, config):
        print "A"
    

def sayHello(config, stopFlagFunc):
    print "Hello func"

def sayBye(config, stopFlagFunc):
    print "Bye func"
    

class CherryPyPeridoicTask(object):
    
    def __init__(self, config):
        
        self.setSequencialTask()
        for task in self.concurrentTasks:
            PeriodicWorker(task, config)
        
    def setConcurrentTasks(self):
        """
        """
        self.concurrentTasks = []
        raise NotImplementedError("need to implement setSequencialTas assign self._callSequence")

if __name__ == '__main__':
    import cherrypy
    helloTask = PeriodicWorker(sayHello, 5)
    byeTask = PeriodicWorker(sayBye, 10)
    aTask = PeriodicWorker(Hello(), 5)
    cherrypy.quickstart()