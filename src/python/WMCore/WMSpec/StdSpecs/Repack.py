"""
_Repack_

Repack workflow

repacking -> RAW -> optional merge
             (supports multiple output with different primary datasets)

"""

from __future__ import division

from Utils.Utilities import makeList
from WMCore.Lexicon import procstringT0, activity

from WMCore.WMSpec.StdSpecs.StdBase import StdBase

class RepackWorkloadFactory(StdBase):
    """
    _RepackWorkloadFactory_

    Stamp out Repack workflows.
    """

    def __init__(self):
        StdBase.__init__(self)

        self.inputPrimaryDataset = None
        self.inputProcessedDataset = None

        return

    def buildWorkload(self):
        """
        _buildWorkload_

        Build the workload given all of the input parameters.  At the very least
        this will create a processing task and merge tasks for all the outputs
        of the processing task.

        Not that there will be LogCollect tasks created for each processing
        task and Cleanup tasks created for each merge task.

        """
        workload = self.createWorkload()
        workload.setDashboardActivity("tier0")
        self.reportWorkflowToDashboard(workload.getDashboardActivity())

        cmsswStepType = "CMSSW"
        taskType = "Processing"

        # complete output configuration
        for output in self.outputs:
            output['moduleLabel'] = "write_%s_%s" % (output['primaryDataset'],
                                                     output['dataTier'])

        # finalize splitting parameters
        mySplitArgs = self.repackSplitArgs.copy()
        mySplitArgs['algo_package'] = "T0.JobSplitting"

        repackTask = workload.newTask("Repack")
        repackOutMods = self.setupProcessingTask(repackTask, taskType,
                                                 scenarioName = self.procScenario,
                                                 scenarioFunc = "repack",
                                                 scenarioArgs = { 'outputs' : self.outputs },
                                                 splitAlgo = "Repack",
                                                 splitArgs = mySplitArgs,
                                                 stepType = cmsswStepType)

        repackTask.setTaskType("Repack")

        self.addLogCollectTask(repackTask)

        for repackOutLabel in repackOutMods.keys():
            self.addRepackMergeTask(repackTask, repackOutLabel)

        workload.setBlockCloseSettings(self.blockCloseDelay,
                                       workload.getBlockCloseMaxFiles(),
                                       workload.getBlockCloseMaxEvents(),
                                       workload.getBlockCloseMaxSize())

        # setting the parameters which need to be set for all the tasks
        # sets acquisitionEra, processingVersion, processingString
        workload.setTaskPropertiesFromWorkload()

        # set the LFN bases (normally done by request manager)
        # also pass run number to add run based directories
        workload.setLFNBase(self.mergedLFNBase, self.unmergedLFNBase,
                            runNumber = self.runNumber)

        return workload

    def addRepackMergeTask(self, parentTask, parentOutputModuleName):
        """
        _addRepackMergeTask_

        Create an repackmerge task for files produced by the parent task.

        """
        mergeTask = parentTask.addTask("%sMerge%s" % (parentTask.name(), parentOutputModuleName))
        self.addDashboardMonitoring(mergeTask)
        mergeTaskCmssw = mergeTask.makeStep("cmsRun1")
        mergeTaskCmssw.setStepType("CMSSW")

        mergeTaskStageOut = mergeTaskCmssw.addStep("stageOut1")
        mergeTaskStageOut.setStepType("StageOut")
        mergeTaskLogArch = mergeTaskCmssw.addStep("logArch1")
        mergeTaskLogArch.setStepType("LogArchive")

        mergeTask.setTaskLogBaseLFN(self.unmergedLFNBase)

        self.addLogCollectTask(mergeTask, taskName = "%s%sMergeLogCollect" % (parentTask.name(), parentOutputModuleName))

        mergeTask.applyTemplates()

        parentTaskCmssw = parentTask.getStep("cmsRun1")
        parentOutputModule = parentTaskCmssw.getOutputModule(parentOutputModuleName)

        mergeTask.setInputReference(parentTaskCmssw, outputModule = parentOutputModuleName)

        mergeTaskCmsswHelper = mergeTaskCmssw.getTypeHelper()

        mergeTaskCmsswHelper.cmsswSetup(self.frameworkVersion, softwareEnvironment = "",
                                        scramArch = self.scramArch)

        mergeTaskCmsswHelper.setErrorDestinationStep(stepName = mergeTaskLogArch.name())
        mergeTaskCmsswHelper.setGlobalTag(self.globalTag)
        mergeTaskCmsswHelper.setOverrideCatalog(self.overrideCatalog)

        #mergeTaskStageHelper = mergeTaskStageOut.getTypeHelper()
        #mergeTaskStageHelper.setMinMergeSize(0, 0)

        mergeTask.setTaskType("Merge")

        # finalize splitting parameters
        mySplitArgs = self.repackMergeSplitArgs.copy()
        mySplitArgs['algo_package'] = "T0.JobSplitting"

        mergeTask.setSplittingAlgorithm("RepackMerge",
                                        **mySplitArgs)
        mergeTaskCmsswHelper.setDataProcessingConfig(self.procScenario, "merge")

        self.addOutputModule(mergeTask, "Merged",
                             primaryDataset = getattr(parentOutputModule, "primaryDataset"),
                             dataTier = getattr(parentOutputModule, "dataTier"),
                             filterName = getattr(parentOutputModule, "filterName"),
                             forceMerged = True)

        self.addOutputModule(mergeTask, "MergedError",
                             primaryDataset = getattr(parentOutputModule, "primaryDataset") + "-Error",
                             dataTier = getattr(parentOutputModule, "dataTier"),
                             filterName = getattr(parentOutputModule, "filterName"),
                             forceMerged = True)

        self.addCleanupTask(parentTask, parentOutputModuleName)

        return mergeTask

    def __call__(self, workloadName, arguments):
        """
        _call_

        Create a Repack workload with the given parameters.
        """
        StdBase.__call__(self, workloadName, arguments)

        # Required parameters that must be specified by the Requestor.
        self.outputs = arguments['Outputs']

        # job splitting parameters
        self.repackSplitArgs = {}
        self.repackSplitArgs['maxSizeSingleLumi'] = arguments['MaxSizeSingleLumi']
        self.repackSplitArgs['maxSizeMultiLumi'] = arguments['MaxSizeMultiLumi']
        self.repackSplitArgs['maxInputEvents'] = arguments['MaxInputEvents']
        self.repackSplitArgs['maxInputFiles'] = arguments['MaxInputFiles']
        self.repackSplitArgs['maxLatency'] = arguments['MaxLatency']
        self.repackMergeSplitArgs = {}
        self.repackMergeSplitArgs['minInputSize'] = arguments['MinInputSize']
        self.repackMergeSplitArgs['maxInputSize'] = arguments['MaxInputSize']
        self.repackMergeSplitArgs['maxEdmSize'] = arguments['MaxEdmSize']
        self.repackMergeSplitArgs['maxOverSize'] = arguments['MaxOverSize']
        self.repackMergeSplitArgs['maxInputEvents'] = arguments['MaxInputEvents']
        self.repackMergeSplitArgs['maxInputFiles'] = arguments['MaxInputFiles']
        self.repackMergeSplitArgs['maxLatency'] = arguments['MaxLatency']

        return self.buildWorkload()

    @staticmethod
    def getWorkloadCreateArgs():
        baseArgs = StdBase.getWorkloadCreateArgs()
        specArgs = {"RequestType": {"default": "Repack"},
                    "ConfigCacheID": {"optional": True, "null": True},
                    "Scenario": {"default": "fake", "attr": "procScenario"},
                    "GlobalTag": {"default": "fake"},
                    "ProcessingString": {"default": "", "validate": procstringT0},
                    "BlockCloseDelay": {"type": int, "optional": False,
                                        "validate": lambda x : x > 0},
                    "Outputs": {"type": makeList, "optional": False},
                    "MaxSizeSingleLumi": {"type": int, "optional": False},
                    "MaxSizeMultiLumi": {"type": int, "optional": False},
                    "MaxInputEvents": {"type": int, "optional": False},
                    "MaxInputFiles": {"type": int, "optional": False},
                    "MaxLatency": {"type": int, "optional": False},
                    "MinInputSize": {"type": int, "optional": False},
                    "MaxInputSize": {"type": int, "optional": False},
                    "MaxEdmSize": {"type": int, "optional": False},
                    "MaxOverSize": {"type": int, "optional": False},
                    "Dashboard": {"default": "tier0", "type": str, "validate": activity}
                    }
        baseArgs.update(specArgs)
        StdBase.setDefaultArgumentsProperty(baseArgs)
        return baseArgs
