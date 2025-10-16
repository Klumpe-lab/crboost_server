
```
ᢹ CBE-login [dev/CryoBoost] tree -L 3  -I 'projects|venv|__pycache__'\\

.

├── bin
│   ├── crboost_extract_tm_candidates.py

│   ├── crboost_filterTitlts_Interactive.py

│   ├── crboost_filterTitlts.py

│   ├── crboost_match_template.py

│   ├── crboost_neighbourAnalysis.py

│   ├── crboost_pipe.py

│   ├── crboost_python

│   ├── crboost_warp_fs_motion_and_ctf.py

│   ├── crboost_warp_ts_alignment.py

│   ├── crboost_warp_ts_ctf.py

│   └── crboost_warp_ts_reconstruct.py

├── config

│   ├── conf.yaml

│   ├── qsub

│   │   ├── qsub_relion_hpcl89.sh

│   │   └── qsub_warp_hpcl89.sh

│   └── Schemes

│       ├── relion_tomo_prep

│       └── warp_tomo_prep

├── data

│   ├── models

│   │   ├── fastAiToPkl.py

│   │   ├── modelNativeFastAi.pkl

│   │   └── model.pkl

│   ├── pickLists

│   │   └── candidatesPytomRel5.star

│   ├── tilts

│   │   ├── mdoc

│   │   ├── tiltImg

│   │   ├── tilt_series

│   │   └── tilt_series_ctf.star

│   └── vols

│       ├── copia_11.8A.mrc

│       ├── copiaBlack_11.8A.mrc

│       ├── mask_copia_11.8A.mrc

│       ├── mask.mrc

│       ├── Position_1.mrc

│       ├── Position_2.mrc

│       ├── rec_Position_1_job.json

│       ├── tilt_series

│       └── tomograms.star

├── docs

│   ├── conf.py

│   ├── extdocs

│   │   ├── intern

│   │   ├── setup

│   │   └── tutorial

│   ├── index.rst

│   ├── make.bat

│   ├── Makefile

│   ├── modules.rst

│   ├── requirements.txt

│   ├── src.deepLearning.rst

│   ├── src.filterTilts.rst

│   ├── src.gui.rst

│   ├── src.misc.rst

│   ├── src.pipe.rst

│   ├── src.rst

│   ├── src.rw.rst

│   └── tests.rst

├── README.md

├── src

│   ├── deepLearning

│   │   ├── __init__.py

│   │   ├── modelClasses.py

│   │   └── predictTilts_Binary.py

│   ├── filterTilts

│   │   ├── filterTiltsDL.py

│   │   ├── filterTiltsInt.py

│   │   ├── filterTiltsRule.py

│   │   ├── __init__.py

│   │   └── libFilterTilts.py

│   ├── gui

│   │   ├── edit_scheme.py

│   │   ├── edit_scheme.ui

│   │   ├── generateTemplate.py

│   │   ├── __init__.py

│   │   ├── libGui.py

│   │   ├── quick_setup.py

│   │   ├── schemeGui.py

│   │   ├── schemeGui.ui

│   │   └── widgets

│   ├── __init__.py

│   ├── misc

│   │   ├── eerSampling.py

│   │   ├── __init__.py

│   │   ├── libimVol.py

│   │   ├── libmask.py

│   │   ├── libpdb.py

│   │   ├── neighbourMap.py

│   │   ├── predictPointCloud.py

│   │   └── system.py

│   ├── pipe

│   │   ├── __init__.py

│   │   └── libpipe.py

│   ├── README.md

│   ├── rw

│   │   ├── __init__.py

│   │   ├── librw.py

│   │   └── particleList.py

│   ├── segment

│   │   └── segmentTomoSlap.py

│   ├── templateMatching

│   │   ├── gapStopTm.py

│   │   ├── libTemplateMatching.py

│   │   ├── pytomExtractCandidates.py

│   │   ├── pytomTm.py

│   │   └── warpTm.py

│   └── warp

│       ├── fsMotionAndCtf.py

│       ├── __init__.py

│       ├── libWarp.py

│       ├── tsAlignment.py

│       ├── tsCtf.py

│       ├── tsExportParticles.py

│       └── tsReconstruct.py

└── tests

    ├── accept

    │   └── test_workflow.py

    ├── __init__.py

    └── unit

        ├── test_cbconfig.py

        ├── test_filterTitls.py

        ├── test_particleList.py

        ├── test_pdb.py

        ├── test_rw.py

        ├── test_schemeMeta.py

        ├── test_templateMatching.py

        ├── test_tiltSeriesMeta.py

        ├── test_tmCandidateExtraction.py

        └── test_warpWrapper.py


34 directories, 98 files

```


libpipe.py:
```

from src.rw.librw import cbconfig,importFolderBySymlink,schemeMeta,dataImport,starFileMeta   

from src.misc.system import run_command,run_command_async

import shutil

import os,re 

import subprocess


class pipe:

  """_summary_


  Raises:if (type(args.scheme) == str and os.path.exists(file_path)):

      self.defaultSchemePath=args.scheme

      Exception: _description_


  Returns:

      _type_: _description_

  """

  def __init__(self,args,invMdocTiltAngle=False):

    CRYOBOOST_HOME=os.getenv("CRYOBOOST_HOME")

    if (type(args.scheme) == str and os.path.exists(args.scheme)==False):

      self.defaultSchemePath=CRYOBOOST_HOME + "/config/Schemes/" + args.scheme

    if (type(args.scheme) == str and os.path.exists(args.scheme)):

      self.defaultSchemePath=args.scheme

    if type(args.scheme)==schemeMeta:  

      self.scheme=args.scheme

    else:

      self.scheme=schemeMeta(self.defaultSchemePath)

    

    self.confPath=CRYOBOOST_HOME + "/config/conf.yaml"

    self.conf=cbconfig(self.confPath)     

    self.args=args

    self.pathMdoc=args.mdocs

    self.pathFrames=args.movies

    self.importPrefix=args.impPrefix

    self.pathProject=args.proj

    self.invMdocTiltAngle=invMdocTiltAngle

    print("init class pipe with invert Mdoc tiltangles set to: " + str(self.invMdocTiltAngle))

    headNode=self.conf.confdata['submission'][0]['HeadNode']

    sshStr=self.conf.confdata['submission'][0]['SshCommand']

    envRel=self.conf.confdata['submission'][0]['Environment']

    schemeName=self.scheme.scheme_star.dict['scheme_general']['rlnSchemeName']

    schemeName=os.path.basename(schemeName.strip(os.path.sep)) #remove path from schemeName

    schemeLockFile=".relion_lock_scheme_" + schemeName + os.path.sep  + "lock_scheme"

    relSchemeStart="export TERM=xterm;(relion_schemer --scheme " + schemeName  + ' --run --verb 2 & pid=\$!; echo \$pid  > Schemes/'+ schemeName + '/scheme.pid)' # working

    relScheduleJob="relion_pipeliner --addJobFromStar XXXJobStarXXX --setJobAlias XXXAliasXXX --addJobOptions XXXJobOptionsXXX"

    

    #relSchemeStart="export TERM=xterm;{relion_schemer --scheme " + schemeName  + ' --run --verb 2 & pid=\$!; echo \$pid  > Schemes/'+ schemeName + '/scheme.pid}' 

    #relSchemeStart="export TERM=xterm;relion_schemer --scheme " + schemeName  + ' --run --verb 2 & pid=\$!; echo \$pid  > Schemes/'+ schemeName + '/scheme.pid'

    #relSchemeStart = "export TERM=xterm; { relion_schemer --scheme " + schemeName + " --run --verb 2 & pid=$!; echo $pid > Schemes/" + schemeName + "/scheme.pid; }"

    #waitFor=';sleep 2; while ps -p \`cat Schemes/' + schemeName + '/scheme.pid\` > /dev/null 2>&1;do sleep 4;done;sleep 2'

    waitFor=";sleep 259200" #keep the ssh for 3 days in case of manual sorting

   

    self.schemeLockFile=schemeLockFile

    self.schemeName=schemeName

    

    chFold="cd " + os.path.abspath(self.pathProject) + ";"

    #relSchemeAbrot="relion_schemer --scheme " + schemeName  + " --abort; + pkill -f """ + 

    #relSchemeAbrot="pkill -f \'relion_schemer --scheme " + schemeName + "\'" 

    relSchemeAbrot="kill XXXPIDXXX"

    relStopLastJob="scancel XXXJOBIDXXX"

    relSchemeReset="relion_schemer --scheme " + schemeName  + " --reset"

    relSchemeUnlock="rm " + schemeLockFile + ";rmdir "+ os.path.dirname(schemeLockFile)


    relGuiStart="relion --tomo --do_projdir "


    relGuiUpdate="relion_pipeliner --RunJobs "

    envStr=envRel + ";"

    logStr=" > " + schemeName + ".log 2>&1 " 

    logStrAdd=" >> " + schemeName + ".log 2>&1 "

    self.commandSchemeStart=sshStr + " " + headNode + ' "'  + envStr + chFold + relSchemeStart + logStrAdd + waitFor + '"'

    self.commandSchemeAbrot=sshStr + " " + headNode + ' "'  + envStr  + relSchemeAbrot + ";" + logStrAdd + '"'

    self.commandSchemeJobAbrot=sshStr + " " + headNode + ' "' + relStopLastJob + ";" + logStrAdd + '"'

    self.commandSchemeReset=sshStr + " " + headNode + ' "'  + envStr + chFold + relSchemeReset + logStrAdd + '"'

    self.commandGui=sshStr + " " + headNode + ' "'  + envStr + chFold + relGuiStart  + '"'

    self.commandSchemeUnlock=sshStr + " " + headNode + ' "'  + envStr + chFold + relSchemeUnlock + logStrAdd + '"'

    self.commandGuiUpdate=sshStr + " " + headNode + ' "'  + envStr + chFold + relGuiUpdate + '"' 

    

    self.commandScheduleJob=sshStr + " " + headNode + ' "'  + envStr + chFold + relScheduleJob + '"'

    

      

  def initProject(self):

    #importFolderBySymlink(self.pathFrames, self.pathProject)

    #if (self.pathFrames!=self.pathMdoc):

    #    importFolderBySymlink(self.pathMdoc, self.pathProject)

    os.makedirs(self.pathProject,exist_ok=True)

    os.makedirs(self.pathProject + "/" + "Logs",exist_ok=True)

    self.generatCrJobLog("initProject","generting: " + self.pathProject + "\n")

    self.generatCrJobLog("initProject","generting: " + self.pathProject + "/Logs" + "\n")

    self.generatCrJobLog("initProject","copying: " + os.getenv("CRYOBOOST_HOME") + "/config/qsub" + "\n")

    self.writeToLog(" + Project: " + self.pathProject + " --> Logs/initProject" "\n")

    shutil.copytree(os.getenv("CRYOBOOST_HOME") + "/config/qsub", self.pathProject + os.path.sep + "qsub",dirs_exist_ok=True)

    

  def importData(self):#,wkFrames,wkMdoc): 

    self.writeToLog(" + ImportData: --> Logs/importData" "\n")

    self.writeToLog("    " + self.pathFrames + " --> frames" +"\n")

    self.writeToLog("    " + self.pathMdoc + " --> mdoc" +"\n")

    

    logDir=self.pathProject + os.path.sep + "Logs" + os.path.sep + "importData"

    dataImport(self.pathProject,self.pathFrames,self.pathMdoc,self.importPrefix,logDir=logDir,invTiltAngle=self.invMdocTiltAngle)

    print("frames/"+os.path.basename(self.pathFrames))

    self.scheme.update_job_star_dict('importmovies','movie_files',"frames/"+os.path.basename(self.pathFrames))

    self.scheme.update_job_star_dict('importmovies','mdoc_files',"mdoc/"+os.path.basename(self.pathMdoc))

    #import movies and mdoc()  

      

  def writeScheme(self):

     

     self.generatCrJobLog("initProject","writing Scheme to: " + self.pathProject + "/Schemes" + "\n")

     path_scheme = os.path.join(self.pathProject, self.scheme.scheme_star.dict['scheme_general']['rlnSchemeName'])

     nodes = {i: job for i, job in enumerate(self.scheme.jobs_in_scheme)}

     #self.scheme.filterSchemeByNodes(nodes) #to correct for input output mismatch within the scheme

     self.scheme.write_scheme(path_scheme)

     self.scheme.schemeFilePath=path_scheme + "/scheme.star"

  

  def initRelionProject(self):

    command=self.commandGui #.replace("--do_projdir","--do_projdir --idle 0")

    run_command_async(command)

    

  def runScheme(self):

    

    self.generatCrJobLog("manageWorkflow","starting workflow:" + "\n")

    self.generatCrJobLog("manageWorkflow","  " + self.commandSchemeStart + "\n")

    p=run_command_async(self.commandSchemeStart)

    #p=run_command(self.commandSchemeStart)

  

  def scheduleJobs(self): 

    #TODO check for alias check for scheduled jobs

    #TODO check running schedule

    

    self.initRelionProject()

    self.writeToLog(" + Schedule Jobs: --> Logs/scheduleJobs" "\n")

    path_scheme = self.scheme.schemeFolderPath

    path_schemeRel = "Schemes/" + path_scheme.split("/Schemes/")[1]

    defPipePath=self.pathProject+os.path.sep+"default_pipeline.star"

    count=1

    fullOutputName=[]

    self.generatCrJobLog("scheduleJobs","scheduling " + str(len(self.scheme.jobs_in_scheme)) + " jobs:" + "\n")

    

    for job in self.scheme.jobs_in_scheme:


        jobpath=os.path.join(path_schemeRel, job, "job.star")

        command=self.commandScheduleJob.replace("XXXJobStarXXX",jobpath)

        if job != "importmovies" and count==1:

            #TODO: Ask user for input check if  jobs already exist

            print("Ask user for job input")

        if job=="importmovies" and count==1: #first job for every pipeline

            command=command.replace("--addJobOptions XXXJobOptionsXXX",'')

        if count > 1: #output of previous job is input for next job

            inputParamName,inputParamValue,inputParamJobType=self.scheme.getMajorInputParamNameFromJob(job)

            if "denoisepredict" in job:# needs additional job options

                updateField="'" + inputParamName + " == " + fullOutputName[-2] 

                fpModel=os.path.dirname(fullOutputName[-1]) + os.path.sep + "denoising_model.tar.gz"

                updateField += ";care_denoising_model == " + fpModel + "'"

            else:

                lastJob=self.getLastJobOfType(inputParamJobType,fullOutputName)

                updateField="'" + inputParamName + " == " + lastJob + "'"

               

        

            command=command.replace("XXXJobOptionsXXX",updateField)

             

        alias=job+str(count)

        command=command.replace("XXXAliasXXX",alias)

        p=run_command(command)

        if p[0] is None:

            print("Error scheduling job: " + job ) 

            print("with command: " + command)

            print("Error message: " + str(p[1]))

            print("Exit code: " + str(p[2]))

            print("Stopping scheduling jobs")

            break

        

        st=starFileMeta(defPipePath)

        df=st.dict["pipeline_processes"]

        lf=df[df['rlnPipeLineProcessAlias'].str.contains(alias, na=False)]

        if lf.empty:

            self.generatCrJobLog("scheduleJobs","Error: no job found with alias: " + alias + "\n",type="err")

            print("Error: no job found with alias: " + alias)

            break

        else:

            self.generatCrJobLog("scheduleJobs"," -->job: " + job + " alias: " + alias + "\n")

            print("Scheduled job: " + job + " with alias: " + alias)  

          

        outpuFold=lf['rlnPipeLineProcessAlias'].values[0]

        outputName=os.path.basename(self.conf.getJobOutput(job.split("_")[0]))

        fullOutputName.append(outpuFold + os.path.sep + outputName)

        

        count+=1

  def getLastJobOfType(self,jobType,listOfJobs):

    """

    Get the last job of a specific type from a list of jobs.


    Args:

        jobType (str): The type of job to search for.

        listOfJobs (list): A list of job names.


    Returns:

        str: The name of the last job of the specified type, or None if not found.

    """

    for job in listOfJobs:

        if jobType in job:

            return job

    return None

        

    

  def runSchemeSync(self):

    p=run_command(self.commandSchemeStart)

   

  def abortScheme(self):

    lastBatchJobId,lastJobFolder=self.parseSchemeLogFile()

    self.writeToLog(" + Abort Workflow: --> Logs/manageWorkflow" + "\n")

    self.writeToLog("   Name: " + self.schemeName + "\n")

    pidFile=self.pathProject + '/Schemes/'+ self.schemeName + '/scheme.pid'

    try:

      with open(pidFile, 'r') as file:

           pid = int(file.read().strip())

    except FileNotFoundError:

      print(f"PID file {pidFile} does not exist.") 


    cSchemeAbroat=self.commandSchemeAbrot.replace("XXXPIDXXX",str(pid))

    self.generatCrJobLog("manageWorkflow","stopping workflow:" + "\n")

    self.generatCrJobLog("manageWorkflow"," + " + cSchemeAbroat + "\n")

    p=run_command(cSchemeAbroat)

    if lastBatchJobId != None:

      self.generatCrJobLog("manageWorkflow","killing job:" + "\n")

      self.generatCrJobLog("manageWorkflow",self.commandSchemeJobAbrot.replace("XXXJOBIDXXX",lastBatchJobId) +"\n")                     

      p=run_command(self.commandSchemeJobAbrot.replace("XXXJOBIDXXX",lastBatchJobId))

    self.setLastRunningJobToFailed()

    p=run_command(self.commandGuiUpdate)

    self.generatCrJobLog("manageWorkflow","unlocking \n")

    self.generatCrJobLog("manageWorkflow"," + " + self.commandSchemeUnlock + "\n")

    self.writeToLog(" + Workflow aborted !\n")

    self.unlockScheme()

    

  def setLastRunningJobToFailed(self):

      defPipePath=self.pathProject+os.path.sep+"default_pipeline.star"

      

      if os.path.isfile(defPipePath):

        try:

          st=starFileMeta(defPipePath)

          df=st.dict["pipeline_processes"]

          hit=df.rlnPipeLineProcessName[df.index[df['rlnPipeLineProcessStatusLabel'] == 'Running']]

          fold=str(hit.values[0])

          if os.path.isfile(fold + os.path.sep + "RELION_JOB_EXIT_SUCCESS")==False:

            df.loc[df['rlnPipeLineProcessStatusLabel'] == 'Running', 'rlnPipeLineProcessStatusLabel'] = 'Failed'

            print("setting to job to failed")

            st.writeStar(defPipePath)

        except:

          print("error resetting pipe!")

          fold=None

        return fold

      else:

        return None

    

  def checkForLock(self):  

    pathLock=self.pathProject + os.path.sep + self.schemeLockFile

    print(pathLock)

    print(os.path.isfile(pathLock))

    return os.path.isfile(pathLock)

  

  def resetScheme(self):

    self.writeToLog(" + Reset Workflow --> Logs/manageWorkflow \n")

    self.generatCrJobLog("manageWorkflow","  " + self.commandSchemeReset + "\n")

    p=run_command(self.commandSchemeReset)

    self.generatCrJobLog("manageWorkflow",self.commandSchemeReset +"\n")

    self.writeToLog(" + Workflow  reset done !\n")

  

  

  def setCurrentNodeScheme(self,NodeName):

    

    path_scheme = os.path.join(self.pathProject, self.scheme.scheme_star.dict['scheme_general']['rlnSchemeName'])

    self.writeToLog(" + Reset Workflow to node: " + NodeName + " --> Logs/manageWorkflow"  + "\n")

    print(self.scheme.schemeFilePath)

    self.scheme.read_scheme()

    self.scheme.scheme_star.dict["scheme_general"]["rlnSchemeCurrentNodeName"]=NodeName

    print(self.scheme.scheme_star.dict["scheme_jobs"])

    self.writeScheme()

    self.writeToLog(" + Workflow reset done !\n")

    

    

  def getCurrentNodeScheme(self):

   #self.scheme=schemeMeta(self.defaultSchemePath)

   self.scheme.read_scheme()

   return self.scheme.scheme_star.dict["scheme_general"]["rlnSchemeCurrentNodeName"]

        

  def unlockScheme(self):

    pathLock=self.pathProject + os.path.sep + os.path.dirname(self.schemeLockFile)

    if os.path.isdir(pathLock):

      self.writeToLog(" + Unlock Workflow: \n")

      self.generatCrJobLog("manageWorkflow" ," + " + self.commandSchemeUnlock + "\n")

      p=run_command(self.commandSchemeUnlock)

        

  def openRelionGui(self):

    print(self.commandGui)

    p=run_command_async(self.commandGui)

   

  def parseSchemeLogFile(self):

    """

    Parses the scheme log file to extract the last batch job ID and job folder.


    Args:

        self (Pipe): An instance of the Pipe class.


    Returns:

        tuple: A tuple containing the last batch job ID and job folder.


    """

    file_path = self.pathProject + os.path.sep + self.schemeName + ".log"

    if (os.path.isfile(file_path) == False):

        return None, None

    with open(file_path, 'r') as file:

        last_batch_job_id = None

        last_job_folder = None

        for line in file:

            re_res_last_batch_job = re.search("Submitted batch job", line)

            if re_res_last_batch_job:

                last_batch_job_id = re_res_last_batch_job.string.split("job")[1].strip()  # Assuming the format is "Submitted jobid"

            

            #re_res_last_job_folder = re.search("Creating new Job", line) #buggy should be "Executing Job:" but it doesn't work gets only upadted when job is finished

            re_res_last_job_folder = re.search("Executing Job:", line)

            if re_res_last_job_folder:

                last_job_folder = re_res_last_job_folder.string.split("Job:")[1].split(" ")[1].strip() 

            

            re_res_last_job_folder = re.search(' --> Logs/', line)

            if re_res_last_job_folder:

                last_job_folder = re_res_last_job_folder.string.split("-->")[1].split(" ")[1].strip()

            

            

            

    return last_batch_job_id,last_job_folder    

               

  def getLastJobLogs(self):

      lastBatchJobId,lastJobFolderScheme=self.parseSchemeLogFile()

      lastJobFolderPipe=self.getRunningJob()

      if (lastJobFolderPipe is  None):

        lastJobFolder=lastJobFolderScheme

      else:

        lastJobFolder=lastJobFolderPipe

      

      if (lastJobFolder is not None):

        jobOut=self.pathProject+os.path.sep+lastJobFolder+os.path.sep+"run.out"

        jobErr=self.pathProject+os.path.sep+lastJobFolder+os.path.sep+"run.err"

      else:

        print("no Logs found")

        jobOut="No logs found"

        jobErr="No logs found"  

      #TODO: check for ext Log

      return jobOut,jobErr               

  

  def getRunningJob(self):

      defPipePath=self.pathProject+os.path.sep+"default_pipeline.star"

      

      if os.path.isfile(defPipePath):

        try:

            st=starFileMeta(defPipePath)

            df=st.dict["pipeline_processes"]

            hit=df.rlnPipeLineProcessName[df.index[df['rlnPipeLineProcessStatusLabel'] == 'Running']]

            if hit.size > 0:

              fold=str(hit.values[0])

            else:

              hit=df.loc[df['rlnPipeLineProcessStatusLabel'] == 'Failed', 'rlnPipeLineProcessName'].iloc[-1]

              fold=hit

        except:

              fold=None

        return fold

      else:

        return None

        

      

  

  def writeToLog(self,text):

      logFile=self.pathProject+os.path.sep+self.schemeName+".log"

      with open(logFile, "a") as myfile:

          myfile.write(text)

  

  def generatCrJobLog(self,jobName,text,type="out"):

      

      os.makedirs(self.pathProject + "/Logs/" + jobName,exist_ok=True)

      if type=="out":

        logFile=self.pathProject+ "/Logs/" + jobName+ "/run.out"

        with open(logFile, "a") as myfile:

          myfile.write(text)

      if type=="err":  

        logFile=self.pathProject+ "/Logs/" + jobName+ "/run.err"

        with open(logFile, "a") as myfile:

          myfile.write(text)

      

      logFile=self.pathProject+ "/Logs/" + jobName+ "/run.err"

      if not os.path.isfile(logFile):

        with open(logFile, "a") as myfile:

          pass

                    

```


librw.py:

```
# %%
import yaml

import os,pathlib

import starfile

import subprocess

import glob

import tempfile

import pandas as pd

import xml.etree.ElementTree as ET

import mrcfile

from collections import namedtuple


class warpMetaData:

  

  def __init__(self,dataPath):

    

    self.data_df=pd.DataFrame()

    self.parseXMLdata(dataPath)

  

  def parseXMLdata(self,wk):

    for name in glob.glob(wk):

      if self.checkXMLFileType(name)== 'fs':

        df=self.__parseXMLFileFrameSeries(name) 

      else:

        df=self.__parseXMLFileTiltSeries(name)

      self.data_df =pd.concat([self.data_df, df], ignore_index=True)

       

  def checkXMLFileType(self,pathXML):

    tree = ET.parse(pathXML)

    root = tree.getroot()

    grid_ctf = root.find('MoviePath')

    if grid_ctf is None:

      xmlType='fs'

    else:

      xmlType='ts'

    #print(xmlType)

    return xmlType  

          

  def __parseXMLFileFrameSeries(self,pathXML):

    data_df=pd.DataFrame()

    tree = ET.parse(pathXML)

    root = tree.getroot()

    ctf = root.find(".//CTF")

    data={}

    data = {

       "cryoBoostKey":pathlib.Path(pathXML).name.replace(".xml",""),

       "name": pathXML,

       "folder": str(pathlib.Path(pathXML).parent.as_posix()),

       "defocus_value": ctf.find(".//Param[@Name='Defocus']").get('Value'),

       "defocus_angle": ctf.find(".//Param[@Name='DefocusAngle']").get('Value'),

       "defocus_delta": ctf.find(".//Param[@Name='DefocusDelta']").get('Value'),

         }

    data_df = pd.DataFrame([data])

    return data_df


  def __parseXMLFileTiltSeries(self, pathXML):

    tree = ET.parse(pathXML)

    root = tree.getroot()

    

    # Parse GridCTF (Defocus)

    grid_ctf = root.find('GridCTF')

    defocus_values = []

    z_values = []

    for node in grid_ctf.findall('Node'):

        value = float(node.get('Value'))

        z = int(node.get('Z'))

        defocus_values.append(value)

        z_values.append(z)

        

    # Parse GridCTFDefocusDelta

    grid_delta = root.find('GridCTFDefocusDelta')

    delta_values = []

    for node in grid_delta.findall('Node'):

        value = float(node.get('Value'))

        delta_values.append(value)

        

    # Parse GridCTFDefocusAngle

    grid_angle = root.find('GridCTFDefocusAngle')

    angle_values = []

    for node in grid_angle.findall('Node'):

        value = float(node.get('Value'))

        angle_values.append(value)

        

    # Parse MoviePath

    movie_paths = []

    for path in root.find('MoviePath').text.split('\n'):

        if path.strip():  # Skip empty lines

            # Get basename and remove .eer extension

            movie_name = os.path.basename(path).replace('_EER.eer', '')

            movie_name = movie_name.replace(".tif","")

            movie_name = movie_name.replace(".eer","")

            movie_paths.append(movie_name)

            

    # Create DataFramemdoc.all_df.mdocFileName[0] in mdoc.all_df.SubFramePath 

    df = pd.DataFrame({

        'Z': z_values,

        'defocus_value': defocus_values,

        'defocus_delta': delta_values,

        'defocus_angle': angle_values,

        'cryoBoostKey': movie_paths

    })

    

    return df


      

#from lib.functions import calculate_dose_rate_per_pixel, extract_eer_from_header

class cbconfig:

  def __init__(self,configPath=None):

    if configPath is None:

        self.CRYOBOOST_HOME=os.getenv("CRYOBOOST_HOME")

        configPath=self.CRYOBOOST_HOME + "/config/conf.yaml"

    self.configPath = configPath

    self.read_config()

    self.get_microscopePreSetNames()


  def read_config(self):

    """

    reads a configuration file in yaml format.

    

    Args:

      filename (str): name of the .yaml file.

    

    Returns: 

      dict: dictioanry with paramName and data.

    """

    with open(self.configPath) as f:

      self.confdata = yaml.load(f, Loader=yaml.FullLoader)

  def getEnvSting(self,typeE):

   

    if typeE=="local":

        envString=self.confdata["local"]['Environment']

    if typeE=="submission":

        self.conf.confdata['submission'][0]['Environment']

    

    return envString

  

  def getJobComputingParams(self,comReq,doNodeSharing):    

       

        self.confdata["computing"]["JOBTypesCompute"]

        confComp=self.confdata["computing"]

        jobType=None

        for entry in confComp["JOBTypesCompute"]:

          for job in confComp["JOBTypesCompute"][entry]:

              if job == comReq[0]:

                jobType=entry

                break

        

        if (jobType == None):

          compParams=None

          return compParams 

        

        partionSetup= self.confdata["computing"][comReq[2]]

        kMPIperNode=self.get_alias_reverse(comReq[0],"MPIperNode")

        kNrGPU=self.get_alias_reverse(comReq[0],"NrGPU")

        kNrNodes=self.get_alias_reverse(comReq[0],"NrNodes")

        kPartName=self.get_alias_reverse(comReq[0],"PartionName")

        kMemory=self.get_alias_reverse(comReq[0],"MemoryRAM")

        compParams={}

        compParams[kPartName]=comReq[2]

        compParams[kMemory]=partionSetup["RAM"]

        NodeSharing= self.confdata["computing"]["NODE-Sharing"]

        if (doNodeSharing and (comReq[2] in NodeSharing["ApplyTo"])):

          compParams[kMemory]=str(round(int(partionSetup["RAM"][:-1])/2))+"G"

        gpuIDString=":".join(str(i) for i in range(0,partionSetup["NrGPU"]))

        maxNodes= self.confdata["computing"]["JOBMaxNodes"]

        

        

        if (comReq[0] in maxNodes.keys()):  

            if (comReq[1]>maxNodes[comReq[0]][0]):

                comReq[1]=maxNodes[comReq[0]][0]

        

        if (jobType == "CPU-MPI"):

          compParams[kMPIperNode]=partionSetup["NrCPU"]

          compParams["nr_mpi"]=partionSetup["NrCPU"]*comReq[1]  

          compParams[kNrGPU]=0

          compParams[kNrNodes]=comReq[1] 

          compParams["nr_threads"]=1

          if (doNodeSharing and comReq[2] in NodeSharing["ApplyTo"]):

              compParams[kMPIperNode]=partionSetup["NrCPU"]-(partionSetup["NrGPU"]*NodeSharing["CPU-PerGPU"]) 

              compParams["nr_mpi"]=compParams[kMPIperNode]*comReq[1]

              

        if (jobType == "CPU-2MPIThreads"):

          compParams[kMPIperNode]=2

          compParams["nr_mpi"]=compParams[kMPIperNode]*comReq[1]  

          compParams[kNrGPU]=0

          compParams[kNrNodes]=comReq[1] 

          compParams["nr_threads"]=round(partionSetup["NrCPU"]/2)

          if (doNodeSharing and comReq[2] in NodeSharing["ApplyTo"]):

              compParams["nr_threads"]=compParams["nr_threads"]-round(partionSetup["NrGPU"]*NodeSharing["CPU-PerGPU"]/2)

        

        if (jobType == "GPU-OneProcess") or (jobType == "GPU-OneProcessOneGPU"):

          compParams[kMPIperNode]=1

          compParams["nr_mpi"]=1  

          compParams[kNrGPU]=partionSetup["NrGPU"]

          compParams[kNrNodes]=1

          compParams["nr_threads"]=partionSetup["NrGPU"]

          compParams["gpu_ids"]=gpuIDString

        

        if (jobType == "GPU-OneProcessOneGPU"):

          compParams["gpu_ids"]=0

          compParams[kNrGPU]=1

        

        if (jobType == "GPU-ThreadsOneNode"):

          compParams[kMPIperNode]=1

          compParams["nr_mpi"]=1  

          compParams[kNrGPU]=partionSetup["NrGPU"]

          compParams[kNrNodes]=1 

          compParams["nr_threads"]=round(partionSetup["NrCPU"]/1)

          if (doNodeSharing and comReq[2] in NodeSharing["ApplyTo"]):

              compParams["nr_threads"]=compParams["nr_threads"]-round(partionSetup["NrGPU"]*NodeSharing["CPU-PerGPU"])

        

        if (jobType == "GPU-MultProcess"):

          compParams[kMPIperNode]=partionSetup["NrGPU"]

          compParams[kNrGPU]=partionSetup["NrGPU"]

          compParams["nr_mpi"]=partionSetup["NrGPU"]*comReq[1]  

          compParams["nr_threads"]=1

          compParams["gpu_ids"]= ":".join([gpuIDString] * comReq[1]) 

          compParams[kNrNodes]=comReq[1]

              

        if comReq[0] in confComp['JOBsPerDevice'].keys():

            compParams["param10_value"]=confComp['JOBsPerDevice'][comReq[0]][comReq[2]]

          

        return compParams 

        

  def get_alias(self,job, parameter):

    """

    some inputs used by Relion are not self-explanatory (eg. qsub_extra2) so a yaml list was created to change the 

    respective name that is displayed while still keeping the original name for writing the data.


    Args:

      job (str): job name the parameter is used for.

      parameter (str): parameter name.


    Returns:

      alias (str): alias displayed instead of the parameter name.


    Example:

      job

    """

    

    for entry in self.confdata["aliases"]:

      # if the entry Job of one of the lists equals the given job or all and the entry Parameter contains the given 

      # parameter name, return the entry Alias

      if (entry["Job"] == job or entry["Job"] == "all") and entry["Parameter"] == parameter:

        return entry["Alias"]

    return None

  

  def getJobOutput(self, jobName):

    return self.confdata["star_file"][jobName]

  

  def get_microscopePreSet(self,microscope):

      mic_data= self.confdata['microscopes']

      for entry in mic_data:

          if entry == microscope:

              microscope_parameters_list_of_dicts= mic_data[entry]

      microscope_parameters = {}

      for dicts in microscope_parameters_list_of_dicts:

          microscope_parameters.update(dicts)

      

      return microscope_parameters    

  

  def get_microscopePreSetNames(self):

      mic_data = self.confdata['microscopes']

      microscope_presets = {}  # Initialize an empty dictionary

      for i, entry in enumerate(mic_data):  # Use enumerate to get both index and entry

        microscope_presets[i] = entry

      self.microscope_presets=microscope_presets

      

      return microscope_presets

      

  

  def get_alias_reverse(self,job, alias,):

    """

    reverse of the get alias function, i.e. returns the parameter name as used in the .star file when entering the 

    alias. Kept seperate to keep reading and writing clearly separated to avoid errors. 


    Args:

      job (str): job name the parameter is used for.

      alias (str): alias displayed instead of the parameter name.


    Returns:

      parameter (str): parameter name as displayed in the job.star file.


    Example:mdoc.all_df.mdocFileName[0] in mdoc.all_df.SubFramePath 

      job

    """

    

    # go through entries in the aliases dict

    for entry in self.confdata["aliases"]:

      # if the entry Job of one of the lists equals the given job or all and the entry Alias contains the given 

      # parameter name, return the entry Parameter

      if (entry["Job"] == job or entry["Job"] == "all") and entry["Alias"] == alias:

        return entry["Parameter"]

    return None


def importFolderBySymlink(sourceFold, targetFold):

    """

    creates a symlink from sourceFold to targetFold


    Args:

        path_frames (str): absolute path to the imported frames.

        targetFold (str): path where the symlink will be created


    Returns:

        -


    Example:

        path_frames = /fs/pool/pool-plitzko3/Michael/01-Data/relion/frames

        path_out_dir = /fs/pool/pool-plitzko3/Michael/01-Data/project


        importFolderBySymlink(path_frames, path_out_dir)

        

    """

    import warnings

    

    if not os.path.exists(targetFold):

        os.makedirs(targetFold)      

    

    command_frames = f"ln -s {os.path.abspath(sourceFold)} {targetFold}" + os.path.sep

    

    foldFrames=targetFold + os.path.sep + os.path.basename(sourceFold.rstrip("/")) 

    if os.path.exists(foldFrames):

        warnings.warn("Path to folder already exists." + f" {foldFrames} ")

        #os.unlink(foldFrames)

    else:

        os.system(command_frames)

    

   

    




def read_header(path_to_frames):

  """

  reads header of a file to fetch the nr of eer's to calculate the optimal split using the calculate_dose_rate_per_pixel function.


  Args:

    path_to_frames (str): path to the respective frames.


  Returns:

    eer_split (dict): best possible eer split per frame as value so it can be added to the dict when transferring

                      information from the file path to the input data.

  Example:


  """

  # so it only fetches the first instance of *.eer, not all of them

  eer_file = glob.glob(f"{path_to_frames}/*.eer")[0]

  command = f"header {eer_file}"

  result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)

  header = str(result)

  #eers = extract_eer_from_header(header)

  #print(eers)

  #eer_split = calculate_dose_rate_per_pixel(eers)

  #header = frame.read()

  #print(result.stdout)

  

class mdocMeta:

  def __init__(self,mdocWk=None):

      

    if (mdocWk is not None):

      self.mdocWk = mdocWk

      self.readAllMdoc(mdocWk)


  

  def filterByTiltSeriesStarFile(self,tiltSeriesFileName):

    

    ts=tiltSeriesMeta(tiltSeriesFileName)

    dfMdoc=self.all_df['cryoBoostKey']

    dfTs=ts.all_tilts_df['rlnMicrographMovieName'].apply(os.path.basename)

    

    mask=dfMdoc.isin(dfTs)

    self.all_df=self.all_df[mask]


  def readAllMdoc(self,wkMdoc):

    mdoc_files = glob.glob(wkMdoc)

    self.all_df=pd.DataFrame()

    for mdoc in mdoc_files:

      header,data,orgPath = self.readMdoc(mdoc)

      df = pd.DataFrame(data)

      df['mdocHeader']=header

      df['mdocFileName']=os.path.basename(mdoc)

      if orgPath is not None:

        df['mdocOrgPath']=orgPath

      else:

        df['mdocOrgPath']=mdoc

      

      self.all_df = pd.concat([self.all_df, df], ignore_index=True) 

      if 'SubFramePath' in self.all_df:

        k = self.all_df['SubFramePath'].apply(lambda x: os.path.basename(x.replace("\\","/").replace("\\","")))

      else:

        raise Exception("SubFramePath entry missing check your mdoc's: "+ mdoc)

      if (k.is_unique):

        self.all_df['cryoBoostKey']=k

      else:

        raise Exception("SubFramePath is not unique !!") 


    self.param4Processing={}

    if self.all_df.mdocHeader[0].find("SerialEM:")==-1:

      self.acqApp="Tomo5"

      #self.param4Processing["TiltAxisAngle"]=-round(-(-1*float(self.all_df.RotationAngle.unique()[0]))+180,1)

      self.param4Processing["TiltAxisAngle"]=abs(round(float(self.all_df.RotationAngle.unique()[0]),1))

    else: #+180

      self.acqApp="SerialEM"

      self.param4Processing["TiltAxisAngle"]=round(float(self.all_df.mdocHeader[0].split("Tilt axis angle =")[1].split(",")[0]),2) #+180

    self.param4Processing["DosePerTilt"]=round(float(self.all_df.ExposureDose[0])*1.5,2)

    self.param4Processing["PixelSize"]= round(float(self.all_df.mdocHeader[0].split("PixelSpacing = ")[1].split('\n')[0]),2)

    self.param4Processing["ImageSize"]=self.all_df.mdocHeader[0].split("ImageSize = ")[1].split('\n')[0].replace(" ","x")

    self.param4Processing["NumMdoc"]=len(mdoc_files)

  def addPrefixToFileName(self,prefix):


    self.all_df['SubFramePath']=self.all_df['SubFramePath'].apply(lambda x: prefix+os.path.basename(x))

    self.all_df['mdocFileName']=self.all_df['mdocFileName'].apply(lambda x: prefix+os.path.basename(x))

         

          

  def writeAllMdoc(self,folder,appendMdocRootPath=False):   

    

    for mdoc in self.all_df['mdocFileName'].unique():

      df = self.all_df[self.all_df['mdocFileName']==mdoc]

      header=df['mdocHeader'].unique()[0]

      mdocPath=os.path.join(folder,mdoc)       

      self.writeMdoc(mdocPath,header,df,appendMdocRootPath)

      del df,header

    

    

  def readMdoc(self,file_path):

      """

      Parses an .mdoc file into a header and a pandas DataFrame containing ZValue sections.


      Args:

          file_path (str): Path to the .mdoc file.


      Returns:

          tuple: A tuple containing the header string and a DataFrame with ZValue section data.

      """

      header = []

      data = []

      current_row = {}

      in_zvalue_section = False

      orgPath=None

      

      with open(file_path, 'r') as file:

          lines = file.readlines()


      for line in lines:

          line = line.strip()

          

          # Check if we're entering ZValue section

          if line.startswith('[ZValue'):

              in_zvalue_section = True

              if current_row:

                  data.append(current_row)

                  current_row = {}

              current_row['ZValue'] = line.split('=')[1].strip().strip(']')

              continue

          

          # Check if we're leaving ZValue section

          if in_zvalue_section and line.startswith('[') and not line.startswith('[ZValue'):

              in_zvalue_section = False

              data.append(current_row)

              current_row = {}

          

          if in_zvalue_section:

              # Process each line within ZValue section

              if line.startswith('TiltAngle'):

                  current_row['TiltAngle'] = line.split('=')[1].strip()

              elif line.startswith('StagePosition'):

                  current_row['StagePosition'] = line.split('=')[1].strip()

              elif line.startswith('StageZ'):

                  current_row['StageZ'] = line.split('=')[1].strip()

              elif line.startswith('Magnification'):

                  current_row['Magnification'] = line.split('=')[1].strip()

              elif line.startswith('Intensity'):

                  current_row['Intensity'] = line.split('=')[1].strip()

              elif line.startswith('ExposureDose'):

                  current_row['ExposureDose'] = line.split('=')[1].strip()

              elif line.startswith('PixelSpacing'):

                  current_row['PixelSpacing'] = line.split('=')[1].strip()

              elif line.startswith('SpotSize'):

                  current_row['SpotSize'] = line.split('=')[1].strip()

              elif line.startswith('Defocus'):

                  current_row['Defocus'] = line.split('=')[1].strip()

              elif line.startswith('ImageShift'):

                  current_row['ImageShift'] = line.split('=')[1].strip()

              elif line.startswith('RotationAngle'):

                  current_row['RotationAngle'] = line.split('=')[1].strip()

              elif line.startswith('ExposureTime'):

                  current_row['ExposureTime'] = line.split('=')[1].strip()

              elif line.startswith('Binning'):

                  current_row['Binning'] = line.split('=')[1].strip()

              elif line.startswith('MagIndex'):

                  current_row['MagIndex'] = line.split('=')[1].strip()

              elif line.startswith('CountsPerElectron'):

                  current_row['CountsPerElectron'] = line.split('=')[1].strip()

              elif line.startswith('MinMaxMean'):

                  current_row['MinMaxMean'] = line.split('=')[1].strip()

              elif line.startswith('TargetDefocus'):

                  current_row['TargetDefocus'] = line.split('=')[1].strip()

              elif line.startswith('PriorRecordDose'):

                  current_row['PriorRecordDose'] = line.split('=')[1].strip()

              elif line.startswith('SubFramePath'):

                  current_row['SubFramePath'] = line.split('=')[1].strip()

              elif line.startswith('NumSubFrames'):

                  current_row['NumSubFrames'] = line.split('=')[1].strip()

              elif line.startswith('FrameDosesAndNumber'):

                  current_row['FrameDosesAndNumber'] = line.split('=')[1].strip()

              elif line.startswith('DateTime'):

                  current_row['DateTime'] = line.split('=')[1].strip()

              elif line.startswith('FilterSlitAndLoss'):

                  current_row['FilterSlitAndLoss'] = line.split('=')[1].strip()

              elif line.startswith('ChannelName'):

                  current_row['ChannelName'] = line.split('=')[1].strip()

              elif line.startswith('CameraLength'):

                  current_row['CameraLength'] = line.split('=')[1].strip()

          else:

              # Collect header information

              header.append(line)

          if line.startswith('CryoBoost_RootMdocPath'):

              orgPath=line.split('=')[1].strip()

              

              

      # Append the last row if it exists

      if current_row:

          data.append(current_row)


      # Convert header list to a single string

      header_str = "\n".join(header)


      # Create DataFrame for ZValue sections

      df = pd.DataFrame(data)

      

      

      return header_str,df,orgPath


  def writeMdoc(self,output_path, header_str, df,appendMdocRootPath=False):

      """

      Writes the modified .mdoc data to a file.


      Args:

          file_path (str): Path to the output .mdoc file.

          header_str (str): Header string of the .mdoc file.

          df (DataFrame): DataFrame containing the ZValue sections.

      """

     

      #df=df.drop(columns=["mdocHeader","mdocFilePath","cryoBoostKey"])   

      # Define the format for each line in the ZValue sections

      # def format_row(row):

      #     return (

      #         f'[ZValue = {row.get("ZValue", "")}]\n'

      #         f'TiltAngle = {row.get("TiltAngle", "")}\n'

      #         f'StagePosition = {row.get("StagePosition", "")}\n'

      #         f'StageZ = {row.get("StageZ", "")}\n'

      #         f'Magnification = {row.get("Magnification", "")}\n'

      #         f'Intensity = {row.get("Intensity", "")}\n'

      #         f'ExposureDose = {row.get("ExposureDose", "")}\n'

      #         f'PixelSpacing = {row.get("PixelSpacing", "")}\n'

      #         f'SpotSize = {row.get("SpotSize", "")}\n'

      #         f'Defocus = {row.get("Defocus", "")}\n'

      #         f'ImageShift = {row.get("ImageShift", "")}\n'

      #         f'RotationAngle = {row.get("RotationAngle", "")}\n'

      #         f'ExposureTime = {row.get("ExposureTime", "")}\n'

      #         f'Binning = {row.get("Binning", "")}\n'

      #         f'MagIndex = {row.get("MagIndex", "")}\n'

      #         f'CountsPerElectron = {row.get("CountsPerElectron", "")}\n'

      #         f'MinMaxMean = {row.get("MinMaxMean", "")}\n'

      #         f'TargetDefocus = {row.get("TargetDefocus", "")}\n'

      #         f'PriorRecordDose = {row.get("PriorRecordDose", "")}\n'

      #         f'SubFramePath = {row.get("SubFramePath", "")}\n'

      #         f'NumSubFrames = {row.get("NumSubFrames", "")}\n'

      #         f'FrameDosesAndNumber = {row.get("FrameDosesAndNumber", "")}\n'

      #         f'DateTime = {row.get("DateTime", "")}\n'

      #         f'FilterSlitAndLoss = {row.get("FilterSlitAndLoss", "")}\n'

      #         f'ChannelName = {row.get("ChannelName", "")}\n'

      #         f'CameraLength = {row.get("CameraLength", "")}\n'

      #     )

      def format_row(row):

          output = [f'[ZValue = {row.get("ZValue", "")}]']  # ZValue is required

          

          # List of possible fields

          fields = [

              "TiltAngle", "StagePosition", "StageZ", "Magnification", 

              "Intensity", "ExposureDose", "PixelSpacing", "SpotSize",

              "Defocus", "ImageShift", "RotationAngle", "ExposureTime",

              "Binning", "MagIndex", "CountsPerElectron", "MinMaxMean",

              "TargetDefocus", "PriorRecordDose", "SubFramePath", 

              "NumSubFrames", "FrameDosesAndNumber", "DateTime",

              "FilterSlitAndLoss", "ChannelName", "CameraLength"

          ]

          

          # Only add fields that exist in the row and have non-null values

          for field in fields:

              if field in row and pd.notna(row[field]):

                  output.append(f'{field} = {row[field]}')

          

          return '\n'.join(output) + '\n'

      


      # Open the file for writing

      with open(output_path, 'w') as file:

          # Write the header

          file.write(header_str + '\n')

          

          # Write each ZValue section using DataFrame

          df.apply(lambda row: file.write(format_row(row) + '\n'), axis=1)

          

          if appendMdocRootPath:

            file.write("CryoBoost_RootMdocPath = " + os.path.abspath(df["mdocOrgPath"].unique()[0]) + "\n")

          


  # def move_files_to_trash(missing_files, tilts_folder, ext):

  #     """

  #     Move the excluded .mrc and .eer files to a 'Trash' folder.


  #     Args:

  #         missing_files (set): Set of filenames of missing files.

  #         tilts_folder (str): Directory containing the files.

  #         extension (str): extension of the file, either mrc or eer

  #     """

  #     trash_folder = os.path.join(tilts_folder, 'Trash')

  #     os.makedirs(trash_folder, exist_ok=True)


  #     for file in missing_files:

  #         src = os.path.join(tilts_folder, f'{file}.{ext}')

  #         dst = os.path.join(trash_folder, f'{file}.{ext}')

  #         if os.path.exists(src):

  #             shutil.move(src, dst)

  #             print(f'Moved {file} to Trash')

  #         else:

  #             print(f'{file} not found in {tilts_folder}')


    

  

#TODO lagacy will be removed by mdoc object

def read_mdoc(path_to_mdoc_dir, path_to_yaml = "../src/read_write/config_reading_meta.yaml"):

  """

  reads mdoc file and fetches the relevant parameters to automatically set the respective values.


  Args:

    path_to_mdoc (str): path to the directory containing the mdoc files.

  

  Returns:

    return_mdoc_data (dict): values set in the config_reading_meta.yaml file for the respective meta-data type.


  Example:

    path_to_mdoc_dir = [path to mdoc file]


    returns a dict with the respective "x/z dimension", "Pixel size in A", and "Voltage of Microscope"

    (names must be set in the config_reading_meta.yaml file and be the same as in the config_aliases.yaml 

    file). This can subsequently be used to update the respective fields in the table.  

  """

  return_mdoc_data = {}

  # using the dir, access the first mdoc file in the folder

  path_to_mdoc = glob.glob(f"{path_to_mdoc_dir}")[0]

  # get respective mdoc file

  with open(path_to_mdoc, "r") as mdoc_file:

    # store the lines in that mdoc file in a list to iterate over

    mdoc_list = [line.strip() for line in mdoc_file if line.strip()]

    # get entries to look for in the mdoc file (based on the config_reading_meta.yaml file)

    # when only accessing the config_reading_meta.yaml file here, it's only accessed once a valid file-path is 

    # entered, not everytime there is a change to the QLine field

    with open(path_to_yaml, "r") as yaml_file:

      yaml_data = yaml.safe_load(yaml_file)

      print("mdoc file found")

      # access the respective list in the config_reading_meta.yaml file (parameter to look for in mdoc and respective alias)

      yaml_mdoc = yaml_data["meta_data"].get("mdoc", [])

      # access the list of dicts

      for yaml_entry in yaml_mdoc:

        # iterate through the meta data yaml to get the parameters which should be found (and the associated alias as key)

        for yaml_param_name, yaml_alias in yaml_entry.items():

          # iterate through the lines in the mdoc file until the respective entry is found

          for mdoc_current_line in mdoc_list:

            # remove spaces ect that might be in front/after the parameter name

            mdoc_current_line = mdoc_current_line.strip()

            # Skip empty lines

            if not mdoc_current_line:

              continue

            else:

              # data in the mdoc file is in this format: "PixelSpacing = 2.93" --> separate into key and value

              mdoc_key_value = mdoc_current_line.split("=")

              mdoc_current_line_key = mdoc_key_value[0].strip()

              mdoc_current_line_value = mdoc_key_value[1].strip()

              # if the current line holds the information we want as specified in the yaml), add it to the data dict

              # (keys = alias; value = value in meta data)

              if yaml_param_name == mdoc_current_line_key:

                # ImageSize contains both xdim and ydim in the mdoc file, have to split it up

                if mdoc_current_line_key == "ImageSize" and len(mdoc_current_line_value.split()) == 2:

                  xdim, ydim = mdoc_current_line_value.split()

                  # Add entries for both x and y dimensions

                  yaml_alias = "x dimensions"

                  return_mdoc_data[yaml_alias] = xdim

                  yaml_alias = "y dimensions"

                  return_mdoc_data[yaml_alias] = ydim

                else:      

                  return_mdoc_data[yaml_alias] = mdoc_current_line_value

  return(return_mdoc_data) 



# %%

# create Sphinx documentation: sphinx-build -M html docs/ docs/

# remove everything in the _build: make clean

# update Sphinx documentation: make html

import pandas as pd

from pathlib import Path

from starfile import read as starread

from starfile import write as starwrite

import copy

import os

from datetime import datetime

import time


class starFileMeta:

  """_summary_


  Raises:

      Exception: _description_


  Returns:

      _type_: _description_

  """

  def __init__(self, starfile,always_dict = True):

    

    self.always_dict = always_dict

    if isinstance(starfile, str):

      self.starfilePath = starfile

      self.readStar()

    if isinstance(starfile, pd.DataFrame):

      self.df = starfile

      self.dict = None  

    if isinstance(starfile, dict):

      self.dict = starfile

      self.df = None  

    

    

  def readStar(self):

    #Hack to avoid caching

    

    file_path = Path(self.starfilePath)

    if not file_path.exists():

        raise FileNotFoundError(f"The file {file_path} does not exist.")

    tmpTargetPath=tempfile.gettempdir() + os.path.sep + "tmpPointer.tmp" + str(time.time())

    os.symlink(os.path.abspath(self.starfilePath),tmpTargetPath)

    #self.dict = starread(self.starfilePath, always_dict = self.always_dict)

    self.dict = starread(tmpTargetPath,always_dict = self.always_dict)

    os.remove(tmpTargetPath)

    if (len(self.dict.keys())==1):

      self.df=self.dict[next(iter(self.dict.keys()))]

    else:

      self.df=None

    

  def writeStar(self,starfilePath):

    

    if isinstance(self.dict, dict):

       starwrite(self.dict,starfilePath)

       return

    if isinstance(self.df, pd.DataFrame):

        starwrite(self.df,starfilePath)

        return

    

class dataImport():

  

  def __init__(self,targetPath,wkFrames,wkMdoc=None,prefix="auto",logDir=None,invTiltAngle=False):  

    self.targetPath=targetPath

    self.wkFrames=wkFrames

    self.wkMdoc=wkMdoc

    if prefix == "auto":

      current_datetime = datetime.now()

      self.prefix=current_datetime.strftime("%Y-%m-%d-%H-%M-%S_")

    else:

      self.prefix=prefix

    self.mdocLocalFold="mdoc/"

    self.framesLocalFold="frames/"

    frameTargetPattern=self.targetPath + "/" + self.framesLocalFold + os.path.basename(self.wkFrames)

    self.existingFramesSource=[os.path.realpath(file) for file in glob.glob(frameTargetPattern)]

    self.existingMdocSource=self.__getexistingMdoc()

    self.logDir=logDir

    self.logToConsole=False

    self.invTiltAngle=invTiltAngle

    print("import TiltAngle Inv data Import Ini:" + str(self.invTiltAngle))

    if logDir is not None:  

      os.makedirs(logDir, exist_ok=True)

      self.logErrorFile=open(os.path.join(logDir,"run.err"),'a')

      self.logInfoFile=open(os.path.join(logDir,"run.out"),'a')

    importOk=self.checkImport()  

    if importOk:

      self.runImport()

      

  

  def checkImport(self):

    importOk=True

    #duplicates=self.__checkDuplicates(self.wkMdoc, self.existingMdocSource)

    if (not glob.glob(self.wkMdoc)):

       self.__writeLog("error", "no mdocs found check wildcard")

       self.__writeLog("error", self.wkMdoc)

       importOk=False

   

    if (not glob.glob(self.wkFrames)):

       self.__writeLog("error", "no frames found check wildcard")

       self.__writeLog("error", self.wkFrames)

       importOk=False

    duplicateFiles=self.__checkDuplicates(self.wkMdoc, self.existingMdocSource)

    if duplicateFiles:

      importOk=False

      for mdocName in duplicateFiles:

        self.__writeLog("error",str(mdocName) + " name already exists")

      self.__writeLog("error","importing files use prefix to import")

    return importOk

  

  def __del__(self):

    if self.logDir is not None:

      self.logErrorFile.close()

      self.logInfoFile.close()


  def __getexistingMdoc(self):

    

    existingMdoc=[]

    mdocTargetPattern=self.targetPath + "/" + self.mdocLocalFold + os.path.basename(self.wkMdoc)

    print("mdocTargetPattern: " + mdocTargetPattern)

    for fileName in glob.glob(mdocTargetPattern):

      with open(fileName, 'r') as file:

        lines = file.readlines()

        for i, line in enumerate(lines):

          if 'CryoBoost_RootMdocPath' in line:

            existingMdoc.append(line.replace("CryoBoost_RootMdocPath = ",""))

    

    return existingMdoc  

  

  def runImport(self):   

    from src.rw.librw import mdocMeta

    self.mdoc=mdocMeta(self.wkMdoc)

    base_filename = os.path.splitext(self.mdoc.all_df.mdocFileName[0])[0]

    path_to_search = os.path.splitext(self.mdoc.all_df.SubFramePath[0])[0]

    base_filename in path_to_search

    if base_filename in path_to_search:

      self.relcompPrefix=False

    else:   

      self.relcompPrefix=True

    

    os.makedirs(self.targetPath, exist_ok=True)

    framesFold=os.path.join(self.targetPath,self.framesLocalFold)

    os.makedirs(framesFold, exist_ok=True)

    

    self.__genLinks(self.wkFrames,framesFold,self.existingFramesSource)

    

    if self.wkMdoc is not None:

      mdocFold=os.path.join(self.targetPath,self.mdocLocalFold)

      os.makedirs(mdocFold, exist_ok=True)  

      self.__writeAdaptedMdoc(self.wkMdoc,mdocFold,self.existingMdocSource,self.invTiltAngle)

      #self.__genLinks(self.wkMdoc,mdocFold,self.existingMdoc)

  

  def __writeAdaptedMdoc(self,inputPattern,targetFold,existingFiles,invTiltAngle=False):

    

    nrFilesAlreadyImported=len(glob.glob(targetFold + "/*" + os.path.splitext(inputPattern)[1])); 

    for file_path in glob.glob(inputPattern):

        file_name = os.path.basename(file_path)

        tragetFileName = os.path.join(targetFold,self.prefix+file_name)

        print("targetFileName:"+tragetFileName)

        print("inputPatter:"+inputPattern)

        if self.__chkFileExists(file_path,existingFiles)==False:

            if self.relcompPrefix:

                file_nameBase=os.path.splitext(os.path.splitext(file_name)[0])[0]+".mdoc"

                tragetFileName=os.path.join(targetFold,self.prefix+file_nameBase)

            self.__adaptMdoc(self.prefix,file_path,tragetFileName,invTiltAngle)

    

    nrFilesTotalImported=len(glob.glob(targetFold + "/*" + os.path.splitext(inputPattern)[1]));

    nrFilesNewImported=nrFilesTotalImported-nrFilesAlreadyImported

    self.__writeLog("info","Total number of mdocs imported: " + str(nrFilesTotalImported) )

    self.__writeLog("info","Number of new mdoc imported: " + str(nrFilesNewImported) )        

          

  def __adaptMdoc(self,prefix,inputMdoc,outputMdoc,invTiltAngle=False):

    

    with open(inputMdoc, 'r') as file:

      lines = file.readlines()

      for i, line in enumerate(lines):

        if 'SubFramePath' in line:

            lineTmp=line.replace("SubFramePath = \\","")

            lineTmp=line.replace("SubFramePath =","")

            lineTmp=os.path.basename(lineTmp.replace('\\',"/"))

            lineTmp=lineTmp.replace(" ","")

            if self.relcompPrefix:

              baseName=os.path.splitext(os.path.splitext(inputMdoc)[0])[0]

              baseName=os.path.basename(baseName)

              lines[i] = "SubFramePath = " + prefix + baseName  + lineTmp

            else:

              lines[i] = "SubFramePath = " + prefix + lineTmp

        if ('TiltAngle =' in line) and invTiltAngle:  

            key,angle=line.split("=")

            lines[i] = key.replace(" ","") + " = " + str(-1*float(angle)) + "\n"

          

      lines.append("CryoBoost_RootMdocPath = " + os.path.abspath(inputMdoc) + "\n")       

      with open(outputMdoc, 'w') as file:

        file.writelines(lines)

  

  def __genLinks(self,inputPattern,targetFold,existingFiles):  

    

    #print("targetWk:" + targetFold + "/*." + os.path.splitext(inputPattern)[1])

    nrFilesAlreadyImported=len(glob.glob(targetFold + "/*" + os.path.splitext(inputPattern)[1]));

    dftmp = self.mdoc.all_df['SubFramePath'].apply(lambda x: x.replace('\\', '/'))

    framesFromMdoc = [os.path.join(os.path.dirname(inputPattern), os.path.basename(x)) for x in dftmp]

   

    #for file_path in glob.glob(inputPattern):

    for file_path in framesFromMdoc:

        file_name = os.path.basename(file_path)

        tragetFileName = os.path.join(targetFold,self.prefix+file_name)

        if self.relcompPrefix:

            mdoc_file = self.mdoc.all_df[self.mdoc.all_df['SubFramePath'].str.contains(file_name, case=False)]['mdocFileName'].iloc[0]

            mdoc_file = os.path.splitext(os.path.splitext(str(mdoc_file))[0])[0]

            tragetFileName = os.path.join(targetFold,self.prefix+mdoc_file+file_name)

        if self.__chkFileExists(os.path.abspath(file_path),existingFiles)==False:

          try:

             os.symlink(os.path.abspath(file_path),tragetFileName)

             self.__writeLog("info","Created symlink: " + tragetFileName + " -> " + file_path)

          except FileExistsError:

             self.__writeLog("info","Symlink already exists: " + tragetFileName + " -> " + file_path)

          except OSError as e:

             self.__writeLog("error","Error creating symlink for " + tragetFileName + ": " + str(e))

    nrFilesTotalImported=len(glob.glob(targetFold + "/*" + os.path.splitext(inputPattern)[1]));

    nrFilesNewImported=nrFilesTotalImported-nrFilesAlreadyImported

    

    self.__writeLog("info","Total number of tilts imported: " + str(nrFilesTotalImported) )

    self.__writeLog("info","Number of new tilts imported: " + str(nrFilesNewImported) )

    

              

  def __writeLog(self,type,message):

    

    if self.logDir is not None:

      if type=="error":

        self.logErrorFile.write("Error: " + message + "\n")

      elif type=="info":

        self.logInfoFile.write(message + "\n")

      

      if (self.logToConsole): 

        print(message)            

    

    

  def __chkFileExists(self,inputPattern,existingFiles):

    

    if inputPattern in existingFiles:

        return True

    

    return False

  def __checkDuplicates(self, inputPattern, existingFiles):


    name_duplicates=[]

    filePathsSource = [file_path for file_path in glob.glob(inputPattern)]

    filePathsSourceAbs = [os.path.abspath(file_path) for file_path in glob.glob(inputPattern)]

    baseNamesSource = [self.prefix+os.path.basename(file).replace('\n', '') for file in filePathsSource]

    baseNamesExisting = [os.path.basename(file).replace('\n', '') for file in existingFiles]

    mdocExistingAbs=self.__getexistingMdoc()

    mdocExistingAbs=[file.replace('\n', '') for file in mdocExistingAbs]

    

    name_sourceConflict = set(mdocExistingAbs).intersection(set(filePathsSourceAbs))

    name_sourceConflict = set([os.path.basename(file).replace('\n', '') for file in name_sourceConflict])

    name_targetConflict = set(baseNamesExisting).intersection(set(baseNamesSource))


    if  name_sourceConflict>=name_targetConflict:

        name_duplicates=[]

    else:    

        name_duplicates=name_targetConflict

    

    return name_duplicates

        

       


class schemeMeta:

  """

  """

  def __init__(self, schemeFolderPath):

    self.CRYOBOOST_HOME=os.getenv("CRYOBOOST_HOME")

    self.conf=cbconfig(self.CRYOBOOST_HOME + "/config/conf.yaml")

    self.schemeFilePath=schemeFolderPath+os.path.sep+"scheme.star"

    self.schemeFolderPath=schemeFolderPath

    self.read_scheme()

    

  def read_scheme(self):

    self.scheme_star=starFileMeta(self.schemeFilePath)

    self.jobs_in_scheme = self.scheme_star.dict["scheme_edges"].rlnSchemeEdgeOutputNodeName.iloc[1:-1]

    self.job_star = {

    f"{job}": starFileMeta(os.path.join(self.schemeFolderPath, f"{job}/job.star"))

    for job in self.jobs_in_scheme}

    #self.scheme_star_dict = starFileMeta(self.schemeFilePath)

    self.nrJobs = len(self.jobs_in_scheme)

  

  def getJobOptions(self, jobName):

    return self.job_star[jobName].dict["joboptions_values"]   

  

  def jobListToNodeList(self,jobList,tag=None):

    

    Node = namedtuple('Node', ['type', 'tag', 'inputType', 'inputTag'])

    nodes = []

    intag=None

    all_types=[]

    for job in jobList:

      inputType=self.getInputJobType(job)

      if inputType is not None and inputType not in all_types and len(all_types)>0:

          inputType =all_types[-1]

      oneNode = Node(type=job, tag=tag, inputType=inputType, inputTag=intag)

      nodes.append(oneNode)

      all_types.append(job)

      intag=tag

    nodes_dict = {i: node for i, node in enumerate(nodes)}   

    #nodes_dict = {i: row.to_dict() for i, row in nodes.iterrows()}

    nodes_df = pd.DataFrame.from_dict(nodes_dict, orient='index')

    

    return nodes,nodes_df

  

  def addNoiseToNoiseFilter(self):

    pass

  def removeNoiseToNoiseFilter(self):

    nFilterJobs=self.conf.confdata['computing']['JOBTypesApplication']['Noise2NoiseFilterJobs']

    nonFilterJobs=[job for job in self.jobs_in_scheme if job not in set(nFilterJobs)]

    nodes,nodes_df=self.jobListToNodeList(nonFilterJobs)

    schemeAdapted=self.filterSchemeByNodes(nodes_df)

    return schemeAdapted

  

  def getMajorInputParamNameFromJob(self, jobName):

    """

    Returns the input type for a given job name.

    

    Args:

        jobName (str): The name of the job.

        

    Returns:

        str: The input type for the job.

        

    Raises:

        Exception: If no input type is found for the job.

    """

    df = self.job_star[jobName].dict['joboptions_values']

    ind=df.rlnJobOptionVariable=="input_star_mics"

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_tiltseries" 

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_mic"

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_tomoset"

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_optimisation"

    if not any(ind):

        raise Exception("nether input_star_mics nor in_tiltseries found")

    row_index = df.index[ind]

    # inputType=os.path.basename(os.path.dirname(df.loc[row_index, "rlnJobOptionValue"].item()))

    inputParamName=df.loc[row_index, "rlnJobOptionVariable"].item()

    inputParamValue=df.loc[row_index, "rlnJobOptionValue"].item()

    scName=self.scheme_star.dict['scheme_general']['rlnSchemeName']

    inutJobType=os.path.dirname(inputParamValue.split(scName)[1])

    return inputParamName,inputParamValue,inutJobType

  

  def removeParticleJobs(self):

    """

    Removes particle jobs from the scheme.

    

    Returns:

        schemeAdapted (schemeMeta): A new schemeMeta object with particle jobs removed.

    """

    particleJobs=self.conf.confdata['computing']['JOBTypesApplication']['ParticleJobs']

    nonParticleJobs=[job for job in self.jobs_in_scheme if job not in set(particleJobs)]

    nodes,nodes_df=self.jobListToNodeList(nonParticleJobs)

    

    schemeAdapted=self.filterSchemeByNodes(nodes_df)

    return schemeAdapted

  

  def removefilterTiltsJobs(self):

    """

    Removes filter jobs from the scheme.

    

    Returns:

        schemeAdapted (schemeMeta): A new schemeMeta object with filter jobs removed.

    """

    filterJobs=self.conf.confdata['computing']['JOBTypesApplication']['FilterTiltsJobs']

    nonFilterJobs=[job for job in self.jobs_in_scheme if job not in set(filterJobs)]

    nodes,nodes_df=self.jobListToNodeList(nonFilterJobs)

    

    schemeAdapted=self.filterSchemeByNodes(nodes_df)

    return schemeAdapted

  

  def addParticleJobs(self,tags):

    particleJobs=self.conf.confdata['computing']['JOBTypesApplication']['ParticleJobs']

    nonParticleJobs=[job for job in self.jobs_in_scheme if job not in set(particleJobs)]

    nodes,nodes_df=self.jobListToNodeList(nonParticleJobs)

    for tag in tags:

      ndf,nodesPlTag_df=self.jobListToNodeList(particleJobs,tag)

      nodes_df = pd.concat([nodes_df, nodesPlTag_df], ignore_index=True)

      

    #nodes_dict = {i: node for i, node in enumerate(nodes)}   

    #nodes_dict = {i: row.to_dict() for i, row in nodes.iterrows()}

    #nodes_df = pd.DataFrame.from_dict(nodes_dict, orient='index')

    schemeAdapted=self.filterSchemeByNodes(nodes_df)

    return schemeAdapted

    

  def getInputJobType(self, jobName):


    if jobName=="importmovies": #first job for every pipeline

      return None

    

    df = self.job_star[jobName].dict['joboptions_values']

    ind=df.rlnJobOptionVariable=="input_star_mics"

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_tiltseries" 

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_mic"

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_tomoset"

    if not any(ind):

        ind=df.rlnJobOptionVariable=="in_optimisation"

    if not any(ind):

        raise Exception("nether input_star_mics nor in_tiltseries found")

    row_index = df.index[ind]

    inputType=os.path.basename(os.path.dirname(df.loc[row_index, "rlnJobOptionValue"].item()))

    

    return inputType

    

  def filterSchemeByNodes(self, nodes_df,filterMode="TypeOnly"):

    

   

    filtEdges_df=self.filterEdgesByNodes(self.scheme_star.dict["scheme_edges"], nodes_df)

    jobStar_dict=self.job_star

    jobStar_dictFilt=self.filterjobStarByNodes(jobStar_dict,nodes_df,filterMode)

    schemeJobs_dfFilt=self._filterSchemeJobsByNodes(self.scheme_star.dict["scheme_jobs"],nodes_df)

    

    scFilt=copy.deepcopy(self)

    scFilt.scheme_star.dict["scheme_edges"]=filtEdges_df

    scFilt.scheme_star.dict["scheme_jobs"]=schemeJobs_dfFilt

    scFilt.job_star=jobStar_dictFilt

    scFilt.nrJobs =len(scFilt.job_star)

    scFilt.jobs_in_scheme = scFilt.scheme_star.dict["scheme_edges"].rlnSchemeEdgeOutputNodeName.iloc[1:-1]

    #scFilt.jobTypes_in_scheme = nodes_df["type"].iloc[:]

    

    return scFilt   

  

  def _filterSchemeJobsByNodes(self,schemJobs,nodes_df):

    

    schemeJobs_dfFilt = pd.DataFrame(columns=schemJobs.columns)

    for index, row in nodes_df.iterrows():

        new_row=copy.deepcopy(schemJobs.head(1))

        new_row['rlnSchemeJobNameOriginal']=row['type'] + ('_' + row['tag'] if row['tag'] != None else '')

        new_row['rlnSchemeJobName']=row['type'] + ('_' + row['tag'] if row['tag'] != None else '')

        schemeJobs_dfFilt=pd.concat([schemeJobs_dfFilt,new_row])

    return schemeJobs_dfFilt

    

  def filterjobStarByNodes(self,jobStarDict,nodes_df, filterMode="TypeOnly"): 

    

    schemeJobs_dfFilt={}

    schemeName=self.scheme_star.dict["scheme_general"]["rlnSchemeName"]

    #for nodeid, node in nodes.items():

    for index, row in nodes_df.iterrows():

      jobNameWithTag=jobNameWithTag = row['type'] + ('_' + row['tag'] if row['tag'] != None else '')

      jobName=row['type']

      if filterMode=="TypeOnly":

        key= jobName

      else:

        key=jobNameWithTag

      schemeJobs_dfFilt[jobNameWithTag]=copy.deepcopy(jobStarDict[key])

      df=schemeJobs_dfFilt[jobNameWithTag].dict["joboptions_values"]

      ## adapt input

      if row['inputType'] is not None:

        #input=schemeName+row['inputType'] + ('_' + row['inputTag'] if row['inputTag'] != 'None' else '')

          input=schemeName+row['inputType'] + ('_' + str(row['inputTag']) if row['inputTag'] not in [None, 'None'] else '')

          input=input+os.path.sep+os.path.basename(self.conf.getJobOutput(row['inputType']))

          ind=df.rlnJobOptionVariable=="input_star_mics"

          if not any(ind):

              ind=df.rlnJobOptionVariable=="in_tiltseries" 

          if not any(ind):

              ind=df.rlnJobOptionVariable=="in_mic"

          if not any(ind):

              ind=df.rlnJobOptionVariable=="in_tomoset"

          if not any(ind):

              ind=df.rlnJobOptionVariable=="in_optimisation"

          if not any(ind):

              raise Exception("nether input_star_mics nor in_tiltseries found")

          

          row_index = schemeJobs_dfFilt[jobNameWithTag].dict["joboptions_values"].index[ind]

          schemeJobs_dfFilt[jobNameWithTag].dict["joboptions_values"].loc[row_index, "rlnJobOptionValue"] = input

          

    return schemeJobs_dfFilt 

  

  

  

  def filterEdgesByNodes(self,edge_df, nodes_df):     

    

    firstEdge=edge_df.loc[0:0].copy(deep=True)

    jobNameWithTagOld=firstEdge["rlnSchemeEdgeOutputNodeName"].item()

    #for nodeid, node in nodes.items(): 

    schemeEdge_df=firstEdge

    for index, row in nodes_df.iterrows():

        jobNameWithTag=jobNameWithTag = row['type'] + ('_' + row['tag'] if row['tag'] != None else '')

        dfOneEdge=firstEdge.copy(deep=True)

        dfOneEdge["rlnSchemeEdgeInputNodeName"]=jobNameWithTagOld

        dfOneEdge["rlnSchemeEdgeOutputNodeName"]=jobNameWithTag

        schemeEdge_df=pd.concat([schemeEdge_df,dfOneEdge],ignore_index=True) 

        jobNameWithTagOld=jobNameWithTag

   

    dfOneEdge=firstEdge.copy(deep=True)

    dfOneEdge["rlnSchemeEdgeInputNodeName"]=jobNameWithTag

    dfOneEdge["rlnSchemeEdgeOutputNodeName"]="EXIT"

    schemeEdge_df=pd.concat([schemeEdge_df,dfOneEdge],ignore_index=True) 

    

    return schemeEdge_df

      

  def locate_val(self,job_name:str,var:str):

    """

    locates the value defined of the dict defined in the job_star_dict dictionary so it can be displayed and edited.


    Args:

      job_name (str): job name as str as it's stated in the job_star_dict ("importmovies", "motioncorr", "ctffind", "aligntilts", "reconstruction").

      var (str): name of variable/parameter that should be changed (parameters as defined in the job.star files).

      job_dict (str): dataframe that should be accessed inside the job defined in job_name (standard input is the df containing the parameters).

      column_variable (str): column in the dataframe containing the parameters (standard input is the correct name).

      column_value (str): column in the dataframe containing the values assigned to each parameter (standard input is the correct name).


    Returns:

      str: value that is currently assigned to the defined parameter of the defined job.

    """

    job_dict = "joboptions_values", 

    column_variable = "rlnJobOptionVariable" 

    column_value = "rlnJobOptionValue"

    val = self.job_star[job_name].dict[job_dict].loc[self.job_star[job_name].dict[job_dict][column_variable] == var, column_value].values[0]

    return val

  

  def update_job_star_dict(self,job_name, param, value):

      """

      updates the job_star_dict dictionary (containing all .star files of the repective jobs) with the values provided.


      Args:

        job_name (str): job name as str as it's stated in the job_star_dict ("importmovies", "motioncorr", "ctffind", "aligntilts", "reconstruction").

        param (str): parameter that should be updated as str as it's called in the respective job.star file.

        value (str): new value that should be placed in the job.star file for the set parameter.


      Returns:

        job_star_dict with updated value for respective parameter.

      """

      index = self.job_star[job_name].dict["joboptions_values"].index[self.job_star[job_name].dict["joboptions_values"]["rlnJobOptionVariable"] == param]

      self.job_star[job_name].dict["joboptions_values"].iloc[index, 1] = value

      

      return self.job_star[job_name].dict



  

  def write_scheme(self,schemeFolderPath):

     self.schemeFilePath =  schemeFolderPath+os.path.sep+"scheme.star"

     self.schemeFolderPath =  schemeFolderPath

     os.makedirs(schemeFolderPath, exist_ok=True)

     self.scheme_star.writeStar(schemeFolderPath+os.path.sep+"scheme.star")


    

    # repeat for all jobs, creating a job.star file in these directories

     for job in self.jobs_in_scheme:

        jobFold = schemeFolderPath+os.path.sep+job 

        os.makedirs(jobFold, exist_ok=True)

        job_star = self.job_star[job]

        job_star.writeStar(jobFold+os.path.sep+"job.star")

        


class tiltSeriesMeta:

    """

    Class for handling tilt series metadata.


    Args:

        tiltseriesStarFile (str): Path to the tilt series star file.

        relProjPath (str): Path to the relative project directory.


    Attributes:

        tilt_series_df (pd.DataFrame): DataFrame containing the tilt series information.

        tiltseriesStarFile (str): Path to the tilt series star file.

        relProjPath (str): Path to the relative project directory.


    Methods:

        __init__(self, tiltseriesStarFile, relProjPath): Initializes the tiltSeriesMeta class.

        readTiltSeries(self): Reads the tilt series star file and its associated tilt series files.

        filterTilts(self, fitlterParams): Filters the tilt series based on the provided parameters.

        filterTiltSeries(self, fitlterParams): Filters the tilt series based on the provided parameters.

        writeTiltSeries(self, tiltseriesStarFile, tiltSeriesStarFolder='tilt_series'): Writes the tilt series star file and its associated tilt series files.

    """


    def __init__(self, tiltseriesStarFile, relProjPath=''):

        """

        Initializes the tiltSeriesMeta class.


        Args:

            tiltseriesStarFile (str): Path to the tilt series star file.

            relProjPath (str): Path to the relative project directory.


        Attributes:

            tilt_series_df (pd.DataFrame): DataFrame containing the tilt series information.

            tiltseriesStarFile (str): Path to the tilt series star file.

            relProjPath (str): Path to the relative project directory.

        """

        self.tilt_series_df = None

        self.tiltseriesStarFile = tiltseriesStarFile

        self.relProjPath = relProjPath

        self.readTiltSeries()


    def readTiltSeries(self):

        

        print("Reading: " + self.tiltseriesStarFile)

        tilt_series=starFileMeta(self.tiltseriesStarFile)

        #tilt_series_df =tilt_series.dict[next(iter(tilt_series.dict.keys()))] 

        self.nrTomo=tilt_series.df.shape[0]

        all_tilts_df = pd.DataFrame()

        tilt_series_tmp = pd.DataFrame()

        

        i = 0

        for tilt_seriesName in tilt_series.df["rlnTomoTiltSeriesStarFile"]:

           # tilt_star_df = read_star(self.relProjPath + tilt_series)

            tilt_star=starFileMeta(self.relProjPath + tilt_seriesName)

            all_tilts_df = pd.concat([all_tilts_df, tilt_star.df], ignore_index=True)

            tmp_df = pd.concat([tilt_series.df.iloc[[i]]] * tilt_star.df.shape[0], ignore_index=True)

            tilt_series_tmp = pd.concat([tilt_series_tmp, tmp_df], ignore_index=True)

            i += 1


        #all_tilts_df = pd.concat([all_tilts_df, tilt_series_tmp], axis=1)

        if "rlnTomoZRot" in tilt_series_tmp.columns:

           tilt_series_tmp = tilt_series_tmp.rename(columns={"rlnTomoZRot": "rlnTomoZRotTs"})

        all_tilts_df = pd.concat([tilt_series_tmp,all_tilts_df], axis=1)


        columns_to_check = [col for col in all_tilts_df.columns if col not in ['rlnCtfScalefactor']]

        all_tilts_df.dropna(subset=columns_to_check,inplace=True)  # check !!

        #generte key to merge later on  

        if 'rlnMicrographName' in all_tilts_df.columns:

          k=all_tilts_df['rlnMicrographName'].apply(os.path.basename)

        else:

          k=all_tilts_df['rlnMicrographMovieName'].apply(os.path.basename)

        

        if (k.is_unique):

          all_tilts_df['cryoBoostKey']=k

        else:

          raise Exception("rlnMicrographName is not unique !!")        


        # duplicate_cols = all_tilts_df.columns[all_tilts_df.columns.duplicated()]

        # if not duplicate_cols.empty:

        #   all_tilts_df= all_tilts_df.loc[:, ~all_tilts_df.columns.duplicated()]

        

        self.all_tilts_df = all_tilts_df

        self.tilt_series_df = tilt_series.df

        self.__extractInformation()

        # Store the key values

        key_values = self.all_tilts_df['cryoBoostKey']

        # Remove the column

        self.all_tilts_df = self.all_tilts_df.drop('cryoBoostKey', axis=1)

        # Add it back (it will be added as the last column)

        self.all_tilts_df['cryoBoostKey'] = key_values

        

    def writeTiltSeries(self, tiltseriesStarFile, tiltSeriesStarFolder='tilt_series'):

        """

        Writes the tilt series star file and its associated tilt series files.


        Args:

            self (tiltSeriesMeta): An instance of the tiltSeriesMeta class.

            tiltseriesStarFile (str): Path to the tilt series star file.

            tiltSeriesStarFolder (str): Folder name for the tilt series star files.


        Example:

            >>> ts = tiltSeriesMeta("/path/to/tilt_series_star_file", "/path/to/rel_proj_path")

            >>> ts.writeTiltSeries("/tmp/fbeck/test8/tiltseries.star")

        """

        print("Writing: " + tiltseriesStarFile)

        #generate tiltseries star from all dataframe ...more generic if filter removes all tilts of one series

        #ts_df = self.all_tilts_df[self.tilt_series_df.columns].copy()

        #indStart=self.all_tilts_df.shape[1]-self.tilt_series_df.shape[1]

        ts_df = self.all_tilts_df.iloc[:,0:self.tilt_series_df.shape[1]].copy()

        ts_df=ts_df.drop("cryoBoostKey",axis=1,errors='ignore')

        

        ts_df.drop_duplicates(inplace=True)

        tsFold = os.path.dirname(tiltseriesStarFile) + os.path.sep + tiltSeriesStarFolder + os.path.sep

        ts_df['rlnTomoTiltSeriesStarFile'] = ts_df['rlnTomoTiltSeriesStarFile'].apply(lambda x: os.path.join(tsFold, os.path.basename(x)))

        os.makedirs(tsFold,exist_ok=True)

        ts_dict={}

        if "rlnTomoZRotTs" in ts_df.columns:

           ts_df = ts_df.rename(columns={"rlnTomoZRotTs": "rlnTomoZRot"})

        ts_dict['global']=ts_df

        

        stTs=starFileMeta(ts_dict)

        stTs.writeStar(tiltseriesStarFile)

       

        fold = os.path.dirname(tiltseriesStarFile)

        Path(fold + os.sep + tiltSeriesStarFolder).mkdir(exist_ok=True)

        for tilt_series in self.tilt_series_df["rlnTomoTiltSeriesStarFile"]:

            oneTs_df = self.all_tilts_df[self.all_tilts_df['rlnTomoTiltSeriesStarFile'] == tilt_series].copy()

            #oneTs_df.drop(self.tilt_series_df.columns, axis=1, inplace=True)

            #oneTs_df=test=self.all_tilts_df.iloc[:,0:indStart-1]

            oneTs_df=oneTs_df.iloc[:,self.tilt_series_df.shape[1]:]

            tomoName = self.all_tilts_df.loc[self.all_tilts_df['rlnTomoTiltSeriesStarFile'] == tilt_series, 'rlnTomoName'].unique()[0]

            oneTS_dict={}

            oneTS_dict[tomoName]=oneTs_df

            stOneTs=starFileMeta(oneTS_dict)

            stOneTs.writeStar(fold + os.sep + tiltSeriesStarFolder + os.sep + os.path.basename(tilt_series))

           

            

    def filterTilts(self, fitlterParams):

      """

      Filters the tilt series based on the provided parameters.


      Args:

          self (tiltSeriesMeta): An instance of the tiltSeriesMeta class.

          fitlterParams (dict): A dictionary containing the parameters and their respective thresholds for filtering the tilt series.


      Raises:

          ValueError: If any of the required star files are not found.


      Returns:

          None: This method modifies the tiltSeriesMeta object in-place.


      Example:

          >>> ts = tiltSeriesMeta("/path/to/tilt_series_star_file", "/path/to/rel_proj_path")

          >>> ts.filterTilts({"rlnCtfMaxResolution": (7.5, 30,-35,35), "rlnDefocusU": (1, 80000,-60,60)})

      """

      pTilt = "rlnTomoNominalStageTiltAngle"  # fieldName for tiltRange

      dfTmp = self.all_tilts_df


      for param, thresholds in fitlterParams.items():

          

          if isinstance(thresholds, set):

              v = dfTmp[param].isin(thresholds)

          else:    

              if isinstance(thresholds[0], str):

                  v = dfTmp[param] == thresholds

              else:

                  vParamRange = (dfTmp[param] > thresholds[0]) & (dfTmp[param] < thresholds[1])

                  vTiltRange = (dfTmp[pTilt] > thresholds[2]) & (dfTmp[pTilt] < thresholds[3])

                  # Tiltrange defines for which tilts the filter gets applied

                  v = vParamRange | (vTiltRange == False)


          dfTmp = dfTmp[v]


      dfTmp.reset_index(drop=True, inplace=True)

      self.all_tilts_df = dfTmp

      

      ts_df = self.all_tilts_df[self.tilt_series_df.columns].copy()

      ts_df.drop_duplicates(inplace=True)

      self.tilt_series_df=ts_df

      self.nrTomo=len(self.tilt_series_df)

      

    def reduceToNonOverlab(self,tsSubset):

      tomoNamesSub=set(tsSubset.tilt_series_df["rlnTomoName"])

      tomoNamesFull=set(self.tilt_series_df["rlnTomoName"])

      tomoNamesDiff=tomoNamesFull-tomoNamesSub

      self.filterTilts({"rlnTomoName": tomoNamesDiff})

    

    def mergeTiltSeries(self,tsToAdd):

      

      self.all_tilts_df=pd.concat([self.all_tilts_df,tsToAdd.all_tilts_df],axis=0)

      self.all_tilts_df.reset_index(drop=True, inplace=True)

      ts_df = self.all_tilts_df[self.tilt_series_df.columns].copy()

      ts_df.drop_duplicates(inplace=True)

      self.tilt_series_df=ts_df

      self.nrTomo=len(self.tilt_series_df) 

      

    def filterTiltSeries(self,minNumTilts,fitlterParams):

      pass 

    

    def getMicrographMovieNameFull(self):

      return self.relProjPath+self.all_tilts_df['rlnMicrographName'] 

    

    def addColumns(self,columns_df):  

      """

      Adds new columns to the DataFrame stored in the instance variable `all_tilts_df`.


      This method takes a DataFrame `columns_df` containing new columns to be added to `all_tilts_df`. 

      The merge is performed on the 'rlnMicrographMovieName' column, using a left join. This means that all 

      entries in `all_tilts_df` will be retained, and matching entries from `columns_df` will be added based 

      on the 'cryoBoostKey' column. If there are no matching entries in `columns_df`, the new 

      columns will contain NaN values for those rows.


      Args:

      - columns_df (DataFrame): A pandas DataFrame containing the columns to be added to `all_tilts_df`. 

         It must include a 'cryoBoostKey' column for the merge operation.


      Returns:

      - None. The method updates `all_tilts_df` in place by adding the new columns from `columns_df`.


      """ 

      k=columns_df['cryoBoostKey'].apply(os.path.basename)

      if (k.is_unique):

        columns_df['cryoBoostKey']=k

      

      self.all_tilts_df=self.all_tilts_df.merge(columns_df,on='cryoBoostKey',how='left') 

    

    def __extractInformation(self):        

      self.tsInfo=type('', (), {})()

      self.tsInfo.allUnique=1

      

      directories = self.all_tilts_df["rlnMicrographMovieName"].apply(lambda x: os.path.dirname(x))

      extensions = self.all_tilts_df["rlnMicrographMovieName"].apply(lambda x: os.path.splitext(x)[1])


      unique_directories = directories.unique()

      unique_extensions = extensions.unique()

      self.tsInfo.allUnique=self.tsInfo.allUnique and len(unique_directories)==1 and len(unique_extensions)==1

      self.tsInfo.frameFold=unique_directories[0]

      self.tsInfo.frameExt=unique_extensions[0]

      

      if len(self.all_tilts_df["rlnMicrographPreExposure"].drop_duplicates())>1:

        self.tsInfo.expPerTilt=self.all_tilts_df["rlnMicrographPreExposure"].drop_duplicates().sort_values().iloc[1]

      else:

        self.tsInfo.expPerTilt=self.all_tilts_df["rlnMicrographPreExposure"].drop_duplicates().sort_values().iloc[0]

      

      self.tsInfo.numTiltSeries=self.tilt_series_df.shape[0]

      

      df_attr="rlnMicrographName"

      #print(self.all_tilts_df.columns)

      if df_attr in self.all_tilts_df.columns:

          warpFrameSeriesFold=self.all_tilts_df[df_attr].sort_values().iloc[0]

          self.tsInfo.warpFrameSeriesFold=os.path.split(warpFrameSeriesFold)[0].replace("average","")

      else:

          print(f"Warning: {df_attr} not found in tilt_series_df")

      

      attributes = {  

                  'volt': 'rlnVoltage',

                  'cs': 'rlnSphericalAberration',

                  'cAmp': 'rlnAmplitudeContrast',

                  'framePixS': 'rlnMicrographOriginalPixelSize',

                  'tiltAxis': 'rlnTomoNominalTiltAxisAngle',

                  'keepHand': 'rlnTomoHand'

                  }


      for attr, df_attr in attributes.items():

          if df_attr in self.all_tilts_df.columns:

              unique_values = self.all_tilts_df[df_attr].unique()

              setattr(self.tsInfo, attr, unique_values)

              self.tsInfo.allUnique = self.tsInfo.allUnique and len(unique_values) == 1

              if len(unique_values) > 0:

                  setattr(self.tsInfo, attr, unique_values[0])

          else:

              print(f"Warning: {df_attr} not found in tilt_series_df")

            

      self.tsInfo.tomoSize = None

      if "rlnTomoReconstructedTomogram" in self.all_tilts_df.columns:

        tomoName = self.all_tilts_df["rlnTomoReconstructedTomogram"].iloc[0]

        if os.path.exists(tomoName):

            with mrcfile.open(tomoName, header_only=True) as mrc:

                self.tsInfo.tomoSize = [mrc.header.nx, mrc.header.ny, mrc.header.nz]

        else:

            print(f"Warning: Tomogram file {tomoName} does not exist.")

```


schemeGui.py:
```
import sys

import os

import pandas as pd

import glob,random  

from PyQt6 import QtWidgets

from PyQt6.QtGui import QTextCursor

from PyQt6.uic import loadUi

from PyQt6.QtWidgets import QDialog,QTableWidget,QScrollArea,QTableWidgetItem, QVBoxLayout, QApplication, QMainWindow,QMessageBox,QWidget,QLineEdit,QComboBox,QRadioButton,QCheckBox,QSizePolicy 

from PyQt6.QtCore import Qt,QSignalBlocker

from src.pipe.libpipe import pipe

from src.rw.librw import starFileMeta,mdocMeta

from src.misc.system import run_command_async

from src.gui.libGui import get_user_selection,externalTextViewer,browse_dirs,browse_files,checkDosePerTilt,browse_filesOrFolders,change_values,change_bckgrnd,checkGainOptions,get_inputNodesFromSchemeTable,messageBox 

from src.gui.libGui import MultiInputDialog,statusMessageBox

from src.misc.libmask import genMaskRelion,caclThreshold

from src.rw.librw import schemeMeta,cbconfig,read_mdoc,importFolderBySymlink

from src.gui.edit_scheme import EditScheme

from src.gui.quick_setup import quickSetup

from src.gui.generateTemplate import TemplateGen

from src.misc.libimVol import processVolume

from src.misc.eerSampling import get_EERsections_per_frame

import subprocess, shutil

from PyQt6.QtCore import QTimer 

import mrcfile

import datetime


current_dir = os.path.dirname(os.path.abspath(__name__))

# change the path to be until src

root_dir = os.path.abspath(os.path.join(current_dir, '../'))

sys.path.append(root_dir)


#from lib.functions import get_value_from_tab


class MainUI(QMainWindow):

    """

    Main window of the UI.

    """

    def __init__(self,args):

        """

        Setting up the buttons in the code and connecting them to their respective functions.

        """ 

        super(MainUI, self).__init__()

        loadUi(os.path.abspath(__file__).replace('.py','.ui'), self)

       

        self.system=self.selSystemComponents()

        self.cbdat=self.initializeDataStrcuture(args)

        if (self.cbdat is None):

            QApplication.instance().quit()

            sys.exit()

            return

        self.setCallbacks()

        self.adaptWidgetsToJobsInScheme()

        self.genSchemeTable()

        

        self.groupBox_WorkFlow.setEnabled(False)

        self.groupBox_Setup.setEnabled(False)

        #self.groupBox_Project.setEnabled(False)

        self.tabWidget.setCurrentIndex(0)

        self.tabWidget.setTabVisible(1, False)

        

        if (self.cbdat.args.autoGen or self.cbdat.args.skipSchemeEdit):

            self.makeJobTabsFromScheme()

    

    def adaptWidgetsToJobsInScheme(self):

        

        if "fsMotionAndCtf" in self.cbdat.scheme.jobs_in_scheme.values:

            self.dropDown_gainRot.clear()  # This will remove all items from the dropdown

            self.dropDown_gainRot.addItem("No transpose")

            self.dropDown_gainRot.addItem("Transpose")

        

         

    def selSystemComponents(self):

        system = type('', (), {})() 

        if shutil.which("zenity") is None:

           system.filebrowser="qt"

        else:   

           system.filebrowser="zenity"

        return system

                

    def initializeDataStrcuture(self,args):

        #custom varibales

        cbdat = type('', (), {})() 

        cbdat.CRYOBOOST_HOME=os.getenv("CRYOBOOST_HOME")

        warpSchemeExists=os.path.exists(str(args.proj) +  "/Schemes/" + "warp_tomo_prep" + "/scheme.star")

        relioSchemeExists=os.path.exists(str(args.proj) +  "/Schemes/" + "relion_tomo_prep" + "/scheme.star")

        if relioSchemeExists:

            args.scheme="relion_tomo_prep"

        if warpSchemeExists:

            args.scheme="warp_tomo_prep"

        

        if args.numInputArgs > -1 and not (warpSchemeExists or relioSchemeExists):

            dialog = quickSetup(args)

            res=dialog.exec()

            args=dialog.getResult()

            if args is None:

                print("cancelled quick setup")

                cbdat= None

                return cbdat

            

        if args.scheme=="gui":

            args.scheme=get_user_selection()

        if args.scheme=="relion_tomo_prep" or args.scheme=="default":      

            cbdat.defaultSchemePath=cbdat.CRYOBOOST_HOME + "/config/Schemes/relion_tomo_prep/"

        if args.scheme=="warp_tomo_prep":

            cbdat.defaultSchemePath=cbdat.CRYOBOOST_HOME + "/config/Schemes/warp_tomo_prep/"

        if args.scheme=="browse":

            cbdat.defaultSchemePath=browse_dirs(target_field=None,target_fold=None,dialog=self.system.filebrowser)

           

        cbdat.confPath=cbdat.CRYOBOOST_HOME + "/config/conf.yaml"

        cbdat.pipeRunner= None

        cbdat.conf=cbconfig(cbdat.confPath)     

        cbdat.localEnv=cbdat.conf.confdata['local']['Environment']+";"

        cbdat.args=args

        cbdat.filtScheme=1

        if os.path.exists(str(args.proj) +  "/Schemes/" + args.scheme + "/scheme.star"):

            print("WARNING missing reload of options")

            print("======> check mdoc invert")

            print("WARNING missing reload of options")

            cbdat.scheme=schemeMeta(args.proj +  "/Schemes/" + args.scheme )

            args.scheme=cbdat.scheme

            invTilt=self.textEdit_invertTiltAngle.toPlainText()

            if invTilt == "Yes":

                invTilt=True

            else:

                invTilt=False

            cbdat.pipeRunner=pipe(args,invMdocTiltAngle=invTilt);

            cbdat.args.skipSchemeEdit=True

            cbdat.filtScheme=0

        else:    

            cbdat.scheme=schemeMeta(cbdat.defaultSchemePath)

            if args.Noise2Noise == "False":

                cbdat.scheme=cbdat.scheme.removeNoiseToNoiseFilter()

            if args.FilterTilts == "False":

                cbdat.scheme=cbdat.scheme.removefilterTiltsJobs()

            if args.species != "noTag" and args.species != "None":

                speciesList = [x.strip() for x in args.species.split(',')]

                cbdat.scheme=cbdat.scheme.addParticleJobs(speciesList) 

            if args.species == "None":

                cbdat.scheme=cbdat.scheme.removeParticleJobs()

                   

        

        return cbdat

    

    def setCallbacks(self):

        self.btn_makeJobTabs.clicked.connect(self.makeJobTabsFromScheme)

        #self.groupBox_in_paths.setEnabled(False)

        self.line_path_movies.textChanged.connect(self.setPathMoviesToJobTap)

        self.line_path_mdocs.textChanged.connect(self.setPathMdocsToJobTap)

        self.line_path_mdocs.textChanged.connect(self.updateTomogramsForTraining)

        self.line_path_gain.textChanged.connect(self.setPathGainToJobTap)

        self.line_path_new_project.textChanged.connect(self.updateLogViewer)

        self.line_path_crImportPrefix.textChanged.connect(self.updateTomogramsForTraining)

        self.dropDown_gainRot.activated.connect(self.setGainRotToJobTap)

        self.dropDown_gainFlip.activated.connect(self.setGainFlipJobTap)

        self.textEdit_pixelSize.textChanged.connect(self.setPixelSizeToJobTap)

        self.textEdit_dosePerTilt.textChanged.connect(self.setdosePerTiltToJobTap)

        self.textEdit_nomTiltAxis.textChanged.connect(self.setTiltAxisToJobTap)

        

        #self.textEdit_invertTiltAngle.textChanged.connect(self.setInvertTiltAngleToJobTap)

        self.textEdit_invertDefocusHand.textChanged.connect(self.setInvertDefocusHandToJobTap)

        

        self.textEdit_eerFractions.textChanged.connect(self.setEerFractionsToJobTap)

        self.textEdit_areTomoSampleThick.textChanged.connect(self.setAreTomoSampleThickToJobTap)

        

        self.textEdit_areTomoPatch.textChanged.connect(self.setAreTomoPatchToJobTap)

        self.textEdit_algRescaleTilts.textChanged.connect(self.setAlgRescaleTiltsJobTap)

        

        self.textEdit_ImodPatchSize.textChanged.connect(self.setImodPatchSizeToJobTap)

        self.textEdit_imodPatchOverlap.textChanged.connect(self.setImodPatchOverlapToJobTap)

        

        self.textEdit_refineTiltAxisNrTomo.textChanged.connect(self.setTiltAxisRefineParamToJobTap)

        self.textEdit_refineTiltAxisIter.textChanged.connect(self.setTiltAxisRefineParamToJobTap)

        self.dropDown_doRefineTiltAxis.activated.connect(self.setTiltAxisRefineParamToJobTap)

        

        self.dropDown_tomoAlignProgram.activated.connect(self.setTomoAlignProgramToJobTap)

        self.btn_openWorkFlowLog.clicked.connect(self.openExtLogViewerWorkFlow)

        self.btn_openWorkFlowLog.clicked.connect(self.openExtLogViewerWorkFlow)

        self.btn_openJobOutpuLog.clicked.connect(self.openExtLogViewerJobOutput)

        self.btn_openJobErrorLog.clicked.connect(self.openExtLogViewerJobError)

        #self.textBrowser_workFlow.clicked.connect(self.openExtLogViewer) 

        self.btn_browse_movies.clicked.connect(self.browsePathMovies)

        self.btn_browse_mdocs.clicked.connect(self.browsePathMdocs)

        self.btn_browse_denoisingModel.clicked.connect(self.browseDenoisingModel)

        self.textEdit_tomoForDenoiseTrain.textChanged.connect(self.setTomoForDenoiseTrainToJobTap)

        self.textEdit_pathDenoiseModel.textChanged.connect(self.setPathDenoiseModelToJobTap)

        self.textEdit_modelForFilterTilts.textChanged.connect(self.setmodelForFilterTiltsToJobTap)

        self.textEdit_probThr.textChanged.connect(self.setProbThrToJobTap)

        self.dropDown_FilterTiltsMode.activated.connect(self.setFilterTiltsModeToJobTap)

        self.textEdit_recVoxelSize.textChanged.connect(self.setRecVoxelSizeToJobTap)

        self.textEdit_recTomosize.textChanged.connect(self.setRecTomosizeToJobTap)

        self.btn_browse_gain.clicked.connect(self.browsePathGain)

        self.btn_browse_autoPrefix.clicked.connect(self.generatePrefix)

        self.btn_use_movie_path.clicked.connect(self.mdocs_use_movie_path)

        self.dropDown_config.activated.connect(self.loadConfig)

        self.dropDown_probThrBehave.activated.connect(self.setProbBehaveToJobTab)

        self.btn_browse_target.clicked.connect(self.browsePathTarget)

        self.btn_genProject.clicked.connect(self.generateProject)

        self.btn_updateWorkFlow.clicked.connect(self.updateWorkflow)    

        self.btn_importData.clicked.connect(self.importData)

        self.btn_startWorkFlow.clicked.connect(self.startWorkflow)

        self.btn_showClusterStatus.clicked.connect(self.showClusterStatus)

        self.checkBox_openRelionGui.stateChanged.connect(self.openRelionGui)

        self.btn_stopWorkFlow.clicked.connect(self.stopWorkflow)

        self.btn_unlockWorkFlow.clicked.connect(self.unlockWorkflow)

        self.btn_resetWorkFlow.clicked.connect(self.resetWorkflow)

        self.btn_resetWorkFlowHead.clicked.connect(self.resetWorkflowHead)

        self.dropDown_nrNodes.activated.connect(self.setNrNodesToJobTap)

        self.dropDown_partitionName.activated.connect(self.setNrNodesToJobTap)

        self.checkBox_shareNodes.stateChanged.connect(self.setNrNodesToJobTap)

        self.dropDown_nrNodes.setCurrentIndex(2)

        self.dropDown_jobSize.setCurrentIndex(1)

        self.dropDown_jobSize.activated.connect(self.setNrNodesFromJobSize)

        for i in self.cbdat.conf.microscope_presets:

            self.dropDown_config.addItem(self.cbdat.conf.microscope_presets[i])

        self.dropDown_config.setCurrentIndex(0)

        self.btn_scheduleJobs.clicked.connect(self.scheduleJobs)

        

    def genSchemeTable(self):

        self.table_scheme.setColumnCount(1) #origianlly 4

        self.labels_scheme = ["Job Name"] #, "Fork", "Output if True", "Boolean Variable"]

        self.table_scheme.setHorizontalHeaderLabels(self.labels_scheme) 

        self.table_scheme.setRowCount(len(self.cbdat.scheme.jobs_in_scheme))    

        for i, job in enumerate(self.cbdat.scheme.jobs_in_scheme):

            self.table_scheme.setItem(i, 0, QTableWidgetItem(str(job))) 

            self.table_scheme.setItem(i, 1, QTableWidgetItem())

            #self.table_scheme.item(i, 1).setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)

            #self.table_scheme.item(i, 1).setCheckState(Qt.CheckState.Unchecked)

            #self.table_scheme.setItem(i, 2, QTableWidgetItem(str("undefined")))

            #self.table_scheme.setItem(i, 3, QTableWidgetItem(str("undefined")))

            

    def makeJobTabsFromScheme(self):

        """

        insert a new tab for every job and place a table with the parameters found in the respective job.star

        file in the ["joboptions_values"] df.

        """

        

        self.tabWidget.setTabVisible(1,True)

        

        if ((self.check_edit_scheme.isChecked()) and (self.cbdat.args.autoGen == False) and (self.cbdat.args.skipSchemeEdit == False)):

            try:

                dialog=EditScheme(self.cbdat.scheme)

                res=dialog.exec()

                newScheme=dialog.getResult()

                if newScheme is not None:

                    self.cbdat.scheme = newScheme

                else:

                    return        

                self.genSchemeTable()

            except Exception as e:

                messageBox(title="Error", text="Error while editing scheme")

                print(e)

                return

        else:

            pass

            # inputNodes,inputNodes_df=get_inputNodesFromSchemeTable(self.table_scheme,jobsOnly=True)

            # if self.cbdat.filtScheme:

            #     self.cbdat.scheme=self.cbdat.scheme.filterSchemeByNodes(inputNodes_df)

       

        self.genParticleSetups()        

       

        insertPosition=self.jobTapNrSetUpTaps

        for job in self.cbdat.scheme.jobs_in_scheme:

           self.schemeJobToTab(job,self.cbdat.conf,insertPosition)

           insertPosition += 1 


        self.groupBox_WorkFlow.setEnabled(True)

        self.groupBox_Setup.setEnabled(True)

        self.groupBox_Project.setEnabled(True)

        

        

        if (self.cbdat.args.mdocs != None):

            self.line_path_mdocs.setText(self.cbdat.args.mdocs)

        if (self.cbdat.args.movies != None):

             self.line_path_movies.setText(self.cbdat.args.movies)

        if (self.cbdat.args.proj != None):

             self.line_path_new_project.setText(self.cbdat.args.proj)

        if (self.cbdat.args.pixS != None):

             self.textEdit_pixelSize.setText(self.cbdat.args.pixS)

        if (self.cbdat.args.gain != None):

            if not os.path.isfile(self.cbdat.args.gain):

                raise Exception("file not found: "+self.cbdat.args.gain)

            if not os.path.isabs(self.cbdat.args.gain):

                self.line_path_gain.setText(os.path.abspath(self.cbdat.args.gain)) 

            else:

                self.line_path_gain.setText(self.cbdat.args.gain)

        if "denoisetrain" in self.cbdat.scheme.jobs_in_scheme.values  or "denoisepredict" in self.cbdat.scheme.jobs_in_scheme.values:

            pass

            #params_dict = {"generate_split_tomograms": "Yes" }

            #self.setParamsDictToJobTap(params_dict)

        self.loadConfig()

        self.btn_makeJobTabs.setEnabled(False)

        

    def genParticleSetups(self):

        

        self.jobTapNrSetUpTaps=1

        #self.tabWidget.removeTab(self.jobTapNrSetUpTaps)

        self.widgets = [] 

        self.layouts = []

        for job in self.cbdat.scheme.jobs_in_scheme:

            if job.startswith("templatematching"):

                

                scroll_area = QScrollArea()

                scroll_area.setWidgetResizable(True)

                scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

                scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)

                

                container_widget = QtWidgets.QWidget()

                scroll_area.setWidget(container_widget)

                layout = QVBoxLayout(container_widget)

                layout.setSpacing(1)

                layout.setContentsMargins(1,1, 1, 1)

                self.layouts.append(layout)

                tag=job.split("_")

                if len(tag)>1:

                    tabName="ParticleSetup_"+tag[1]

                else:

                    tabName="ParticleSetup"

                

                widget=self.iniWidget(job)

                layout.addWidget(widget,stretch=0)

                self.widgets.append(widget)

                scroll_area.setWidget(container_widget)

                self.tabWidget.insertTab(self.jobTapNrSetUpTaps, scroll_area, tabName)

                self.jobTapNrSetUpTaps+=1   


            if job.startswith("tmextractcand"):    

                widget=self.iniWidget(job)

                layout.addWidget(widget,stretch=0)             

            

            if job.startswith("subtomoExtraction"):    

                widget=self.iniWidget(job)

                layout.addWidget(widget,stretch=0)  

        

        for layout in self.layouts:   

            spacer = QtWidgets.QSpacerItem(20, 40, QtWidgets.QSizePolicy.Policy.Minimum, QtWidgets.QSizePolicy.Policy.Expanding)

            layout.addItem(spacer)    

                                    

                

    def iniWidget(self,jobName):             

        

        if len(jobName.split('_'))>1:

            jobName=jobName.split("_")[0]


        srcBasePase=self.cbdat.CRYOBOOST_HOME

        widget = QtWidgets.QWidget()

        if jobName=="templatematching":

            widgetPath=srcBasePase+'/src/gui/widgets/templateMatching.ui'

        if jobName=="tmextractcand":

            widgetPath=srcBasePase+'/src/gui/widgets/candidateExtraction.ui'

        if jobName=="subtomoExtraction":

            widgetPath=srcBasePase+'/src/gui/widgets/particleReconstruction.ui'

        

        widget = loadUi(widgetPath,widget)

        widget.setContentsMargins(0, 0, 0, 0)

        widget=self.setCallbacksPartcileSetup(widget,jobName)

        

        return widget

           

       

        

    def setCallbacksPartcileSetup(self,widget,jobName):

        

        if jobName=="templatematching":

            widget.line_path_tm_template_volume.textChanged.connect(self.setTmVolumeTemplateToJobTap)

            widget.line_path_tm_template_volumeSym.textChanged.connect(self.setTmVolumeTemplateSymToJobTap)

            widget.line_path_tm_template_volumeMask.textChanged.connect(self.setTmVolumeTemplateMaskToJobTap)    

            widget.btn_browse_tm_template_volume.clicked.connect(self.browseTmVolumeTemplate)

            widget.btn_browse_tm_template_volumeMask.clicked.connect(self.browseTmVolumeTemplateMask)

            widget.btn_view_tm_template_volume.clicked.connect(self.viewTmVolumeTemplate)

            widget.btn_view_tm_template_volumeMask.clicked.connect(self.viewTmVolumeTemplateMask)

            widget.btn_generate_tm_template_volume.clicked.connect(self.generateTmVolumeTemplate)

            widget.btn_generate_tm_template_volumeMask.clicked.connect(self.generateTmVolumeTemplateMask)

            widget.chkbox_tm_template_volumeMaskNonSph.stateChanged.connect(self.setTmVolumeTemplateMaskNonSpToJobTap)

            widget.dropDown_tm_SearchVolType.currentTextChanged.connect(self.setTmVolumeTypeToJobTap)

            widget.line_path_tm_SearchVolSplit.textChanged.connect(self.setTmSearchVolSplitToJobTap)            

            widget.line_path_tm_SearchVolMaskFold.textChanged.connect(self.setTmSearchVolMaskFoldToJobTap)

            widget.btn_browse_tm_SearchVolMaskFold.clicked.connect(self.browseSearchVolMaskFold)

            widget.checkBox_tm_CtfWeight.stateChanged.connect(self.setTmCtfWeightToJobTap)

            widget.checkBox_tm_DoseWeight.stateChanged.connect(self.setTmDoseWeightToJobTap)

            widget.checkBox_tm_RandomPhaseCorrection.toggled.connect(self.setTmRandomPhaseCorrectionToJobTap)

            widget.checkBox_tm_SpectralWhitening.toggled.connect(self.setTmSpectralWhiteningToJobTap)

            widget.line_path_tm_BandPass.textChanged.connect(self.setTmBandPassToJobTap)            

            widget.line_path_tm_AngSamp.textChanged.connect(self.setTmAngSampToJobTap)            

            widget.btn_browse_tm_AngList.clicked.connect(self.browseTmAngList)

        

        if jobName=="tmextractcand":

            widget.line_path_ce_diaInAng.textChanged.connect(self.setCeDiaInAngToJobTap)

            widget.dropDown_cutOffType.currentTextChanged.connect(self.setCeScoreCutOffTypeToJobTap)

            widget.line_path_ce_cutOffVal.textChanged.connect(self.setCeScoreCutOffValueToJobTap)

            widget.line_path_ce_maxNumParticles.textChanged.connect(self.setCeMaxNumParticlesToJobTap)

            widget.line_path_ce_maskFold.textChanged.connect(self.setCeMaskFoldPathToJobTap)

            widget.btn_browse_ce_maskFold.clicked.connect(self.browseCeMaskFold)

            widget.dropDown_scoreFiltType.currentTextChanged.connect(self.setCeScoreFiltTypeToJobTap)

            widget.line_path_ce_scoreFiltVal.textChanged.connect(self.setCeScoreFiltValueToJobTap)

            widget.dropDown_ce_implementation.currentTextChanged.connect(self.setCeImplementationToJobTap)

        

        if jobName=="subtomoExtraction":

            widget.line_path_partRecBoxSzCropped.textChanged.connect(self.setPartRecBoxSzCroppedToJobTap)

            widget.line_path_partRecBoxSzUnCropped.textChanged.connect(self.setPartRecBoxSzUnCroppedToJobTap)

            widget.line_path_partRecPixS.textChanged.connect(self.setPartRecPixSToJobTap)


                

        return widget                    

        

    def schemeJobToTab(self,job,conf,insertPosition):

        # arguments: insertTab(index where it's inserted, widget that's inserted, name of tab)

        self.tabWidget.insertTab(insertPosition, QWidget(), job)

        # build a table with the dataframe containinng the parameters for the respective job in the tab

        df_job = self.cbdat.scheme.job_star[job].dict["joboptions_values"]

        nRows, nColumns = df_job.shape

        # create empty table with the dimensions of the df

        self.table = QTableWidget(self)

        self.table.setColumnCount(nColumns)

        self.table.setRowCount(nRows)

        self.table.setHorizontalHeaderLabels(("Parameter", "Value"))

       

        for row in range(nRows):

            for col in range(nColumns):

                # set the value that should be added to the respective col/row combination in the df containing the parameters

                current_value =df_job.iloc[row, col]

                # see whether there is an alias for the parameter (only for the Parameters column)

                if col == 0:

                    alias = conf.get_alias(job, current_value)

                    # if there is an alias, set the widgetItem to this alias, else, do as normal

                    if alias != None:

                        self.table.setItem(row, col, QTableWidgetItem(alias))

                    else:

                        self.table.setItem(row, col, QTableWidgetItem(current_value))

                    self.table.item(row, col).setFlags(self.table.item(row, col).flags() & ~Qt.ItemFlag.ItemIsEditable)

                else:

                    self.table.setItem(row, col, QTableWidgetItem(current_value))

        # set where this table should be placed

        tab_layout = QVBoxLayout(self.tabWidget.widget(insertPosition))

        tab_layout.addWidget(self.table)

        #self.table.setMinimumSize(1500, 400)            

        

    def getTagFromCurrentTab(self):

         

        if len(self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_"))>1:

            jobTag="_"+self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_")[1]

        else:

            jobTag=""

        

        return jobTag

    

    def splitJobByTag(self,jobName):

        

        if len(jobName.split("_"))>1:

            jobBase=jobName.split("_")[0]

            jobTag="_"+jobName.split("_")[1]

        else: 

            jobBase=jobName

            jobTag=""

        

        return jobBase,jobTag

    

        

    def setTmSearchVolMaskFoldToJobTap(self,textLine):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget().findChild(QComboBox, "dropDown_tm_implementation")  # Find QTextEdit named "text1"

        imp = widget.currentText()

        argsFull="--implementation " + imp + " --volumeMaskFold " + textLine + " --gpu_ids auto"

        

        params_dict = {"other_args":argsFull }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

        

        

    def setPathMoviesToJobTap(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        

        import re

        params_dict = {"movie_files": "frames/*" + os.path.splitext(self.line_path_movies.text())[1] }

        self.setParamsDictToJobTap(params_dict)

        folderName = self.line_path_movies.text()

        if (bool(re.search(r'[\*\?\[\]]', folderName))):

            folderName = os.path.dirname(folderName)

        folderName = folderName.rstrip(os.sep) + os.sep

        files = {ext: glob.glob(f"{folderName}*.{ext}") for ext in ['tif', 'tiff', 'eer']}

        max_type = max(files.items(), key=lambda x: len(x[1]))

        selected_files = max_type[1]

        if selected_files:

            self.groupBox_Frames.setTitle("Frames   (" + str(len(selected_files)) + "  " + max_type[0] + " files found in folder)")

            self.groupBox_Frames.setStyleSheet("QGroupBox { color: green; }")

        else:

            self.groupBox_Frames.setTitle("Frames   (0 files found)")

            self.groupBox_Frames.setStyleSheet("QGroupBox { color: red; }")

        ext="*.eer"

        self.updateEERFractions()

    

    def updateEERFractions(self):

        folder=os.path.join(os.path.dirname(self.line_path_movies.text()),'')  # Ensure folder path ends with a slash

        ext="*.eer"

        eerFiles=glob.glob(folder + "/" + ext)

        if not eerFiles:  # checks if list is empty

           self.textEdit_eerFractions.setEnabled(False)

           return  

        try:

            self.textEdit_eerFractions.setEnabled(True)

            print("Nr eer files found: " + str(len(eerFiles)))

            totDosePerTilt=float(self.textEdit_dosePerTilt.toPlainText())

            tragetDosePerFrame=float(self.textEdit_eerFractions.toPlainText())

            nrFramesToGroup=get_EERsections_per_frame(eerFiles[0],dosePerTilt=totDosePerTilt,dosePerRenderedFrame=tragetDosePerFrame)

            if "motioncorr" in self.cbdat.scheme.jobs_in_scheme.values: 

                params_dict = {"eer_grouping": str(nrFramesToGroup) }

                self.setParamsDictToJobTap(params_dict,["motioncorr"]) 

            if "fsMotionAndCtf" in self.cbdat.scheme.jobs_in_scheme.values:

                params_dict = {"param1_value": str(nrFramesToGroup) }

                self.setParamsDictToJobTap(params_dict,["fsMotionAndCtf"]) 

        except Exception as e: 

            pass

            

    

               

    def setTmVolumeTemplateToJobTap(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volume")  # Find QTextEdit named "text1"

        text = text_field.text()  # Get text content

        params_dict = {"in_3dref":text }

        if len(self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_"))>1:

            jobTag="_"+self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_")[1]

        else:

            jobTag=""

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    def setTmVolumeTypeToJobTap(self,textDropDown):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        if textDropDown=="Uncorrected":

            text="rlnTomoReconstructedTomogram"       

        if textDropDown=="Deconv (Warp)":

            text="rlnTomoReconstructedTomogramDeconv"

        if textDropDown=="Filtered":

            text="rlnTomoReconstructedTomogramDenoised"

       

        params_dict = {"param1_value":text }

        

        if len(self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_"))>1:

            jobTag="_"+self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_")[1]

        else:

            jobTag=""

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    def getTagFromParentTab(self, widget):

        """

        Find parent QTabWidget and the tab name containing the widget

        Returns: tuple (QTabWidget, tab_name) or (None, None) if not found

        """

        from PyQt6.QtWidgets import QTabWidget

        jobTag=""

        current = widget

        while current:

            parent = current.parent()

            if isinstance(parent, QTabWidget):

                for i in range(parent.count()):

                    if parent.widget(i).isAncestorOf(widget):

                        tab_name = parent.tabText(i)

                        if len(tab_name.split("_"))>1:

                            jobTag="_"+tab_name.split("_")[1]

                        else:

                            jobTag=""

                        return jobTag

            current = parent

        

        return jobTag

    

    def setTmSearchVolSplitToJobTap(self,textLine):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

       

        # print("callback==="+ tag)

        params_dict = {"param10_value":textLine }

        # jobTag=self.getTagFromCurrentTab()

        widget = self.sender() 

        jobTag = self.getTagFromParentTab(widget)

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    def setTmCtfWeightToJobTap(self,state):

    

        if state==0:

            flag="False"

        else:

            flag="True"

        params_dict = {"param6_value":str(flag) }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    def setTmDoseWeightToJobTap(self,state):

    

        if state==0:

            flag="False"

        else:

            flag="True"

        params_dict = {"param7_value":str(flag) }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    def setTmRandomPhaseCorrectionToJobTap(self,state):

    

        params_dict = {"param9_value":str(state) }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    def setTmBandPassToJobTap(self,text):

        

        params_dict = {"param5_value":str(text) }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    def browseTmAngList(self):

        

        targetFold=os.getcwd()

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_AngSamp") 

        dirName=browse_files(text_field,self.system.filebrowser)

        

        

    def setTmAngSampToJobTap(self,text):

        

        params_dict = {"param3_value":str(text) }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    

        

    

    def setTmSpectralWhiteningToJobTap(self,state):

    

        # if state==0:

        #     flag="False"

        # else:

        #     flag="True"

        params_dict = {"param8_value":str(state) }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    

    

    def browseSearchVolMaskFold(self):

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_SearchVolMaskFold") 

        targetFold=os.getcwd()

        dirName=browse_dirs(text_field,targetFold,self.system.filebrowser)

        

    

    def setTmVolumeTemplateMaskNonSpToJobTap(self):

        

        widget = self.tabWidget.currentWidget()

        chk = widget.findChild(QCheckBox, "chkbox_tm_template_volumeMaskNonSph")  # Find QTextEdit named "text1"

        text = str(chk.isChecked())  # Get text content

        params_dict = {"param4_value":text }

        if len(self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_"))>1:

            jobTag="_"+self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_")[1]

        else:

            jobTag=""

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    

    def setTmVolumeTemplateMaskToJobTap(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volumeMask")  # Find QTextEdit named "text1"

        maskName = text_field.text()  # Get text content

        tag=self.getTagFromCurrentTab()

        tag=tag[1:]

        if os.path.isfile(maskName):

            pixSRec=self.getReconstructionPixelSizeFromJobTab()

            with mrcfile.open(maskName, header_only=True) as mrc:

                pixSMask = mrc.voxel_size.x

                boxsize = mrc.header.nx  # or 

               

            if pixSRec!=str(pixSMask):

                msg = QMessageBox()

                msg.setWindowTitle("Problem!")

                msg.setText("Pixelsize of template/mask and tomograms differ!\n\nDo you want to resize the template")

                msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

                msg.setMinimumWidth(300)

                result = msg.exec()

                if result == QMessageBox.StandardButton.Yes:

                    importedVol=self.adaptVolume(maskName,boxsize,pixSMask,pixSRec,checkInvert=False,tag=tag)

            else:

                importedVol=self.adaptVolume(maskName,boxsize,pixSMask,pixSRec,checkInvert=False,tag=tag)

            absPathToVol=os.path.abspath(importedVol)        

        else:

            absPathToVol=maskName

       

        with QSignalBlocker(text_field):

            text_field.setText(absPathToVol)

        

        params_dict = {"in_mask":maskName }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    def browseTmVolumeTemplateMask(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        targetFold=os.getcwd()

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volumeMask") 

        dirName=browse_files(text_field,self.system.filebrowser)

    

    def viewTmVolumeTemplateMask(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volumeMask") 

        self.viewVolume(text_field.text())    

    

    def viewTmVolumeTemplate(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volume") 

        self.viewVolume(text_field.text())    

    

    def generateTmVolumeTemplate(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget()

       

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volume") 

        tag=self.getTagFromCurrentTab()

        pixS=self.getReconstructionPixelSizeFromJobTab()

        

        if self.line_path_new_project.text()=="":

             messageBox("Info","No Projcet Path. Specify Projcet Path")

             projPath=browse_dirs()   

        else:

            projPath=self.line_path_new_project.text()

        templateFolder=projPath+os.path.sep+"templates"+ os.path.sep + tag[1:]

        os.makedirs(templateFolder, exist_ok=True)

        

        self.template_dialog = TemplateGen()

        self.template_dialog.line_edit_templatePixelSize.setText(pixS)

        self.template_dialog.line_edit_outputFolder.setText(templateFolder)

        self.template_dialog.framePixs=self.textEdit_pixelSize.toPlainText()

        result = self.template_dialog.exec()

        with QSignalBlocker(text_field):

            text_field.setText(self.template_dialog.line_edit_mapFile.text())

        params_dict = {"in_3dref":os.path.abspath(text_field.text()) }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    

    def getReconstructionPixelSizeFromJobTab(self):

        

        widget = self.tabWidget.currentWidget()

        index = self.tabWidget.indexOf(widget)

        scheme=self.updateSchemeFromJobTabs(self.cbdat.scheme,self.tabWidget)

        self.tabWidget.setCurrentIndex(index)

        tag=self.getTagFromCurrentTab()

        

        if "tsReconstruct"+tag in scheme.jobs_in_scheme.values: 

            pixS=scheme.job_star['tsReconstruct'+tag].dict['joboptions_values']['rlnJobOptionValue'][9]

            return pixS

        if "reconstructionfull"+tag in scheme.jobs_in_scheme.values: 

            pixS = scheme.job_star['reconstructionfull'+tag].dict['joboptions_values'][

            scheme.job_star['reconstructionfull'].dict['joboptions_values']['rlnJobOptionVariable'] == 'binned_angpix'

            ]['rlnJobOptionValue'].values[0]

            return pixS    

        if "tsReconstruct" in scheme.jobs_in_scheme.values: 

            pixS=scheme.job_star['tsReconstruct'].dict['joboptions_values']['rlnJobOptionValue'][9]

            return pixS

        if "reconstructionfull" in scheme.jobs_in_scheme.values: 

            pixS = scheme.job_star['reconstructionfull'].dict['joboptions_values'][

            scheme.job_star['reconstructionfull'].dict['joboptions_values']['rlnJobOptionVariable'] == 'binned_angpix'

            ]['rlnJobOptionValue'].values[0]

            return pixS    

        

        if pixS is None:

            messageBox("Problem","No Reconstruction Job. You cannot run template matching")

            pixS=-1

        

        return pixS

        

        

        

    def generateTmVolumeTemplateMask(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volumeMask") 

        text_fieldTempl = widget.findChild(QLineEdit, "line_path_tm_template_volume") 

        inputVol=text_fieldTempl.text().replace("_black.mrc","_white.mrc")

        if not os.path.isfile(inputVol):

            messageBox("Problem","No Template Volume. Generate Template Volume first")

            return

        maskName=os.path.splitext(inputVol.replace("_white.mrc",".mrc"))[0]+"_mask.mrc"

        lowpass=20

        thr=caclThreshold(inputVol,lowpass=None)

        thr=round(thr['fb'],5)

        

        fields = {

        "MaskPath": maskName,

        "Threshold": str(thr),

        "Extend": "3",

        "SoftEdge": "4",

        "LowPass": str(lowpass)

         }

        dialog = MultiInputDialog(fields)

        if dialog.exec() == QDialog.DialogCode.Accepted:

            val = dialog.getInputs()

            msg=statusMessageBox("Generating Mask")

            genMaskRelion(inputVol,

                          val["MaskPath"],

                          val["Threshold"],

                          val["Extend"],

                          val["SoftEdge"],  

                          val["LowPass"],

                          threads=20,

                          envStr=self.cbdat.localEnv,

                           )

            with QSignalBlocker(text_field):

                text_field.setText(val["MaskPath"])

            params_dict = {"in_mask":os.path.abspath(maskName) }

            jobTag=self.getTagFromCurrentTab()

            self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

            msg.close()

    def setTmVolumeTemplateSymToJobTap(self):

        

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volumeSym")  # Find QTextEdit named "text1"

        text = text_field.text()  # Get text content

        params_dict = {"param2_value":text }

        if len(self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_"))>1:

            jobTag="_"+self.tabWidget.tabText(self.tabWidget.currentIndex()).split("_")[1]

        else:

            jobTag=""

        print(jobTag)

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

        

    def viewVolume(self,volume):

        os.system(self.cbdat.localEnv + " imod " + volume)

        os.system(self.cbdat.localEnv + " chimera " + volume) 

       

    def setTmVolumeTemplateToJobTap(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volume")  # Find QTextEdit named "text1"

        tmVolName = text_field.text()

        tag=self.getTagFromCurrentTab()

        tag=tag[1:]

        if os.path.isfile(tmVolName):

            pixSRec=self.getReconstructionPixelSizeFromJobTab()

            with mrcfile.open(tmVolName, header_only=True) as mrc:

                pixSTemplate = mrc.voxel_size.x

                boxsize = mrc.header.nx  # or 

               

            if pixSRec!=str(pixSTemplate):

                msg = QMessageBox()

                msg.setWindowTitle("Problem!")

                msg.setText("Pixelsize of template/mask and tomograms differ!\n\nDo you want to resize the template")

                msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

                msg.setMinimumWidth(300)

                result = msg.exec()

                if result == QMessageBox.StandardButton.Yes:

                    importedVol=self.adaptVolume(tmVolName,boxsize,pixSTemplate,pixSRec,tag=tag)

            else:

                importedVol=self.adaptVolume(tmVolName,boxsize,pixSTemplate,pixSRec,tag=tag)

            absPathToVol=os.path.abspath(importedVol)        

        else:

            absPathToVol=tmVolName

       

        with QSignalBlocker(text_field):

            text_field.setText(absPathToVol)

            

        params_dict = {"in_3dref":absPathToVol }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["templatematching"+jobTag])

    

    def adaptVolume(self,inputVolName,boxsize,pixSTemplate,pixSRec,checkInvert=True,tag=""):

        templateFold="templates/"+ tag 

        tmBase= templateFold + "/template_box"

        if self.line_path_new_project.text()=="":

            msg=messageBox("Info","No Projcet Path. Specify Projcet Path")

            projPath=browse_dirs()   

        else:

            projPath=self.line_path_new_project.text()

        if checkInvert:

            msg = QMessageBox()

            msg.setWindowTitle("Decision")

            msg.setText("Mass needs to black!\n\nDo you want to invert the template")

            msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

            result = msg.exec()

            if result == QMessageBox.StandardButton.Yes:

                invert=True

            else:

                invert=False

        else:

            invert=False        

        

        calcBox=boxsize*(float(pixSTemplate)/float(pixSRec))

        offset=32

        newBox = ((calcBox + offset - 1) // offset)*offset

        if newBox<96:

            newBox=96

        os.makedirs(os.path.join(projPath,"templates"),exist_ok=True)

        

        if checkInvert:

            resizedVolNameB=os.path.join(projPath,tmBase +str(newBox)+"_apix"+str(pixSRec) + "_black.mrc")

        else:

            resizedVolNameB=os.path.join(projPath,tmBase +str(newBox)+"_apix"+str(pixSRec) + "_mask.mrc")

        

        os.makedirs(os.path.dirname(resizedVolNameB),exist_ok=True)

        envStr=self.cbdat.localEnv

        processVolume(inputVolName,resizedVolNameB, voxel_size_angstrom=pixSTemplate,

                voxel_size_angstrom_out_header=pixSRec,voxel_size_angstrom_output=pixSRec,

                box_size_output=newBox,invert_contrast=invert,envStr=envStr)

    

        if checkInvert:

            resizedVolNameW=os.path.join(projPath,tmBase +str(newBox)+"_apix"+str(pixSRec) + "_white.mrc")

            invI=invert==False

            processVolume(inputVolName,resizedVolNameW, voxel_size_angstrom=pixSTemplate,

                        voxel_size_angstrom_out_header=pixSRec,voxel_size_angstrom_output=pixSRec,

                        box_size_output=newBox,invert_contrast=invI,envStr=envStr)

        

        

        return resizedVolNameB

        

    

    def browseTmVolumeTemplate(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        targetFold=os.getcwd()

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_tm_template_volume") 

        dirName=browse_files(text_field,self.system.filebrowser)

        

    def setCeScoreCutOffTypeToJobTap(self,text):   

        params_dict = {"param1_value":text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

   

    def setCeScoreCutOffValueToJobTap(self,text):   

        params_dict = {"param2_value":text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

   

    def setCeDiaInAngToJobTap(self,text):

        params_dict = {"param3_value":text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

    

    def setCeMaxNumParticlesToJobTap(self,text):

        params_dict = {"param4_value":text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

    

    def setCeScoreFiltTypeToJobTap(self,text):   

        params_dict = {"param6_value":text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

    

    def setCeScoreFiltValueToJobTap(self,text):   

        params_dict = {"param7_value":text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

   

    def setCeMaskFoldPathToJobTap(self,text):

        params_dict = {"param8_value":text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

    

    def setCeImplementationToJobTap(self,text):

        

        textToSet="--implementation " + text + "\'"

        params_dict = {"other_args": textToSet }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["tmextractcand"+jobTag])

    

    def setPartRecBoxSzCroppedToJobTap(self,text):

        

        params_dict = {"crop_size": text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["subtomoExtraction"+jobTag])

    def setPartRecBoxSzUnCroppedToJobTap(self,text):

        

        params_dict = {"box_size": text }

        jobTag=self.getTagFromCurrentTab()

        self.setParamsDictToJobTap(params_dict,["subtomoExtraction"+jobTag])

    

    def setPartRecPixSToJobTap(self,text,jobTag=None):

        

        if jobTag is None:

            jobTag=self.getTagFromCurrentTab()

        pixS=self.textEdit_pixelSize.toPlainText()

        if pixS.replace(".", "", 1).isdigit() and text.replace(".", "", 1).isdigit():

            binF=float(text)/float(pixS)

            params_dict = {"binning": str(binF) }

            self.setParamsDictToJobTap(params_dict,["subtomoExtraction"+jobTag])

    

    def browseCeMaskFold(self):

        """

        Sets the path to movies in the importmovies job to the link provided in the line_path_movies field.

        Then, sets the parameters dictionary to the jobs in the tab widget.


        Args:

            None


        Returns:

            None

        """

        targetFold=os.getcwd()

        widget = self.tabWidget.currentWidget()

        text_field = widget.findChild(QLineEdit, "line_path_ce_maskFold") 

        dirName=browse_dirs(text_field,targetFold,self.system.filebrowser)

    

        

    def setPathMdocsToJobTap(self):

        """

        Sets the parameters dictionary to the jobs in the tab widget for the mdoc files.


        This function retrieves the file extension from the `line_path_mdocs` text field and constructs the `mdoc_files` parameter dictionary. The constructed dictionary is then passed to the `setParamsDictToJobTap` method.


        Parameters:

            None


        Returns:

            None

        """

        

        params_dict = {"mdoc_files": "mdoc/*" + os.path.splitext(self.line_path_mdocs.text())[1] }

        

        import re

        

        

        

        if "ctffind" in self.cbdat.scheme.jobs_in_scheme.values:

            thoneRingFade = self.cbdat.scheme.getJobOptions("ctffind").loc[

                             self.cbdat.scheme.getJobOptions("ctffind")["rlnJobOptionVariable"] == "exp_factor_dose",

                             "rlnJobOptionValue"

                             ].values[0]  

            if self.textEdit_dosePerTilt.toPlainText().isnumeric():

                checkDosePerTilt(self.line_path_mdocs.text(),float(self.textEdit_dosePerTilt.toPlainText()),float(thoneRingFade))

        

        self.setParamsDictToJobTap(params_dict)

        try:

            mdoc=mdocMeta(self.line_path_mdocs.text())

            nrMdoc=mdoc.param4Processing["NumMdoc"]

            if nrMdoc>0:

                self.groupBox_mdoc.setTitle("Mdoc   (" + str(nrMdoc) + " mdoc files found in folder)")

                self.groupBox_mdoc.setStyleSheet("QGroupBox { color: green; }")

            else:

                self.groupBox_mdoc.setTitle("Mdoc   (0 files found)")

                self.groupBox_mdoc.setStyleSheet("QGroupBox { color: red; }")

            

            self.textEdit_pixelSize.setText(str(mdoc.param4Processing["PixelSize"]))

            dosePerTilt=mdoc.param4Processing["DosePerTilt"]

            if dosePerTilt<0.1 or dosePerTilt > 9:

                print("dose per tilt from mdoc out of range setting to 3")

                dosePerTilt=3.0

            self.textEdit_dosePerTilt.setText(str(dosePerTilt))

            self.textEdit_nomTiltAxis.setText(str(mdoc.param4Processing["TiltAxisAngle"]))

            self.textEdit_recTomosize.setText(str(mdoc.param4Processing["ImageSize"])+str("x2048"))

            # if int(mdoc.param4Processing["ImageSize"].split("x")[0])>4096:

            #     print("detected large camera chip size increasing default patch size by 10 percent")

            #     self.textEdit_ImodPatchSize.setText=(str(800))

            line_edits = self.tabWidget.findChildren(QLineEdit, "line_path_partRecPixS")

            for line_edit in line_edits:

                current_value = line_edit.text()

                line_edit.setText(str(mdoc.param4Processing["PixelSize"]))

        except: 

            self.groupBox_mdoc.setTitle("Mdoc   (0 files found)")

            self.groupBox_mdoc.setStyleSheet("QGroupBox { color: red; }")

        

        

    #self.textEdit_tomoForDenoiseTrain.textChanged.connect(self.setTomoForDenoiseTrainToJobTap)

    #self.textEdit_pathDenoiseModel.textChanged.connect(self.setPathDenoiseModelToJobTap)

    

    def showClusterStatus(self):

        

        sshStr=self.cbdat.conf.confdata['submission'][0]['SshCommand']

        headNode=self.cbdat.conf.confdata['submission'][0]['HeadNode']

        command=sshStr + " " + headNode + ' "' + "sinfo -o '%P %.6D %.6t' | sort -k3 | grep -v 'PAR'" + '"'

        print(command)

        proc=subprocess.Popen(command,shell=True ,stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        stdout, stderr = proc.communicate()

        

        msg_box = QMessageBox()

        msg_box.setIcon(QMessageBox.Icon.Information)

        msg_box.setWindowTitle('Information')

        msg_box.setText(stdout)

        msg_box.setStandardButtons(QMessageBox.StandardButton.Ok)

        msg_box.resize(2000, 400) 

        msg_box.exec()

       

    def setTomoForDenoiseTrainToJobTap(self):

  

        params_dict = {"tomograms_for_training": self.textEdit_tomoForDenoiseTrain.toPlainText() }

        self.setParamsDictToJobTap(params_dict)

    

    def setPathDenoiseModelToJobTap(self):

  

        params_dict = {"care_denoising_model": self.textEdit_pathDenoiseModel.toPlainText() }

        self.setParamsDictToJobTap(params_dict)

    

    def setmodelForFilterTiltsToJobTap(self):

        

        params_dict = {"param1_value": self.textEdit_modelForFilterTilts.toPlainText() }

        self.setParamsDictToJobTap(params_dict,applyToJobs="filtertilts")

    

    def setProbThrToJobTap(self):

        

        params_dict = {"param5_value": self.textEdit_probThr.toPlainText() }

        self.setParamsDictToJobTap(params_dict,applyToJobs="filtertilts")

    

    def setFilterTiltsModeToJobTap(self,index):

        params_dict = {"param1_value":  self.dropDown_FilterTiltsMode.currentText()}

        self.setParamsDictToJobTap(params_dict,applyToJobs="filtertiltsInter")


    

    def setProbBehaveToJobTab(self):

        

        params_dict = {"param6_value": self.dropDown_probThrBehave.currentText() }

        self.setParamsDictToJobTap(params_dict,applyToJobs="filtertilts")

    

    

    def updateTomogramsForTraining(self):

        wk_mdocs=self.line_path_mdocs.text()

        mdocList=glob.glob(wk_mdocs)

        pref=self.line_path_crImportPrefix.text()

        mdocList=[mdoc for mdoc in mdocList]# if pref in mdoc]

        #tomoNames=[pref+os.path.splitext(os.path.basename(path))[0].replace(".st","") for path in mdocList]

        tomoNames=[pref+os.path.splitext(os.path.splitext(os.path.basename(path))[0])[0] for path in mdocList]

        if len(tomoNames)<3:

            nTomo=len(tomoNames)

        else:

            nTomo=3

        tomoNamesSub=random.sample(tomoNames, k=nTomo)

        tomoStr=":".join(tomoNamesSub)

        self.textEdit_tomoForDenoiseTrain.setText(tomoStr)

       

    def setPixelSizeToJobTap(self):

        textline=self.textEdit_pixelSize.toPlainText()

        params_dict = {"angpix": textline}

        self.setParamsDictToJobTap(params_dict,["importmovies"])      

        if textline.replace('.', '', 1).isdigit():  # Allows one decimal point

            bin4Pixs=str(float(textline)*4)

            self.textEdit_algRescaleTilts.setText(bin4Pixs)

            self.textEdit_recVoxelSize.setText(bin4Pixs)

        

    def setdosePerTiltToJobTap(self):

        params_dict = {"dose_rate": self.textEdit_dosePerTilt.toPlainText()} 

        if "ctffind" in self.cbdat.scheme.jobs_in_scheme.values:

            thoneRingFade = self.cbdat.scheme.getJobOptions("ctffind").loc[

                             self.cbdat.scheme.getJobOptions("ctffind")["rlnJobOptionVariable"] == "exp_factor_dose",

                             "rlnJobOptionValue"

                             ].values[0]  

            if self.textEdit_dosePerTilt.toPlainText().isnumeric():

                checkDosePerTilt(self.line_path_mdocs.text(),float(self.textEdit_dosePerTilt.toPlainText()),float(thoneRingFade))

        

        self.setParamsDictToJobTap(params_dict,["importmovies"])       

        self.updateEERFractions()

        

    def setTiltAxisToJobTap(self):

        params_dict = {"tilt_axis_angle": self.textEdit_nomTiltAxis.toPlainText()} 

        self.setParamsDictToJobTap(params_dict,["importmovies"]) 

    

    def setPathGainToJobTap(self):

        

        params_dict = {"fn_gain_ref": self.line_path_gain.text()} 

        self.setParamsDictToJobTap(params_dict,["motioncorr"]) 

        params_dict = {"param2_value": self.line_path_gain.text()} 

        self.setParamsDictToJobTap(params_dict,["fsMotionAndCtf"]) 

        

    

    def setGainRotToJobTap(self):

        if "motioncorr" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"gain_rot": self.dropDown_gainRot.currentText()} 

            checkGainOptions(self.line_path_gain.text(),self.dropDown_gainRot.currentText(),self.dropDown_gainFlip.currentText())

            self.setParamsDictToJobTap(params_dict,["motioncorr"]) 

        if "fsMotionAndCtf" in self.cbdat.scheme.jobs_in_scheme.values:   

            selFlip=self.dropDown_gainFlip.currentText()

            selRot=self.dropDown_gainRot.currentText()

            gainOpString=""

            if selFlip=="Flip upside down (1)":

                gainOpString=gainOpString+"flip_y"

            if selFlip=="Flip left to right (2)":

                if gainOpString!="":

                    gainOpString=gainOpString+":"

                gainOpString=gainOpString+"flip_x"

            if selRot=="Transpose":

                if gainOpString!="":

                    gainOpString=gainOpString+":"

                gainOpString=gainOpString+"transpose"

            

            #checkGainOptions(self.line_path_gain.text(),self.dropDown_gainRot.currentText(),self.dropDown_gainFlip.currentText())

            params_dict = {"param3_value": gainOpString}

            print(params_dict)

            self.setParamsDictToJobTap(params_dict,["fsMotionAndCtf"]) 

        

        

    def setGainFlipJobTap(self):

        if "motioncorr" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"gain_flip": self.dropDown_gainFlip.currentText()} 

            checkGainOptions(self.line_path_gain.text(),self.dropDown_gainRot.currentText(),self.dropDown_gainFlip.currentText())

            self.setParamsDictToJobTap(params_dict,["motioncorr"]) 

        if "fsMotionAndCtf" in self.cbdat.scheme.jobs_in_scheme.values:   

            

            selFlip=self.dropDown_gainFlip.currentText()

            selRot=self.dropDown_gainRot.currentText()

            gainOpString=""

            if selFlip=="Flip upside down (1)":

                gainOpString=gainOpString+"flip_y"

            if selFlip=="Flip left to right (2)":

                if gainOpString!="":

                    gainOpString=gainOpString+":"

                gainOpString=gainOpString+"flip_x"

            if selRot=="Transpose":

                if gainOpString!="":

                    gainOpString=gainOpString+":"

                gainOpString=gainOpString+"transpose"

            

            #checkGainOptions(self.line_path_gain.text(),self.dropDown_gainRot.currentText(),self.dropDown_gainFlip.currentText())

            params_dict = {"param3_value": gainOpString}

            print(params_dict)

            self.setParamsDictToJobTap(params_dict,["fsMotionAndCtf"]) 

            

    def setInvertTiltAngleToJobTap(self):

        params_dict = {"flip_tiltseries_hand": self.textEdit_invertHand.toPlainText()} 

        self.setParamsDictToJobTap(params_dict,["importmovies"]) 

    def setInvertDefocusHandToJobTap(self):

        params_dict = {"flip_tiltseries_hand": self.textEdit_invertDefocusHand.toPlainText()} 

        self.setParamsDictToJobTap(params_dict,["importmovies"]) 

        print("setting Warp Handness same as Relion")

        if self.textEdit_invertDefocusHand.toPlainText()=="Yes":

            print("  Warp Handness set_flip")

            params_dict = {"param4_value": "set_flip"} 

        else:

            print("  Warp Handness set_noflip")

            params_dict = {"param4_value": "set_noflip"} 

        self.setParamsDictToJobTap(params_dict,["tsCtf"]) 

        

        

        

    def setRecVoxelSizeToJobTap(self):

        if "reconstructionsplit" in self.cbdat.scheme.jobs_in_scheme.values or "reconstructionfull" in self.cbdat.scheme.jobs_in_scheme.values: 

            params_dict = {"binned_angpix": self.textEdit_recVoxelSize.toPlainText()} 

            self.setParamsDictToJobTap(params_dict,["reconstructionsplit"])

            self.setParamsDictToJobTap(params_dict,["reconstructionfull"])

        if "tsReconstruct" in self.cbdat.scheme.jobs_in_scheme.values: 

            params_dict = {"param1_value": self.textEdit_recVoxelSize.toPlainText()} 

            self.setParamsDictToJobTap(params_dict,["tsReconstruct"])

    def setRecTomosizeToJobTap(self):

        if "reconstructionsplit" in self.cbdat.scheme.jobs_in_scheme.values or "reconstructionfull" in self.cbdat.scheme.jobs_in_scheme.values: 

            params_dict = {}

            dims = self.textEdit_recTomosize.toPlainText().split("x")

            params_dict["xdim"] = dims[0]

            params_dict["ydim"] = dims[1]

            params_dict["zdim"] = dims[2]

            self.setParamsDictToJobTap(params_dict,["reconstructionsplit"])

            self.setParamsDictToJobTap(params_dict,["reconstructionfull"])

        if "tsReconstruct" in self.cbdat.scheme.jobs_in_scheme.values: 

            tomoSz=self.textEdit_recTomosize.toPlainText().split("x")

            tomoSz=str(tomoSz[1])+"x"+str(tomoSz[0])+"x"+str(tomoSz[2])

            params_dict = {"param1_value": tomoSz} 

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"])


        

    def setEerFractionsToJobTap(self):

        

        self.updateEERFractions()

        # if "motioncorr" in self.cbdat.scheme.jobs_in_scheme.values: 

        #     params_dict = {"eer_grouping": self.textEdit_eerFractions.toPlainText()}

        #     self.setParamsDictToJobTap(params_dict,["motioncorr"]) 

        # if "fsMotionAndCtf" in self.cbdat.scheme.jobs_in_scheme.values:

        #     params_dict = {"param1_value": self.textEdit_eerFractions.toPlainText()}

        #     self.setParamsDictToJobTap(params_dict,["fsMotionAndCtf"]) 

            

    def setAreTomoSampleThickToJobTap(self):

        

        params_dict = {"tomogram_thickness": self.textEdit_areTomoSampleThick.toPlainText()} 

        self.setParamsDictToJobTap(params_dict,["aligntilts"]) 

        

        if "aligntiltsWarp" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"param6_value": self.textEdit_areTomoSampleThick.toPlainText()}

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"]) 

    #self.textEdit_areTomoPatch.textChanged.connect(self.setAreTomoPatchToJobTap)

    #self.textEdit_algRescaleTilts.textChanged.connect(self.setAlgRescaleTiltsJobTap)

    def setAreTomoPatchToJobTap(self):

        

        if "aligntiltsWarp" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"other_args": self.textEdit_areTomoPatch.toPlainText()}

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"]) 

        

    def setAlgRescaleTiltsJobTap(self):

        

        if "aligntiltsWarp" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"param8_value": self.textEdit_algRescaleTilts.toPlainText()}

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"]) 

        

            

    # def setAreTomoSampleThickToJobTap(self):

    #     params_dict = {"aretomo_thickness": self.textEdit_areTomoSampleThick.toPlainText()} 

    #     self.setParamsDictToJobTap(params_dict,["aligntilts"]) 

    

    def setTiltAxisRefineParamToJobTap(self):

        if "aligntiltsWarp" in self.cbdat.scheme.jobs_in_scheme.values:

            if self.dropDown_doRefineTiltAxis.currentText() == "False":

                refineStr = "0:0"

            else:

                refineStr = self.textEdit_refineTiltAxisIter.toPlainText()+":"+self.textEdit_refineTiltAxisNrTomo.toPlainText()

            params_dict = {"param9_value": refineStr}

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"]) 

    

    def setImodPatchSizeToJobTap(self):

        params_dict = {"patch_size": self.textEdit_ImodPatchSize.toPlainText()} 

        self.setParamsDictToJobTap(params_dict,["aligntilts"]) 

        if "aligntiltsWarp" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"param7_value": self.textEdit_ImodPatchSize.toPlainText()+

                           ":"+self.textEdit_imodPatchOverlap.toPlainText()

                           }

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"]) 

    

    def setImodPatchOverlapToJobTap(self):

        params_dict = {"patch_overlap": self.textEdit_imodPatchOverlap.toPlainText()} 

        self.setParamsDictToJobTap(params_dict,["aligntilts"]) 

        if "aligntiltsWarp" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"param7_value": self.textEdit_ImodPatchSize.toPlainText()+

                           ":"+self.textEdit_imodPatchOverlap.toPlainText()

                           }

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"]) 

        

        

    def setNrNodesFromJobSize(self):

       

       if self.dropDown_jobSize.currentText().strip()=="small":

            self.dropDown_nrNodes.setCurrentText("1")

       if self.dropDown_jobSize.currentText().strip()=="medium":

            self.dropDown_nrNodes.setCurrentText("3") 

       if self.dropDown_jobSize.currentText().strip()=="large":

            self.dropDown_nrNodes.setCurrentText("5") 

       

       self.setNrNodesToJobTap() 

            

    def setTomoAlignProgramToJobTap(self):

        

        programSelected=self.dropDown_tomoAlignProgram.currentText()

        if (programSelected=="Imod"):

            params_dictAre = {"do_aretomo2": "No"}

            params_dictImod = {"do_imod_patchtrack": "Yes"}

        

        if (programSelected=="Aretomo"):

            params_dictAre = {"do_aretomo2": "Yes"}

            params_dictImod = {"do_imod_patchtrack": "No"}

        

        self.setParamsDictToJobTap(params_dictAre,["aligntilts"])

        self.setParamsDictToJobTap(params_dictImod,["aligntilts"])

        if "aligntiltsWarp" in self.cbdat.scheme.jobs_in_scheme.values:

            params_dict = {"param5_value": programSelected}

            self.setParamsDictToJobTap(params_dict,["aligntiltsWarp"]) 

        

        

    def setNrNodesToJobTap(self):

       

        nrNodes=int(self.dropDown_nrNodes.currentText())

        partion=self.dropDown_partitionName.currentText()

        shareNodes=self.checkBox_shareNodes.isChecked()

        for job in self.cbdat.scheme.jobs_in_scheme:

            jobNoTag,_=self.splitJobByTag(job) 

            comDict=self.cbdat.conf.getJobComputingParams([jobNoTag,nrNodes,partion],shareNodes)

            if (comDict is not None):

                self.setParamsDictToJobTap(comDict,applyToJobs=job)

        vRam=int(self.cbdat.conf.confdata['computing'][partion]['VRAM'].replace("G",""))

        if vRam>40:

            spString="2:2:1"

        else:

            spString="4:4:2"

        line_edits = self.tabWidget.findChildren(QLineEdit, "line_path_tm_SearchVolSplit")

        for line_edit in line_edits:

            line_edit.setText(spString)

                


         

    def setParamsDictToJobTap(self,params_dict,applyToJobs="all"):

        """

        A function that sets the parameters dictionary to the jobs in the tab widget based on the given parameters.


        Args:

            params_dict (dict): A dictionary containing the parameters to be set.

            applyToJobs (str, optional): A List specifying which jobs to apply the p

            arameters to. Defaults to "all".

               

        Returns:

            None

        """

        if applyToJobs == "all":

           applyToJobs = list(self.cbdat.scheme.jobs_in_scheme)

        if isinstance(applyToJobs, str):

            applyToJobs = [applyToJobs]

         

        idxOrg=self.tabWidget.currentIndex()

        

        for current_tab in self.cbdat.scheme.jobs_in_scheme:

            #print(current_tab in applyToJobs)

            if current_tab in applyToJobs:

                index_import = self.cbdat.scheme.jobs_in_scheme[self.cbdat.scheme.jobs_in_scheme == current_tab].index

                self.tabWidget.setCurrentIndex(index_import.item()+self.jobTapNrSetUpTaps-1)

                table_widget = self.tabWidget.currentWidget().findChild(QTableWidget)

                change_values(table_widget, params_dict, self.cbdat.scheme.jobs_in_scheme,self.cbdat.conf)

        self.tabWidget.setCurrentIndex(idxOrg)

    

    

    def browsePathMovies(self):

        

        #browse_files(self.line_path_movies)

        targetFold=os.getcwd()

        dirName=browse_dirs(self.line_path_movies,targetFold,self.system.filebrowser)

        if glob.glob(dirName+"*.tif"):

            self.line_path_movies.setText(dirName + "*.tif")

        if glob.glob(dirName+"*.tiff"):

            self.line_path_movies.setText(dirName + "*.tiff")

        if glob.glob(dirName+"*.eer"):

            self.line_path_movies.setText(dirName + "*.eer")  

        

    def browsePathMdocs(self):

        targetFold=os.getcwd()

        dirName=browse_dirs(None,targetFold,self.system.filebrowser)

        if glob.glob(dirName+"*.mdoc"):

            self.line_path_mdocs.setText(dirName + "*.mdoc")


    def browsePathGain(self):

        browse_files(self.line_path_gain,self.system.filebrowser)

    

    def browseDenoisingModel(self):

        browse_files(self.textEdit_pathDenoiseModel,self.system.filebrowser)


    def generatePrefix(self):

       current_datetime = datetime.datetime.now()

       prefix=current_datetime.strftime("%Y-%m-%d-%H-%M-%S_")

       self.line_path_crImportPrefix.setText(prefix)

        

    def mdocs_use_movie_path(self):

        movieP=self.line_path_movies.text()

        mpRoot,ext= os.path.splitext(movieP)

        self.line_path_mdocs.setText(mpRoot+".mdoc")


    def startWorkflow(self):

        

        if self.checkPipeRunner()==False:

            return

        

        if self.cbdat.pipeRunner.checkForLock():

            messageBox("lock exists","stop workflow first")

            return

        print(self.cbdat.pipeRunner.getCurrentNodeScheme())

        

        if self.cbdat.pipeRunner.getCurrentNodeScheme()=="EXIT":

            reply = QMessageBox.question(self, 'Workflow has finished!',

                                 "WorkFlow has finished. To run it again on new data you need to reset the workflow head. Do you want to reset the workflow head?",

                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)

       

            if reply == QMessageBox.StandardButton.Yes:

                self.cbdat.pipeRunner.setCurrentNodeScheme("WAIT")

            else:

                return    

        

        self.cbdat.pipeRunner.runScheme()

    

    def scheduleJobs(self):

        

        if self.checkPipeRunner()==False:

            return

        

        self.cbdat.pipeRunner.scheduleJobs()

        

    def openRelionGui(self):

        

        if self.checkPipeRunner()==False:

            self.checkBox_openRelionGui.setChecked(False)

            return

        

        if self.checkBox_openRelionGui.isChecked():

            self.cbdat.pipeRunner.openRelionGui()    

         

    

    def stopWorkflow(self):

        

        if self.checkPipeRunner()==False:

            return

        reply = QMessageBox.question(self, 'Message',

                                 "Do you really want to stop the workflow?",

                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)

        if reply == QMessageBox.StandardButton.Yes:

            self.cbdat.pipeRunner.abortScheme()

  

       

    def resetWorkflow(self):

       

       if self.checkPipeRunner()==False:

            return

       

       if (self.cbdat.pipeRunner.checkForLock()):

            messageBox("lock exists","stop workflow first")

            return

       

       reply = QMessageBox.question(self, 'Message',

                                 "Do you really want to reset the workflow?",

                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)

       if reply == QMessageBox.StandardButton.Yes:

            self.cbdat.pipeRunner.resetScheme()          

         

    def resetWorkflowHead(self):

       

       if self.checkPipeRunner()==False:

            return

       

       if (self.cbdat.pipeRunner.checkForLock()):

            messageBox("lock exists","stop workflow first")

            return

       

       reply = QMessageBox.question(self, 'Reset',

                                 "Do you really want to reset the workflow head?",

                                 QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)

       

       if reply == QMessageBox.StandardButton.Yes:

            self.cbdat.pipeRunner.setCurrentNodeScheme("WAIT")      

    

    

    def unlockWorkflow(self):

       

       if self.checkPipeRunner()==False:

            return

       self.cbdat.pipeRunner.unlockScheme()          

    

    def checkPipeRunner(self,warnProjectExists=False):

        

        if self.cbdat.pipeRunner is not None: 

            if (warnProjectExists):

                messageBox("Project!","You have already a Project")

            return True    

        else:

            if (warnProjectExists==False):

                messageBox("No Project!","Generate a Project first")

            return False 

    

    def openExtLogViewerWorkFlow(self):

        logMid=self.cbdat.scheme.scheme_star.dict["scheme_general"]["rlnSchemeName"].replace("Schemes/","").replace("/","")

        logfile_path=self.line_path_new_project.text()+os.path.sep + logMid +".log"

        self.viewer = externalTextViewer(logfile_path)

        self.viewer.show()

        

    def openExtLogViewerJobOutput(self):

        

        if (self.cbdat.pipeRunner is  None):

            return

        

        logOut,logError=self.cbdat.pipeRunner.getLastJobLogs()

        logfile_path=logOut

        self.viewer = externalTextViewer(logfile_path)

        self.viewer.show()

    

    def openExtLogViewerJobError(self):

        

        if (self.cbdat.pipeRunner is  None):

            return

        

        logOut,logError=self.cbdat.pipeRunner.getLastJobLogs()

        logfile_path=logError

        self.viewer = externalTextViewer(logfile_path)

        self.viewer.show()

    

    

    def view_log_file(self, log_file_path):

        """

        This function reads the content of a log file and displays it in a text browser.


        Parameters:

        log_file_path (str): The path to the log file.


        Returns:

        None. The function updates the text in the text browser.

        """

        if (self.cbdat.pipeRunner is None):

            return

        

        try:

            with open(log_file_path, 'r') as log_file:

                log_content = log_file.read()

                self.textBrowser_workFlow.setText(log_content)

        except Exception as e:

            self.textBrowser_workFlow.setText(f"Failed to read log file: {e}")

        self.textBrowser_workFlow.moveCursor(QTextCursor.MoveOperation.End)    

        

        logOut,logError=self.cbdat.pipeRunner.getLastJobLogs()

       

        

        try:

            with open(logOut, 'rb') as log_file:  # Open in binary mode

                binary_lines = log_file.readlines()

                log_contentOut = []

                

                for i, binary_line in enumerate(binary_lines):

                    line = binary_line.decode('utf-8')

                    starts_with_cr = binary_line.startswith(b'\r')

                    if starts_with_cr:

                        line = line.split('\r')[-1]

                    cleaned_line = self.process_backspaces(line).strip()

                    if cleaned_line:

                        log_contentOut.append(cleaned_line)


                if len(log_contentOut) > 200:

                    log_contentOut = log_contentOut[-200:]

                

                log_contentOutStr = "\n".join(log_contentOut)

                self.textBrowserJobsOut.setText(log_contentOutStr)

            

            log_contentError= []

            with open(logError, 'r') as log_fileError:

                # log_contentError = log_fileError.readlines()

                for lineError in log_fileError:

                   cleaned_lineError = self.process_backspaces(lineError).strip()

                   if cleaned_lineError:

                        log_contentError.append(cleaned_lineError)

                if len(log_contentError) > 200:

                     log_contentError=log_contentError[-200:]

                

                if self.checkBox_jobErrroShowWarning.isChecked()==False:

                    log_contentError = [line for line in log_contentError if "warning" not in line.lower() and "warn" not in line.lower()]

                    

                log_contentErrorStr= "\n".join(log_contentError)

                self.textBrowserJobsError.setText(log_contentErrorStr)    

        except Exception as e:

            #print(e)

            self.textBrowserJobsOut.setText(f"Logfile not available your job is probably waiting\nCheck queue") #{e})

        self.textBrowserJobsOut.moveCursor(QTextCursor.MoveOperation.End) 

        self.textBrowserJobsError.moveCursor(QTextCursor.MoveOperation.End)

    

    

    def process_backspaces(self,line):

        

        result = []

        for char in line:

            if char == '\b':  # '\b' is the backspace character in Python

                if result:

                    result.pop()  # Remove the last character in the result

            else:

                result.append(char)

        return ''.join(result)

    

    

    def loadConfig(self):

        """

        go through all parameters of all tabs and see whether any parameter is in the config_microscopes file

        (= contains all parameters that are solely dependent on the setup) under the chosen setup. If a parameter

        is found, its value is set to the value defined in the config_microscopes for this parameter.

        """

        microscope = self.dropDown_config.currentText()

        microscope_parameters=self.cbdat.conf.get_microscopePreSet(microscope)

       

        self.textEdit_invertTiltAngle.setText(microscope_parameters["invert_tiltAngles"])

        self.textEdit_invertDefocusHand.setText(microscope_parameters["invert_defocusHandness"])

        


    def browsePathTarget(self):

        defPath=os.getcwd()   

        browse_dirs(self.line_path_new_project,defPath,self.system.filebrowser)

       


    def updateLogViewer(self):

        print("logViewer updated")

        logMid=self.cbdat.scheme.scheme_star.dict["scheme_general"]["rlnSchemeName"].replace("Schemes/","").replace("/","")

        logfile_path=self.line_path_new_project.text()+ os.path.sep + logMid +".log"

        if hasattr(self, 'timer') and self.timer.isActive():

            self.timer.stop()  

        if not hasattr(self, 'timer'):

            self.timer = QTimer(self)

        self.timer = QTimer(self)

        self.timer.timeout.connect(lambda: self.view_log_file(logfile_path))

        self.timer.start(4000)  # Updat

    

    def generateProject(self):

        """

        first, create a symlink to the frames and mdoc files and change the absolute paths provided by the browse 

        function to relative paths to these links and change the input fields accordingly.

        Then, go through all tabs that were created using the makeJobTabs function, select the table that is in that tab

        and iterate through the columns and rows of that table, checking whether there is an alias (and reverting

        it if there is) and then writing the value into the df for the job.star file at the same position as it 

        is in the table (table is created based on this df so it should always be the same position and name). 

        """

        

        if self.checkPipeRunner(warnProjectExists=True):

            return

        

        scheme=self.cbdat.scheme

        scheme=self.updateSchemeFromJobTabs(scheme,self.tabWidget)

        self.cbdat.scheme=scheme

        

        args=self.cbdat.args

        args.mdocs=self.line_path_mdocs.text()

        args.movies=self.line_path_movies.text()

        args.proj=self.line_path_new_project.text()

        args.scheme=scheme

        invTilt=self.textEdit_invertTiltAngle.toPlainText()

        if invTilt == "Yes":

            invTilt=True

        else:

            invTilt=False

        pipeRunner=pipe(args,invMdocTiltAngle=invTilt)

        pipeRunner.initProject()

        pipeRunner.writeScheme()

        #pipeRunner.scheme.schemeFilePath=args.proj +  "/Schemes/relion_tomo_prep/scheme.star"

        self.cbdat.pipeRunner=pipeRunner

        

    def updateWorkflow(self):

        

        if self.checkPipeRunner()==False:

            return

        scheme=self.cbdat.scheme

        scheme=self.updateSchemeFromJobTabs(scheme,self.tabWidget)

        scheme.scheme_star=starFileMeta(self.cbdat.pipeRunner.scheme.schemeFilePath)

        self.cbdat.scheme=scheme

        self.cbdat.pipeRunner.scheme=scheme

        self.cbdat.pipeRunner.writeScheme()

        

    def importData(self):    

        

        if self.checkPipeRunner()==False:

            return

        self.cbdat.pipeRunner.pathFrames=self.line_path_movies.text()

        self.cbdat.pipeRunner.pathMdoc=self.line_path_mdocs.text()

        self.cbdat.pipeRunner.importPrefix=self.line_path_crImportPrefix.text()

        self.cbdat.pipeRunner.importData()    

        #scheme=self.updateSchemeFromJobTabs(scheme,self.tabWidget)

        #self.cbdat.pipeRunner.writeScheme()

    

    def updateSchemeFromJobTabs(self,scheme,tabWidget):

        """

        Updates the given scheme by iterating over each job tab in the given tab widget and updating the corresponding job's star file based on the table widget's contents.


        Parameters:

            scheme (Scheme): The scheme object to be updated.

            tabWidget (QTabWidget): The tab widget containing the job tabs.


        Returns:

            Scheme: The updated scheme object.

        """

        

        for job_tab_index in range(1, len(scheme.jobs_in_scheme) + 1):

            tabWidget.setCurrentIndex(job_tab_index+self.jobTapNrSetUpTaps-1)

            table_widget = tabWidget.currentWidget().findChild(QTableWidget)

            jobName = scheme.jobs_in_scheme[job_tab_index]

            scheme.job_star[jobName]=self.updateSchemeJobFromTable(scheme.job_star[jobName], table_widget,jobName,self.cbdat.conf)


            

        tabWidget.setCurrentIndex(len(self.cbdat.scheme.jobs_in_scheme) + self.jobTapNrSetUpTaps)

        return scheme    

    

    def updateSchemeJobFromTable(self,job, table_widget, jobName, conf):

        """

        update the job_star_dict with the values of the table_widget

        """

        nRows = table_widget.rowCount()

        nCols = table_widget.columnCount()

        

        for row in range(nRows):

            for col in range(nCols):

                value = table_widget.item(row, col).text()

                if col == 0:

                    original_param_name = conf.get_alias_reverse(jobName, value)

                    if original_param_name != None:

                        # param_name = original_param_name

                        value = original_param_name     

                # insert value at the position defined by the index of the table

                job.dict["joboptions_values"].iloc[row, col] = value

        

        return job

   

        

    


    


if __name__ == "__main__":

    app = QApplication(sys.argv)

    ui = MainUI()

    ui.show()

    app.exec()

```