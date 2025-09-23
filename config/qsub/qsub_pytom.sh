#!/usr/bin/env bash

#SBATCH --job-name="relion_sub_you_shouldn't_sbatch_this"
#SBATCH --mem=10M
#SBATCH --time=00:01:00

##############
# TODO: make sure there is an error to the user if THIS script doesn't even reach the end! ASAP!
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################
##########################################


set -eu

version='ja_relion5_cbe_240416'

# clear

function timestamp () {
	
	echo $(date +%Y%m%d_%H%M%S)
	
}

###################################################################################

# external constants

helperScriptsFolder='/appdata/vbc-tools/clausen/scripts/cbe'

# logsRoot/relion/version
logRootFolder='/scratch/juraj.ahel/TEMP_LOG/relion/ja_relion5_cbe_240416'
 

EmFrameworkSourceFile="${helperScriptsFolder}/EMFrameworkSource.src"

#module_motioncorr='motioncor2/1.6.4-gcccore-12.2.0'
#module_gctf='gctf/1.18_b2'
#module_ctffind='ctffind/4.1.13-fosscuda-2018b'

#module_ghostscript='ghostscript/9.23-gcccore-7.3.0'

#module_cpu_environment='build-env/i2020'
#module_relion_cpu='relion/3.1-beta-a7b0b-avx5122-intel-2019.02'

#module_resmap='resmap/1.95-linux64-cuda-8.0.61'
#module_anaconda='anaconda3/2019.10'
#relionPythonCondaEnvPath='/software/extra/em/python-relion'
#topazPythonCondaEnvPath='/software/extra/em/topaz/0.2.4'

###################################################################################


# needed because relion will weirdly handle this, adding multiple lines of this XXX_command_XXX thing...

tempFilenameForCommands='.RelionGUISubmissionCommands_'$(timestamp)
trap "rm $tempFilenameForCommands" EXIT 

cat <<'EOF' > "$tempFilenameForCommands"
XXXcommandXXX
EOF


relion_gui_jobtype="XXXjobtypeXXX"
relion_gui_mpinodes="XXXmpinodesXXX"
relion_gui_threads="XXXthreadsXXX"
relion_gui_qos="XXXextra1XXX"
relion_gui_mem="XXXextra2XXX"
relion_gui_jobname="XXXextra3XXX"
relion_gui_timelimit="XXXextra4XXX"
relion_gui_nodeSpread="XXXnodespreadXXX"
relion_gui_filename="XXXnameXXX"   
relion_gui_errorfile="XXXerrfileXXX"
relion_gui_outputfile="XXXoutfileXXX"
relion_gui_queue="XXXqueueXXX"
relion_gui_sbatch_extra="XXXextra5XXX"

runningUser=$(whoami)
relionFolder=$(realpath .)
relionParentFolder=$(realpath '..')

loadedModules=$(ml 2>&1 | sed -r 's|^|# |')

# ############################################################################################################
# ############################################################################################################
# ############################################################################################################

# internal constants

extraLoad=""
commandsBeforeRelionCommand=""
commandsAfterRelionCommand=""

gpus_per_gpu_node=4
memory_per_gpu_node=175
cpus_per_gpu_node=14
cpus_per_cpu_node=22


jobTypes=(
'Import'
'MotionCorr'
'CtfFind'
'ManualPick'
'AutoPick'
'Extract'
'Select'
'Class2D'
'Class3D'
'Refine3D'
'MaskCreate'
'JoinStar'
'Subtract'
'PostProcess'
'LocalRes'
'InitialModel'
'MultiBody'
'Polish'
'CtfRefine'
'DynaMight'
'ModelAngelo'
'ImportTomo'
'PseudoSubtomo'
'CtfRefineTomo'
'ExcludeTiltImages'
'FrameAlignTomo'
'ReconstructParticleTomo'
'DenoiseTomo'
'PickTomo'
'External'
'AlignTiltSeries'
'ReconstructTomograms'
)

declare -A jobsMapping

jobsMapping[-1]="unrecognizedJob"
jobsmapping[0]="Import"
jobsmapping[1]="MotionCorr"
jobsmapping[2]="CtfFind"
jobsmapping[3]="ManualPick"
jobsmapping[4]="AutoPick"
jobsmapping[5]="Extract"
jobsmapping[6]="RelionObsoleteJob_6"
jobsmapping[7]="Select"
jobsmapping[8]="Class2D"
jobsmapping[9]="Class3D"
jobsmapping[10]="Refine3D"
jobsmapping[11]="RelionObsoleteJob_11"
jobsmapping[12]="MaskCreate"
jobsmapping[13]="JoinStar"
jobsmapping[14]="Subtract"
jobsmapping[15]="PostProcess"
jobsmapping[16]="LocalRes"
jobsmapping[17]="RelionObsoleteJob_17"
jobsmapping[18]="InitialModel"
jobsmapping[19]="MultiBody"
jobsmapping[20]="Polish"
jobsmapping[21]="CtfRefine"
jobsmapping[22]="DynaMight"
jobsmapping[23]="ModelAngelo"
jobsmapping[50]="ImportTomo"
jobsmapping[51]="PseudoSubtomo"
jobsmapping[52]="CtfRefineTomo"
jobsmapping[53]="ExcludeTiltImages"
jobsmapping[54]="ReconstructParticleTomo"
jobsmapping[55]="FrameAlignTomo"
jobsmapping[56]="ReconstructTomograms"
jobsmapping[57]="PickTomo"
jobsmapping[58]="DenoiseTomo"
jobsmapping[59]="AlignTiltSeries"
jobsmapping[99]="External"


# ############################################################################################################
# ############################################################################################################
# ############################################################################################################

# TODO: add GPU usage logging

initial_info_commands1="

set -eu

##############################################################################################################
echo '$(timestamp)'
echo 'Submission script version: ' $version
echo 'Submitted from $(hostname -f) using template script at
$(realpath $0)'

echo 'Module environment: '
${loadedModules}

"

initial_info_commands2=$(cat <<'INITIAL_INFO_COMMANDS'

completionFlag=0

## exclude manual OMPI parameter override based on IT input on 2023-12-14, ISD-45961
# # correct MPI communication parameters
# export OMPI_MCA_btl="self,vader,tcp"
# 
# # Uemit suggested this 2020-07 for communication stability
# export OMPI_MCA_btl_tcp_if_include=ens5
# export OMPI_MCA_oob_tcp_if_include=ens5


if [[ ! -v SLURM_MEM_PER_NODE ]]; then
	SLURM_MEM_PER_NODE='unavailable'
fi

if [ ! -v SLURM_MEM_PER_CPU ]; then
	SLURM_MEM_PER_CPU='unavailable'
fi

echo '########################################'
echo "This job is being run through slurm on cluster $SLURM_CLUSTER_NAME ($SLURM_SUBMIT_HOST / $(hostname -f))"
echo "Job id: $SLURM_JOB_ID" 
echo "MPI tasks: $SLURM_NTASKS    ::    Cpus per task: $SLURM_CPUS_PER_TASK"
echo "Memory per CPU: $SLURM_MEM_PER_CPU    ::    Per node: $SLURM_MEM_PER_NODE"
echo "Partition: $SLURM_JOB_PARTITION"
echo "QOS: $SLURM_JOB_QOS"
echo "Running user: $(whoami) (Slurm user: $SLURM_JOB_ACCOUNT)"
echo "Parent working folder: $SLURM_SUBMIT_DIR"
echo
if [ "$SLURM_JOB_PARTITION" == 'g' ]; then
	echo "available GPUs (on master node - $(hostname)):"
	nvidia-smi --list-gpus
else
	echo "INFO: Skipping gpu detection as the partition is not gpu"
fi

echo "#####################################"
echo "relion version:
$(relion --version)"

echo "Relion module:"
echo "$(ml -t 2>&1 | grep relion)"

echo '########################################'
echo
# commands to run below:
###############################################################################

INITIAL_INFO_COMMANDS
)


# NOTES:
# 1)
# --spread-job is important in GPU jobs, because the GPUs are allocated on different nodes based on gres and evenly,
# but if you ask for 14 tasks with 1 thread on 2 nodes, slurm will easily put 13 tasks on one node, and 1 task on the other,
# so most of the GPUs on node 2 will not be utilized, and the GPUs on node 1 will be shared by multiple processes!
# with CPUs it's not necessary to force spreading
# 2)

# ############################################################################################################
# ############################################################################################################
# ############################################################################################################

# TEMPLATES #########################################################

shared_header_template=$(cat <<'SHARED'
#!/usr/bin/env bash

# jobtype in gui: JA_jobtype_JA

#SBATCH --job-name=JA_jobname_JA

#SBATCH --wckey=relion

#SBATCH --qos=JA_qos_JA
#SBATCH --partition=JA_partition_JA
JA_nodeSpreadPreference_JA
JA_timeLimit_JA

#SBATCH --ntasks=JA_mpitasks_JA
#SBATCH --cpus-per-task=JA_threads_JA

#SBATCH --open-mode=truncate
#SBATCH --error=JA_errorfile_JA
#SBATCH --output=JA_outputfile_JA


SHARED
)

template_gpu=$(cat <<'TEMPLATE_END'
#SBATCH --mem-per-cpu=JA_mem_per_cpu_JA

#SBATCH --constraint="g2|g3|g4"
##SBATCH --gres=gpu:JA_gpus_JA
#SBATCH --gres=gpu:1
#SBATCH --spread-job

TEMPLATE_END
)

template_gpu_fullnode=$(cat <<'TEMPLATE_END'
#SBATCH --mem=MaxMemPerNode

#SBATCH --constraint="g2|g3|g4"
##SBATCH --gres=gpu:8
#SBATCH --gres=gpu:1
#SBATCH --spread-job

TEMPLATE_END
)

template_cpu=$(cat <<'TEMPLATE_END'
#SBATCH --mem-per-cpu=JA_mem_per_cpu_JA

TEMPLATE_END
)

template_cpu_fullnode=$(cat <<'TEMPLATE_END'
#SBATCH --mem=MaxMemPerNode

TEMPLATE_END
)


# ############################################################################################################
# ############################################################################################################
# ############################################################################################################


# helper

function mod() {

	target=$1
	divisor=$2

	echo $(($1 - (($1 / $2) * $2) ))

}

function min() {

	first=$1
	second=$2

	if [ $first -lt $second ]; then
		echo $first
	else
		echo $second
	fi

}

function max() {

	first=$1
	second=$2

	if [ $first -gt $second ]; then
		echo $first
	else
		echo $second
	fi

}

function CEIL() {

	target=$1
	roundingFactor=$2
	
	echo $((($target + $roundingFactor - 1) / $roundingFactor))

}

function getMaxWholeNumberDivisorUpTo() {

	target=$1
	maxDivisor=$2

	for i in $(seq $maxDivisor -1 1); do
		if [ $(mod $target $i) -eq 0 ]; then
			echo $i
			exit 0
		fi
	done

}

function getLastFinalDataStarFromFolder() {
	
	targetFolder="$1"
	
	ls -t ${targetFolder}/* | grep -E '/run(_ct[0-9]+)?_data.star$' | head -1
	
}

# ############################################################################################################
# ############################################################################################################
# ############################################################################################################

# all the initial checks (do inputs make sense?)

# function parseCommand() {
	
# 	# binary_command=$(sed -r 's|`which ([^ ]+)`.*|\1|' <"$tempFilenameForCommands")	
# 	# echo "$binary_command"
		
# 	for testjobType in ${jobTypes[@]}; do
		
# 		# most jobs syntax
# 		if [ "$(grep -c -F ' --o '${testjobType}'/job' <"$tempFilenameForCommands")" -gt 0 ]; then
# 			echo $testjobType
# 			exit 0
# 		# autopick job syntax
# 		elif [ "$(grep -c -F ' --odir '${testjobType}'/job' <"$tempFilenameForCommands")" -gt 0 ]; then
# 			echo $testjobType
# 			exit 0
# 		# extract job syntax
# 		elif [ "$(grep -c -F ' --part_dir '${testjobType}'/job' <"$tempFilenameForCommands")" -gt 0 ]; then
# 			echo $testjobType
# 			exit 0		
# 		# DynaMight
# 		elif [ "$(grep -c -F ' --output-directory '${testjobType}'/job' <"$tempFilenameForCommands")" -gt 0 ]; then
# 			echo $testjobType
# 			exit 0		
# 		# ModelAngelo
# 		elif [ "$(grep -c -F ' -o '${testjobType}'/job' <"$tempFilenameForCommands")" -gt 0 ]; then
# 			echo $testjobType
# 			exit 0
# 		fi

# 	done

# 	echo 'unrecognizedJob'

# }


function isScratchUsed() {

	targetFlag='--scratch_dir'

	if [ "$(grep -c -F ' '"${targetFlag}"' ' <"$tempFilenameForCommands")" -gt 0 ]; then
		echo 1
	else
		echo 0
	fi
	
}


function isMotionCorr2Requested() {

	targetFlag='--use_motioncor2'

	if [ "$(grep -c -F ' '"${targetFlag}"' ' <"$tempFilenameForCommands")" -gt 0 ]; then
		echo 1
	else
		echo 0
	fi
	
}

function isTopazRequested() {
	
	targetFlag=' --topaz_extract'
	targetFlag2='--topaz_train_picks'

	if [ "$(grep -c -F ' '"${targetFlag}"' ' <"$tempFilenameForCommands")" -gt 0 ] || [ "$(grep -c -F ' '"${targetFlag2}"' ' <"$tempFilenameForCommands")" -gt 0 ]; then
		echo 1
	else
		echo 0
	fi

}


function isGctfRequested() {

	targetFlag='--use_gctf'

	if [ "$(grep -c -F ' '"${targetFlag}"' ' <"$tempFilenameForCommands")" -gt 0 ]; then
		echo 1
	else
		echo 0
	fi
	
}

function isCtffindRequested() {

	targetFlag='--is_ctffind4'

	if [ "$(grep -c -F ' '"${targetFlag}"' ' <"$tempFilenameForCommands")" -gt 0 ]; then
		echo 1
	else
		echo 0
	fi
	
}


function parseRelionFilename() {

	tJobNumber=$(sed -r 's|.*/job([0-9]+).*|\1|' <<<"$relion_gui_filename")
	echo $tJobNumber

}

function isGpuRequestedForClassicRelionJob() {
	if [ "$(grep -c -F ' --gpu ' <"$tempFilenameForCommands")" -gt 0 ] ; then
		echo 1
	else
		echo 1
	fi
}


function isCpuAccelerationRequested() {
	if [ "$(grep -c -F ' --cpu ' <"$tempFilenameForCommands")" -gt 0 ]; then
		echo 1
	else
		echo 0
	fi
}

function parseQueue() {

	if [ "$relion_gui_queue" == "" ] || [ "${relion_gui_queue^^}" == "AUTO" ]; then
		forcePartition=0
		forcedPartition=''
	else
		forcePartition=1
		forcedPartition="$relion_gui_queue"
	fi

}

function parseSpread() {
	echo $relion_gui_nodeSpread
}

function parseMpiNodes() {
	echo $relion_gui_mpinodes
}

function parseMemory() {

	if [ "$relion_gui_mem" == "" ] || [ "${relion_gui_mem^^}" == "AUTO" ]; then
		echo 'INFO: Requested memory defaulting to 13 GB' >&2
		requestedMemoryPerTask='13'
	elif [[ "$relion_gui_mem" =~ ^[1-9][0-9]*$ ]]; then
		requestedMemoryPerTask=$relion_gui_mem
	else
		echo 'WARNING: Invalid memory specification, defaulting to 13 GB' >&2
		requestedMemoryPerTask='13'
	fi

}

function parseSbatchExtraParameters() {

	# HACKING MY PAIN WITH HIS FINGERS
	# to ensure relion doesn't replace this before comparison
	if [ "$relion_gui_sbatch_extra" == "XXX""extra""5XXX" ]; then
		sbatch_extra_parameters=""
	else
		sbatch_extra_parameters="${relion_gui_sbatch_extra}"
	fi


}

function parseJobname() {

	if [ "$relion_gui_jobname" == "" ] || [ "${relion_gui_jobname^^}" == "AUTO" ]; then
	
		
		echo 'relion-'${jobType}'_'${relionJobNumber}
		
	else
	
		echo "$relion_gui_jobname"
		
	fi

}

function parseQos () {

	if [ "$relion_gui_qos" == "" ] || [ "${relion_gui_qos^^}" == "AUTO" ]; then
		echo 'short'
	else
		echo "$relion_gui_qos"
	fi

}

function parseTimeLimit () {
		
	if [[ "$relion_gui_timelimit" =~ ^([0-9]{1,2}-)?[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$ ]]; then
	
		echo '#SBATCH --time='$relion_gui_timelimit
		
	else
	
		case ${qos^^} in
		
			short)
				echo ''
			;;
			
			medium)
				echo ''
			;;
			
			long)
				echo ''
			;;
		esac
	fi
	
}
	
function getAvailableGpuNodes() {
	
	# G-gres n-name T-state E-reason for unavailability
	totalNumber=$(sinfo --format="%G %n %T %E" | grep -F 'clip-g' | grep -c -E '(idle|mixed|allocated)')	
	echo $totalNumber

}

function getOfflineGpuNodes() {
	
	# G-gres n-name T-state E-reason for unavailability
	totalNumber=$(sinfo --format="%G %n %T %E" | grep -F 'clip-g' | grep -c --invert-match -E '(idle|mixed|allocated)')	
	echo $totalNumber

}

########################## INITIALIZE ###########################################

#2023 relion5
if [[ "${relion_gui_jobtype}" == "" ]]; then
	job_relion_int=-1
else
	job_relion_int=${relion_gui_jobtype}
fi

jobTypeNew=${jobsmapping[${job_relion_int}]}

#


availableGpuNodes=$(getAvailableGpuNodes)

#jobType=$(parseCommand)
jobType=${jobTypeNew}

spread=$(parseSpread)
requestedMpiNodes=$(parseMpiNodes)

requestedMemoryPerTask=0
parseMemory
parseSbatchExtraParameters

qos=$(parseQos)
threads=$relion_gui_threads

use_gpu=$(isGpuRequestedForClassicRelionJob)
cpu_accelerate=$(isCpuAccelerationRequested)

relionJobNumber=$(parseRelionFilename)

job_folder=${jobType}'/job'${relionJobNumber}

slurmTimeLimitSpecifier=$(parseTimeLimit)

errorFilePath="$relion_gui_errorfile"
outputFilePath="$relion_gui_outputfile"

if [ -f "${errorFilePath}" ]; then
	replacementErrorFile=$(dirname "${errorFilePath}")'/old_'$(date +%Y%m%d_%H%M%S)'_'$(basename "${errorFilePath}")
	mv "${errorFilePath}" "${replacementErrorFile}"
fi

if [ -f "${outputFilePath}" ]; then
	replacementoutputFile=$(dirname "${outputFilePath}")'/old_'$(date +%Y%m%d_%H%M%S)'_'$(basename "${outputFilePath}")
	mv "${outputFilePath}" "${replacementoutputFile}"
fi

########################## /INITIALIZE ##########################################

# corrections for each job type

function correctMpiNodesAndGpuForSelectedMPIJobs () {

	if [ $requestedMpiNodes -gt 2 ]; then
		
		# initial3D likes having an odd number of mpiNodes, since one is consumed as master node
		# and the rest is split in two equal parts
		# so I choose the decrease the req. number by one if it's even
		if [ $(mod $requestedMpiNodes 2) -eq 0 ]; then
			echo "WARNING: Initial model is inefficient with even number of mpi ranks (${requestedMpiNodes}), taking one less ($(($requestedMpiNodes - 1)))" >&2
			mpiNodes=$(($requestedMpiNodes - 1))
		else
			mpiNodes=$requestedMpiNodes
		fi
		
		# one mpi rank is the master mpi rank and does not do computation, so doesn't need a gpu
		number_of_gpus=$(($mpiNodes - 1))
		
	else
		
		# the program requested only 1 MPI, give it 1 gpu
		mpiNodes=1
		number_of_gpus=1
	
	fi
	
}

commandsBeforeRelionCommand="
# extra commands before the job::

set +e
source "${EmFrameworkSourceFile}"
set -e

"

# set -x

case $jobType in 

	MultiBody)
	
		expectedMemory=0
		
		correctMpiNodesAndGpuForSelectedMPIJobs
		
	;;
		
	InitialModel)
		
		expectedMemory=0
		
		correctMpiNodesAndGpuForSelectedMPIJobs
			
	;;
	
	Class3D)
		
		expectedMemory=0
		
		correctMpiNodesAndGpuForSelectedMPIJobs
		
		commandsAfterRelionCommand="${commandsAfterRelionCommand}

#############################################################
# after relion is done, print counts of particles assigned to each class

echo
echo ############
echo Class counts:
scriptPath=${helperScriptsFolder}/countMembersOfClassNumberInDataStar.sh
cd ${job_folder}
dataStar=\$(ls -t *.star | grep -E 'run.*_data.star$' | head -1)
set +e
\${scriptPath} \${dataStar} 2>/dev/null
set -e
cd ${relionFolder}
				
"		
			
	;;

	Refine3D)
		
		expectedMemory=0
		
		correctMpiNodesAndGpuForSelectedMPIJobs


	commandsAfterRelionCommand="${commandsAfterRelionCommand}

#############################################################
# after relion is done, generate the angular distribution plot

scriptPath=${helperScriptsFolder}/plotAngularDistributionFromDataStar.sh
angDistLog=angdist.log
cd ${job_folder}
dataStar=\$(getFinalParticlesInRelionRefinementFolder.sh .)
set +e
echo
echo 'Generating angular distribution plot...'
echo using \$dataStar
\${scriptPath} \${dataStar} 1>\${angDistLog} 2>\${angDistLog}
echo Generated ${job_folder}/angdist_${relionJobNumber}_angdist.png
echo
set -e
cd ${relionFolder}
				
"



		
		
	;;
	
	Class2D)
		
		expectedMemory=0
		
		mpiNodes=$requestedMpiNodes
		if [ $mpiNodes -gt 1 ]; then
			number_of_gpus=$(($mpiNodes - 1))
		else
			number_of_gpus=1
		fi


		commandsAfterRelionCommand="${commandsAfterRelionCommand}
		
#############################################################
# after relion is done, generate individual images of class averages as png files

scriptPath=${helperScriptsFolder}/extractClassAveragesFromMrcsToPng.sh
cd ${job_folder}
mrcsFile=\$(ls -t *.mrcs | grep -E 'run(_ct[0-9]+)?_it[0-9]+_classes.mrcs$' | head -1)
tOutFolder=pngClassImages
set +e
echo 'Generating png images of classes...'
\${scriptPath} \${mrcsFile} \${tOutFolder} 1>/dev/null 2>/dev/null
set -e
cd ${relionFolder}

				
"		

		
	;;
		
	AutoPick)
	
		expectedMemory=0
		
		# Autopicks will use one GPU per MPI node, including MPI master node, so we need as many gpus as mpi nodes!
		# TODO: is this still true in new relion?
		# TODO: how many gpus does topaz use when run through relion?
		
		if [ $(isTopazRequested) -eq 1 ]; then 
		#	extraLoad="ml ${module_anaconda}
#source activate ${topazPythonCondaEnvPath}"
			number_of_gpus=$mpiNodes
		fi
		
		mpiNodes=$requestedMpiNodes
		number_of_gpus=$mpiNodes
		
#		extraLoad="${extraLoad}
#ml ${module_ghostscript}"
	
	;;
	
	CtfFind)
		
		expectedMemory=0
		
		mpiNodes=$requestedMpiNodes		
		
		# add check whether the external module is used
		
		if [ $(isGctfRequested) -eq 1 ]; then 
		#	extraLoad="ml ${module_gctf}"
			number_of_gpus=$mpiNodes
		fi

		if [ $(isCtffindRequested) -eq 1 ]; then 
            extraLoad=""
        #	extraLoad="ml ${module_ctffind}"
		fi
				
		
	;;
	
	MotionCorr)
		
		expectedMemory=0
		
		if [ $(isMotionCorr2Requested) -eq 1 ]; then
            extraLoad="" 
		#	extraLoad="ml ${module_motioncorr}"
		fi
		
		
		mpiNodes=$requestedMpiNodes
		number_of_gpus=$mpiNodes
		
	;;
		
	
	LocalRes)
		
		expectedMemory=0		
		
		# ResMap version cannot be submitted to queue, and by now it breaks the relion one because of incompatible cuda	
		#extraLoad="ml ${module_resmap}"
				
		mpiNodes=$requestedMpiNodes
		#number_of_gpus=$mpiNodes
		number_of_gpus=0
		
	;;
	
	DynaMight)

		hardcode_use_gpu=1
		expectedMemory=0
		# can use only 1 gpu
		mpiNodes=1
		number_of_gpus=1

	;;

	ModelAngelo)

		hardcode_use_gpu=1
		expectedMemory=0		
		mpiNodes=1
		# todo: make this variable
		number_of_gpus=1

	;;

	*)
	
		mpiNodes=$requestedMpiNodes
		number_of_gpus=$mpiNodes

	;;

esac


##################################### handle CPU acceleration

if [ ${cpu_accelerate} -eq 1 ]; then
    echo "CPU accelleration not yet supported as of 2023-11-02 [Juraj Ahel]
Aborting." >&2
    exit 1
	# commandsBeforeRelionCommand="${commandsBeforeRelionCommand}
	# ml ${module_cpu_environment}
	# ml --ignore-cache ${module_relion_cpu}	
	# "
fi


##################################### add commands before relion command:

commandsBeforeRelionCommand="${commandsBeforeRelionCommand}
${extraLoad}"

######################################


totalMemory=$(($requestedMemoryPerTask * $mpiNodes))
number_of_cpus=$(($threads * $mpiNodes))


# slurm is not smart enough to distribute the requested number of gpus on different nodes, I have to figure that one out
# also, use the requested spread information here

if [[ -v hardcode_use_gpu ]]; then
	use_gpu=${hardcode_use_gpu}
fi

if [ $use_gpu -eq 1 ]; then

	#TODO: add a check whether GPU, Mem, and CPU can be split in mutually-compatible ways
	# e.g. it's not possible with 80 GB per task + 9 tasks, as only 2 tasks fit per node memory-wise, and 9 is not divisible by 2
	# this is a rare event though, as it will happen only with large mem per task combined with mpinodes larger than number of avail gpu nodes (which would anyhow run into GrpMemLimit probably
	
	if [ $requestedMemoryPerTask -gt $memory_per_gpu_node ]; then
		echo "It is not possible to request $requestedMemoryPerTask GB of memory per task, as each node has only $memory_per_gpu_node" >&2
		exit 1
	fi
	
	# CEIL(number / number per node)
	
	# gpus can be assigned in aliquots of 1 so it's a simple calculation 
	maxTasksPerNodeGPU=$gpus_per_gpu_node
	minPossibleNumberOfNodesGPU=$(CEIL $number_of_gpus $maxTasksPerNodeGPU)
	# cpus are assigned in aliquots of $threads - so I first need to see how many n-thread mpi ranks fit into a single node
	# and then see how many of nodes I need by dividing total number of mpi ranks with this number
	maxTasksPerNodeCPU=$(($cpus_per_gpu_node / $threads))
	minPossibleNumberOfNodesCPU=$(CEIL $mpiNodes $maxTasksPerNodeCPU)
	# memory is assigned per task (per cpu actually, but cpu-task ratio is fixed as number of threads)
	# so like with cpus I need to see how many memory-equivalents fit per node, and only then how many nodes I need
	# otherwise it might seem like I can fit 3x 100GB jobs in two 180 GB nodes, if I just divided total mem by mem per node!!!
	maxTasksPerNodeMem=$(($memory_per_gpu_node / $requestedMemoryPerTask))
	minPossibleNumberOfNodesMemory=$(CEIL $mpiNodes $maxTasksPerNodeMem)
	
	# the overall minimum is the largest of the three minima
	minPossibleNumberOfNodes=$(max $minPossibleNumberOfNodesGPU $minPossibleNumberOfNodesMemory)
	minPossibleNumberOfNodes=$(max $minPossibleNumberOfNodes $minPossibleNumberOfNodesCPU)	
	
	if [ $minPossibleNumberOfNodes -gt $availableGpuNodes ]; then
	
		echo "It is not possible to request $number_of_gpus gpus + $number_of_cpus cpus + $totalMemory GB of memory on $availableGpuNodes GPU nodes that are available" >&2
		echo "It would require $minPossibleNumberOfNodes nodes of the defined configuration ( each node has $gpus_per_gpu_node gpus; $memory_per_gpu_node GB memory; $cpus_per_cpu_node cpus)" >&2
		echo "Submission will likely fail, or this script has a bug." >&2
		echo "20191120 Juraj: for the time being, it does have a bug. If the submission works, just ignore this" >&2
		offlineNodes=$(getOfflineGpuNodes)
		
		if [ $offlineNodes -gt 0 ]; then
			
			if [ $offlineNodes -gt 1 ]; then
				stringExtension='s'
			else
				stringExtension=''
			fi
		
			echo "$offlineNodes node${stringExtension} are currently not available for job submission due to breakdown or maintenance"
		
		fi
		
		exit 1
		
	fi
	
	additional_header_template="$template_gpu"
	
	if [ "$spread" == 'single' ]; then
	
		desirableNumberOfNodes=1
		
	fi
	
	if [ "$spread" == 'full' ]; then
	
		additional_header_template="$template_gpu_fullnode"
		desirableNumberOfNodes=$(CEIL $number_of_gpus $gpus_per_gpu_node)
	
	elif [ "$spread" == 'max' ]; then
			
		# depending on divisibility
		desirableNumberOfNodes=$(getMaxWholeNumberDivisorUpTo $number_of_gpus $availableGpuNodes)
		
	elif [ "$spread" == 'min' ]; then
		
		desirableNumberOfNodes=$minPossibleNumberOfNodes
		
	else
		
		# for now keep the min number of nodes unless requested
		desirableNumberOfNodes=$minPossibleNumberOfNodes
	
	fi

	# e.g. it's not posssible to place 11, 13, or 17 gpus since each node must take an equal number of gpus
	if [ $desirableNumberOfNodes -lt $minPossibleNumberOfNodes ]; then
		echo "WARNING: It is not possible to request $number_of_gpus gpus + $number_of_cpus cpus + $totalMemory GB of memory on $availableGpuNodes GPU nodes that are available in a nice way" >&2
		desirableNumberOfNodes=${minPossibleNumberOfNodes}
	fi
	
	# set +x
	
	nodes=$desirableNumberOfNodes
	gpus_per_node=$(CEIL $number_of_gpus $nodes)
	
	slurmNodeSpecifier='#SBATCH --nodes='$desirableNumberOfNodes
	
	partition='g'
	
	slurm_job_suffix='gpu'
		
# no gpu
else
	
	gpus_per_node=0
	
	
	partition='c'
	additional_header_template="$template_cpu"
	
	commandsBeforeRelionCommand="${commandsBeforeRelionCommand}

## exclude manual OMPI parameter override based on IT input on 2023-12-14, ISD-45961
# export OMPI_MCA_mpi_cuda_support=0

"
	
	if [ "$spread" == 'full' ]; then
	
		slurmNodeSpecifier=''
		additional_header_template="$template_cpu_fullnode"
		# slurmNodeSpecifier='#SBATCH --tasks-per-node='$((n
	
	elif [ "$spread" == 'max' ]; then	
		
		slurmNodeSpecifier='#SBATCH --spread-job'
		
	elif [ "$spread" == 'min' ]; then
	
		slurmNodeSpecifier='#SBATCH --use-min-nodes'
		
	else
	
		slurmNodeSpecifier=''
	
	fi
	
	# slurm can handle these on its own, only the gres=gpu:x is tricky
	
	slurm_job_suffix='cpu'
	
fi


parseQueue

submitted_job_name="$(parseJobname)"'-'${slurm_job_suffix}


if [ $forcePartition -eq 1 ]; then
	partition=$forcedPartition
fi

targetMemoryPerCPU=$(CEIL $requestedMemoryPerTask $threads)'G'

# modify the command strings to have all it needs:
# sed -i 's|^|srun --mpi=pmi2 |' "$tempFilenameForCommands"

# changed on 26.06.2020. to make job killing work properly, according to IT this only works for now
sed -i 's|^|mpirun |' "$tempFilenameForCommands"

sharedScriptHeader=$(sed 's|JA_qos_JA|'"${qos}"'|g' <<<"$shared_header_template" | \
sed 's|JA_partition_JA|'"${partition}"'|g' | \
sed 's|JA_mpitasks_JA|'"${mpiNodes}"'|g' | \
sed 's|JA_threads_JA|'"${threads}"'|g' | \
sed 's|JA_mem_per_cpu_JA|'"${targetMemoryPerCPU}"'|g' | \
sed 's|JA_gpus_JA|'"${gpus_per_node}"'|g' | \
sed 's|JA_jobname_JA|'"${submitted_job_name}"'|g' | \
sed 's|JA_nodeSpreadPreference_JA|'"$slurmNodeSpecifier"'|g' | \
sed 's|JA_timeLimit_JA|'"$slurmTimeLimitSpecifier"'|g' | \
sed 's|JA_errorfile_JA|'"$errorFilePath"'|g' | \
sed 's|JA_outputfile_JA|'"$outputFilePath"'|g' | \
sed 's|JA_commands_JA|\n|g' | \
sed 's|JA_jobtype_JA|'"${jobTypeNew}"'|g' )

additionalScriptHeader=$(sed 's|JA_qos_JA|'"${qos}"'|g' <<<"$additional_header_template" | \
sed 's|JA_partition_JA|'"${partition}"'|g' | \
sed 's|JA_mpitasks_JA|'"${mpiNodes}"'|g' | \
sed 's|JA_threads_JA|'"${threads}"'|g' | \
sed 's|JA_mem_per_cpu_JA|'"${targetMemoryPerCPU}"'|g' | \
sed 's|JA_gpus_JA|'"${gpus_per_node}"'|g' | \
sed 's|JA_jobname_JA|'"$submitted_job_name"'|g' | \
sed 's|JA_nodeSpreadPreference_JA|'"$slurmNodeSpecifier"'|g' | \
sed 's|JA_timeLimit_JA|'"$slurmTimeLimitSpecifier"'|g' | \
sed 's|JA_errorfile_JA|'"$errorFilePath"'|g' | \
sed 's|JA_outputfile_JA|'"$outputFilePath"'|g' | \
sed 's|JA_commands_JA|\n|g' )

jobSubmissionScriptsFolder=$(realpath ./scripts)

if [ ! -d "${jobSubmissionScriptsFolder}" ]; then
	mkdir "${jobSubmissionScriptsFolder}"
fi

scriptName=${jobSubmissionScriptsFolder}'/'$(timestamp)'_'"${jobType}"'_job'"${relionJobNumber}"'_'$(sed 's| |_|g' <<<"$submitted_job_name")

# ensure it's a free name
while [ -f "${scriptName}" ]; do
	scriptName="$scriptName"'x'
done

# user-specific stuff
#TODO: external scripts

if [ "${runningUser}" == 'juraj.ahel' ]; then
	#echo 'Test.'
	set +eu
	currenttime=$(date +%H:%M)
    if [[ "$currenttime" > "20:00" ]] || [[ "$currenttime" < "04:00" ]]; then		
	#	echo ...echo
		echo "Test time (shows for j.a. only)." >&2
	fi
	set -eu
fi

# if [ "${runningUser}" == 'juraj.ahel' ]; then
	
if [ $(isScratchUsed) -eq 1 ]; then
	
	# if a valid path is given, use that
	# if not, then automatically select a path
	
	inputScratchPath=$(grep -F -e '--scratch_dir' "$tempFilenameForCommands" | sed -r 's|.+--scratch_dir ([^ ]+) --.+|\1|')
	
	if [ ! -d ${inputScratchPath} ] || [[ ${inputScratchPath} == 'auto' ]]; then
		
		jobFolder=$(dirname $0)		
				
		targetScratchFolder='/scratch-cbe/users/'${runningUser}'/relionAutoScratch'${relionFolder}'/'${jobFolder}
		
		# replace whatever is in scratch with the parent folder structure, job type, job number
		# pay attention to spaces
		sed -i -r 's|( --scratch_dir )[A-Za-z0-9.-+_]+( --)|\1'${targetScratchFolder}'\2|' "$tempFilenameForCommands"
	
	fi
	
fi	
	
# fi

logFolderExists=0

set +eu
logFolder=${logRootFolder}/${runningUser}/${relionFolder}
scriptLogFolder="${logFolder}/scripts"
resultLogFolder="${logFolder}/results"

mkdir -p "$logFolder" "$scriptLogFolder" "$resultLogFolder" 1>/dev/null 2>/dev/null

if [ -d "$scriptLogFolder" ];  then
	logFolderExists=1
fi
set -eu

############################################ after relion command:



set +eu
if [ ${logFolderExists} -eq 1 ]; then
	saveLogsFuncDef="
function trySaveLogs() {
	##########################################################
	# save logs for statistics & optimization

	set +e

	originalOut=\"${relionFolder}/${outputFilePath}\"
	originalErr=\"${relionFolder}/${errorFilePath}\"

	renamedOut=\"${resultLogFolder}/${jobType}_job${relionJobNumber}_$(timestamp)_$(basename $outputFilePath)\"
	renamedErr=\"${resultLogFolder}/${jobType}_job${relionJobNumber}_$(timestamp)_$(basename $errorFilePath)\"

	echo 'Saving logs...'

	cp \"\${originalOut}\" \"\${renamedOut}\"
	cp \"\${originalErr}\"  \"\${renamedErr}\"

	chmod 777 \"\${renamedOut}\"
	chmod 777 \"\${renamedErr}\"
	
}

"
else
	saveLogsFuncDef="function trySaveLogs() { echo 'Logs reports will not be submitted' >&2 }"
fi
set -eu

commandsBeforeRelionCommand="${commandsBeforeRelionCommand}

${saveLogsFuncDef}
"'
function terminatingCommands () {
	
	set +e
	
	trySaveLogs
	
	echo
	echo "Jobinfo:"
	jobinfo $SLURM_JOB_ID
	if [ $completionFlag -ne 1 ]; then
		echo "Making sure the job is cancelled if the srun command produced an error (i.e. running scancel)"
		scancel $SLURM_JOB_ID
	fi
	echo
	echo "Bye!"
	trap - SIGCONT SIGTERM SIGKILL EXIT
	sleep 1
	exit 1
	set -e
}


# SIGCONT + SIGTERM + SIGKILL from scancel, "EXIT" will be triggered from set -e if error
trap terminatingCommands SIGCONT SIGTERM SIGKILL EXIT
'

terminating_info_commands='
###############################################################################
echo
completionFlag=1
trap - SIGCONT SIGTERM SIGKILL EXIT
terminatingCommands
echo
'


# assemble final script

# make the relion command look more readable
sed -i -r 's|\n|\n\n|' ${tempFilenameForCommands}
sed -i -r 's| (--[a-zA-Z_]+)| \\\n\1|g' ${tempFilenameForCommands}

echo "$sharedScriptHeader" > ${scriptName}              # non-variable slurm header
echo "$additionalScriptHeader" >> ${scriptName}                       # job-type-specific slurm header
echo "$initial_info_commands1" >> ${scriptName}             # notification
echo "$initial_info_commands2" >> ${scriptName}             # make the slurm relion job smarter and more informative to the user
echo "$commandsBeforeRelionCommand" >> ${scriptName}        # preload external modules and stuff like that for jobs that need it (motioncorr, ...)
cat ${tempFilenameForCommands} >> ${scriptName}               # put in the actual commands generated by relion
echo "$commandsAfterRelionCommand" >> ${scriptName}        # all commands that can run after relion (analyses etc)
echo "$terminating_info_commands" >> ${scriptName}          # make the slurm relion job smarter and more informative to the user

# echo $sbatch_extra_parameters
# echo $scriptName

# echo "slurmJobNumber=\$(sbatch \"$sbatch_extra_parameters\" \"$scriptName\" | sed -r 's|.*[Jj]ob ([0-9]+).*|\1|')"

# 20191120:JA: careful, don't quote sbatch_extra_parameters or it won't work!
slurmJobNumber=$(sbatch $sbatch_extra_parameters "$scriptName" | sed -r 's|.*[Jj]ob ([0-9]+).*|\1|')



finalScriptName="$scriptName"'_'"$slurmJobNumber"

mv "$scriptName" "${finalScriptName}"

###################################################################
# save script for documentation
set +eu
if [ ${logFolderExists} -eq 1 ]; then
	cp "${finalScriptName}" "${scriptLogFolder}"
	chmod 777 "${scriptLogFolder}/$(basename ${finalScriptName})"
fi
set -eu


if [ "$slurmJobNumber" == "" ]; then
	echo "Submitted job ${jobType}/job${relionJobNumber} - no known job submission number ???"
	echo "WARNING: the job was likely not properly submitted!"
else
	echo "Submitted job ${jobType}/job${relionJobNumber} - slurm job $slurmJobNumber"
fi

rm ${tempFilenameForCommands}
trap - EXIT

# echo "$scriptHeader" >&2

