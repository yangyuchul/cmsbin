#!/bin/bash

function dirString {
   dirStr=${1//\//_D_}
	echo ${dirStr/_D_/}
}

sleep 2
DataSet=NULL
DataList=NULL
OutPATH=NULL
RunPy=NULL
MaxNFiles=NULL
MaxNEvents=NULL
ReturnOutput=0
AddFiles=""

if [ "$1" == "" ]; then echo "usage: $0 condor_config.file"; exit; fi
if [ ! -f $1 ]; then echo "usage: $0 condor_config.file"; exit; fi
jobConfigFile=$1
source $1

InType=0
if [ "${DataList}" != "NULL" ]; then ((InType++)); fi
if [ "${DataSet}" != "NULL" ]; then ((InType++)); ((InType++)); fi
if [ $InType == 3 ]; then echo "I don't know what is your source? $DataList or $DataSet"; exit; fi
if [ "${RunPy}"   == "NULL" ]; then echo "Set RunPy=Your_Main_Run_cfg.py"; exit; fi
if [ ! -f ${RunPy} ];          then echo "NotFound ${RunPy}"; exit; fi

if [ "${OutPATH}" == "NULL" ]; then 
	ReturnOutput=1
	#echo "Set OutPATH=root://cms-xrdr.sdfarm.kr:1094///cms/data/xrd/ycyang/test"; 
fi
if [ ${MaxNFiles}  == "NULL" ]; then MaxNFiles="1"; fi
if [ ${MaxNEvents} == "NULL" ]; then MaxNEvents="-1"; fi

echo "### Set Options #####" 
if [ "${DataSet}" != "NULL" ]; then echo "DataSet=$DataSet"; fi
if [ "${DataList}" != "NULL" ]; then echo "DataList=$DataList"; fi
echo "OutPATH=${OutPATH}"
echo "ReturnOutput=${ReturnOutput}"
echo "RunPy=${RunPy}"
echo "MaxNFiles=$MaxNFiles"
echo "MaxNEvents=$MaxNEvents"
if [ "${AddFiles}" != "" ]; then
   for AddFile in $AddFiles
   do
      echo "AddFile=$AddFile"
   done
fi
echo "#####################"

##### makeDASList
function makeDASList() {
	echo "# Step2 Checking Dataset "
   dataset=$1
   datasetStr=`dirString ${dataset}`
   dir=$2
   if [ "${dataset:(-4)}" == "USER" ]; then instance="instance=prod/phys03"; fi
   das_client.py --query="file dataset=${dataset} ${instance} | grep file.name, file.size, file.nevents" --limit=0 | grep ".root" >  ${dir}/dataset.txt
	cat ${dir}/dataset.txt | awk '{print $1}' > ${dir}/dataset.list

   numFile=`grep ".root" ${dir}/dataset.txt | wc -l`
   totalsize=0
   for size in `cat ${dir}/dataset.txt | awk '{print $2}'`
   do
      if [ "${size:(-2)}" == "MB" ]; then size=${size/MB}; size=`python -c "print $size * 1024 * 1024"`; fi
      if [ "${size:(-2)}" == "GB" ]; then size=${size/GB}; size=`python -c "print $size * 1024 * 1024 * 1024"`; fi
      totalsize=`python -c "print $totalsize + $size"`
   done
	totalsize=`python -c "print $totalsize / 1024.0 / 1024.0 / 1024.0"`
   totalevent=0
   for event in `cat ${dir}/dataset.txt | awk '{print $3}'`
   do
      totalevent=`python -c "print $totalevent + $event"`
   done

	sites=`das_client.py --query="site dataset=${dataset} ${instance}" --limit=0`
	sites=${sites//'N/A'/}
	isT3_KR_KISTI=`echo $sites | grep -E "cms-se.sdfarm.kr|T3_KR_KISTI" | wc -l`
	if [ "$isT3_KR_KISTI" != "0" ]; then 
		rm -rf ${dir}/dataset.list.temp
		for file in `cat ${dir}/dataset.list`
		do
			echo "root://cms-xrdr.sdfarm.kr:1094//cms/data/xrd${file}" >> ${dir}/dataset.list.temp
		done
		mv ${dir}/dataset.list.temp ${dir}/dataset.list
	fi

	tempSites=""
	for site in $sites; do tempSites="$site $tempSites"; done
	echo "SummaryDataset $dataset $numFile $totalevent $totalsize sites $tempSites" >> ${dir}/dataset.txt


   numFiles=`cat ${dir}/dataset.list | wc -l`
   if [ ${numFiles} == "0" ]; then
      echo "   Number of File is 0 in ${dataset}"
      exit
   else
      echo "   OK, Found `cat ${dir}/dataset.list | wc -l` files on ${dataset}"
   fi
}

##### makeCode
function makeCode() {
	echo "# Step1 Making CMSSW Code "
	if [ ! -d ${1} ]; then echo "usage $0 diretory"; exit; fi
	tarDir=`basename $1`
	tDir="/tmp/`whoami`/CondorJob/${tarDir}/input"
	mkdir -p ${tDir}
	dirs="biglib bin cfipython doc external include lib logs objs python test tmp"
	for dir in $dirs
	do
		cp -r ${CMSSW_BASE}/$dir ${tDir}/
	done
	mkdir $tDir/input
	cd $CMSSW_BASE/src
	pythonDirs=`find . -type d -name "python"`
	for pythonDir in $pythonDirs
	do
		#echo $pythonDir
		dirName=`dirname $pythonDir`
		driName=${dirName/".\/"/}
		if [ ! -d $tDir/src/$dirName ]; then mkdir -p $tDir/src/$dirName; fi
		cp -r $pythonDir $tDir/src/$dirName/
	done
	cd $tDir/..
	tar zcf input.tgz input
	mv input.tgz ${1} 
}

###### makeRun
function makeRun() {
echo ""
echo "# Step3 Creating CondorJob "
runName=`basename $1`
runName=${runName/.py/}
MaxNFiles=$2
MaxNEvents=$3
dir=$4
dirName1=`basename $dir`
dirName2=`dirname $dir`
dirName2=`basename $dirName2`
proxyFile=$5
OutPATH=$6

if [ "${OutPATH}" != "NULL" ]; then
	echo "   OutPATH=${OutPATH}"
fi
cat << EOF > ${dir}/condorPset.py
import FWCore.ParameterSet.Config as cms
import FWCore.ParameterSet.VarParsing as VarParsing
options = VarParsing.VarParsing ('standard')
options.parseArguments()
#process = cms.Process('${dirName1}')
from ${dirName2}.${dirName1}.${runName} import *
process.source.fileNames = cms.untracked.vstring(options.files)
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(${MaxNEvents}))
EOF

cat << EOF > ${dir}/condor.sh
#!/bin/bash
CondorCluster=\$1
CondorProcess=\$2
DoXrdCpPATH=\$3

echo "### Condor Running \`whoami\`@\`hostname\`:\`pwd\` \${CondorCluster} \${CondorProcess} ###"
echo ""
FirstDir=\`pwd\`
export X509_USER_PROXY=\${FirstDir}/`basename ${proxyFile}`
echo "### VOMS Proxy \${X509_USER_PROXY} "
voms-proxy-info
echo ""
echo "### FirstDir=\${FirstDir}"
mkdir \${FirstDir}/output
ls -al
echo ""
tar zxf input.tgz
ls -al input/
echo "### CMSSW SET in \${PWD}"
export SCRAM_ARCH=${SCRAM_ARCH}
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source \${VO_CMS_SW_DIR}/cmsset_default.sh
scramv1 project CMSSW `basename ${CMSSW_VERSION}`
cd `basename ${CMSSW_VERSION}`
rm -rf biglib bin cfipython doc external include lib logs objs python src test tmp
mv ../input/* .
echo "### \`pwd\` ###"
ls -al 
echo ""
cd src
eval \`scramv1 runtime -sh\`
echo "which cmsRun \`which cmsRun\`"
mv \${FirstDir}/condorPset.py .
mv \${FirstDir}/dataset.list .
echo "### \`pwd\` ###"
ls -al
echo ""

LastN=\`expr \${CondorProcess} + 1\`
LastN=\`expr \${LastN} \* ${MaxNFiles} - 1\`
FirstN=\`expr \${LastN} - ${MaxNFiles} + 1\`
index=0
aFile=""
for file in \`cat dataset.list\`
do
	if [ \${index} -ge \${FirstN} ]; then
		if [ \${index} -le \${LastN} ]; then
			if [ "\${index}" == "\${FirstN}" ]; then
				aFile=\$file
			else
				aFile="\${aFile}, \${file}"
			fi
		fi
	fi
	((index++))
done
for a in \${aFile}
do
	echo "InFile: \$a"
done
echo "### start cmsRun"
cmsRun condorPset.py print files="\${aFile}" 
echo "END cmsRun \$?"

echo ""
echo "### Copying to SE"
rootFiles=\`find . -maxdepth 1 -name "*.root"\`
for rootFile in \${rootFiles}
do
	ls -al \$rootFile
	bRootFile=\`basename \${rootFile}\`
	bRootFile=\${bRootFile/.root/}
	bRootFile="\${bRootFile}_\${CondorCluster}_\${CondorProcess}_\${RANDOM}.root"
	mv \${rootFile} \${FirstDir}/output/\${bRootFile}
done

if [ "\${DoXrdCpPATH}" != "NULL" ]; then
	rootFiles=\`find \${FirstDir}/output -maxdepth 1 -name "*.root"\`
	for rootFile in \${rootFiles}
	do
		bRootFile=\`basename \${rootFile}\`
		echo "xrdcp \${rootFile} \${DoXrdCpPATH}/\${bRootFile}"
		xrdcp \${rootFile} \${DoXrdCpPATH}/\${bRootFile}
	done
fi

echo "END Copy \$?"

cd \${FirstDir}


echo "Done CondorRun \$CondorCluster \$CondorProcess"
ls -al 
echo "Bye Bye !!! "
EOF

AddLINE="export XRD_TRANSACTIONTIMEOUT=80"

sed -i "s/^cmsRun/${AddLINE}\ncmsRun/" ${dir}/condor.sh
chmod +x ${dir}/condor.sh


total=`cat $dir/dataset.list | wc -l`
total=`expr $total + $MaxNFiles - 1`
nJob=`expr $total / $MaxNFiles`
mkdir ${dir}/log
cp $proxyFile ${dir}/
cat << EOF > ${dir}/job.jdl
executable = ${dir}/condor.sh
universe = vanilla
output   = log/outCondor_\$(Cluster).\$(Process).stdout
error    = log/outCondor_\$(Cluster).\$(Process).stdout
log      = /dev/null
should_transfer_files = yes
initialdir = ${dir}
transfer_input_files = dataset.list, input.tgz, condorPset.py, `basename ${proxyFile}`
EOF
if [ "${ReturnOutput}" == "1" ]; then
	echo "when_to_transfer_output = ON_EXIT" >> ${dir}/job.jdl
	echo "transfer_output_files = output" >> ${dir}/job.jdl
fi

cat << EOF >> ${dir}/job.jdl
arguments = \$(Cluster) \$(Process) ${OutPATH} 
queue $nJob 
EOF
echo "   `cat $dir/dataset.list | wc -l` files / $MaxNFiles (maxFile) = $nJob jobs will be submitted on Condor"
}

function makeDataList() {
echo ""
echo "# Step2 Checking DataList "
listFile=$1
CondorDir=$2
cp $listFile ${CondorDir}/dataset.list
echo "   OK, Found `cat ${CondorDir}/dataset.list | wc -l` files in $DataList"
}


echo ""
echo -n "# Cheking voms proxy "
CheckProxyFile=`voms-proxy-info | wc -l`
if [ "${CheckProxyFile}" == "0" ]; then 
	echo ""
	echo "Do voms-proxy-init -voms cms" 
	exit 
fi
vomsTime=`voms-proxy-info | grep "timeleft" | awk '{print $3}' | cut -f1 -d:`
if [ ${vomsTime} -le 5 ]; then 
	echo ""
	echo "Your proxy time left less then 5 hours"
	echo "You may need voms-proxy-init -voms cms";
	exit
else 
	echo "Ok"
fi
ProxyFile=`voms-proxy-info | grep path | awk '{print $3}'`
#voms-proxy-info

##### makeJob Strat Here
echo -n "# Cheking Your CMSSW "
CurrentDir=`pwd`
dateString="`date +"%y%m%d_%H%M%S"`"
CondorDir="${CMSSW_BASE}/src/Condor/Condor_${dateString}"
if [ ! -d $CondorDir ]; then mkdir -p ${CondorDir}/python; fi
cp ${RunPy} ${CondorDir}/python/
cd ${CondorDir}
scram b -j10 >& /dev/null
st_scramb=$?
if [ ${st_scramb} == "0" ]; then
	echo "OK, Your ${CMSSW_BASE}"
else
	echo ""
	echo -e "\e[0;31m   scram build failed!, Something Wrong Your ${CMSSW_BASE} exit \e[0m"
	cd ${CurrentDir}
	rm -rf ${CondorDir}
	exit
fi

 
cd $CurrentDir
makeCode ${CondorDir}

cd $CurrentDir
if [ $InType == 1 ]; then 
	makeDataList ${DataList} ${CondorDir}
elif [ ${InType} == 2 ]; then
	makeDASList ${DataSet} ${CondorDir}
fi

cd $CurrentDir
makeRun ${RunPy} ${MaxNFiles} ${MaxNEvents} ${CondorDir} ${ProxyFile} ${OutPATH}

if [ "${dataset}" == "" ]; then
	linkDirName=`basename $DataList`
else
	linkDirName="`dirString $dataset`"
fi
linkDirName="Condor_${linkDirName}_${dateString}"
ln -s $CondorDir $linkDirName
bName=`basename $linkDirName`

cp $jobConfigFile ${CondorDir}/jobConfig.txt
echo ""
echo -e "To submit:\e[0;32m  condor_submit ${bName}/job.jdl \e[0m"

if [ "${2}" == "-submit" ]; then
	condor_submit ${bName}/job.jdl
fi

