#!/bin/bash

FirstDir=$PWD
IsNewCMSSW=0
NewCheckAAADir=/tmp/CheckAAADir_`date +"%y%m%d_%H%M%S"_${RANDOM}`
if [ "$CMSSW_BASE" == "" ]; then 
	IsNewCMSSW=1
	mkdir $NewCheckAAADir
	cd $NewCheckAAADir
	CMSSW_Version=CMSSW_7_2_1
	echo "Setup New $CMSSW_Version ";	
	export SCRAM_ARCH=slc6_amd64_gcc481
	export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
	source $VO_CMS_SW_DIR/cmsset_default.sh
	scramv1 project CMSSW $CMSSW_Version
	cd $CMSSW_Version/src
	eval `scramv1 runtime -sh`
fi

echo "### Your CMSSW $CMSSW_VERSION $CMSSW_BASE"
CheckProxyFile=`voms-proxy-info | wc -l || grid-proxy-info | wc -l`
if [ "${CheckProxyFile}" == "0" ]; then echo "Do voms-proxy-init -voms cms"; exit; fi

vomsTime=`voms-proxy-info | grep "timeleft" | awk '{print $3}' | cut -f1 -d: || grid-proxy-info | grep "timeleft" | awk '{print $3}' | cut -f1 -d:`
if [ ${vomsTime} -le 5 ]; then
   echo "Your proxy time left less then 5 hours"
   echo "You may need voms-proxy-init -voms cms"
   exit
fi

ProxyFile=`voms-proxy-info | grep path | awk '{print $3}'`

#export XRD_TRANSACTIONTIMEOUT=60
#echo XRD_TRANSACTIONTIMEOUT=$XRD_TRANSACTIONTIMEOUT

dataset=$1

echo "### Searching... $dataset"
if [ "${dataset:(-4)}" == "USER" ]; then instance="instance=prod/phys03"; fi
datafile="/tmp/checkAAA_`date +"%y%m%d_%H%M%S"_${RANDOM}`.list"
das_client.py --query="file dataset=${dataset} ${instance} | grep file.name, file.size, file.nevents" --limit=0 > ${datafile}
file=`cat $datafile | grep .root | sort -k 3 -n | head -n 1 | awk '{print $1}'`
maxN=`cat $datafile | grep .root | sort -k 3 -n | head -n 1 | awk '{print $3}'`
runMaxN=$maxN
if [ $maxN -gt 3 ]; then runMaxN=3; fi
rm -rf $datafile

sites=`das_client.py --query="site dataset=${dataset} ${instance}" --limit=0`
sites=${sites//'N/A'/}
sitesStr=""
for site in $sites
do
	sitesStr="$sitesStr $site"
done

echo "	--> TestFile $runMaxN $file $maxN on $sitesStr"

isT3_KR_KISTI=`echo $sites | grep -E "cms-se.sdfarm.kr|T3_KR_KISTI" | wc -l`
isT2_KR_KNU=`echo $sites | grep -E "cluster142.knu.ac.kr|T2_KR_KNU" | wc -l`
if [ "$isT3_KR_KISTI" != "0" ]; then	
	file="root://cms-xrdr.sdfarm.kr:1094//cms/data/xrd${file}"
elif [ "$isT2_KR_KNU" != "0" ]; then
	file="root://cluster142.knu.ac.kr/${file}" 
fi

tempFileName="/tmp/checkAAA_`date +"%y%m%d_%H%M%S"_${RANDOM}`.py"
cat << EOF > $tempFileName
import FWCore.ParameterSet.Config as cms
process = cms.Process("CheckEDMRead")
process.load("FWCore.MessageService.MessageLogger_cfi")
process.maxEvents = cms.untracked.PSet( input = cms.untracked.int32(${runMaxN}) )
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring('${file}'))
EOF

tryN=0
for index in `seq 1 10`
do
	tryN=$index
	echo "### cmsRun $tryN $dataset $file"
	cmsRun ${tempFileName}
	JobEnd=$?
	echo "### ThisResult cmsRun $JobEnd $tryN $dataset $file"
	if [ "$JobEnd" == "0" ]; then break; fi
done

echo "### Result: JobEnd $JobEnd TryNumber $tryN Dataset $dataset File $file SITES $sitesStr"
rm -rf ${tempFileName}
if [ "$IsNewCMSSW" == "1" ]; then
	rm -rf $NewCheckAAADir
fi

cd $FirstDir


