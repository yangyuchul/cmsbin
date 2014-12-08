#!/bin/bash

function dirString() {
	dataStr=${1//\//_D_}
	echo ${dataStr/_D_/}
}

function printCFG() {
	inputFiles=""
	for inFile in `cat $1 | awk '{print $1}'`
	do
		inputFiles="${inputFiles}${inFile},"
	done
	inputFiles=${inputFiles/%,/}
	echo "inputFiles='$inputFiles' outputFile=$2 >& ${2/.root/.log} &"
}


if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
	echo "$0 CMSDatasSet Option"
	echo "	Option: -summary  : Print Summary Information for the dataset"
	echo "	Option: -site     : Print Sites which have the dataset"
	echo "	Option: -file     : Print all LFN for the dataset"
	echo "	Option: -list     : Print all LFN for the dataset"
	echo "	Option: -small    : Print the Smallest file in the dataset"
	echo "	Option: -cfg      : Print Arguments for cmsRun"
	echo "	Option: -all      : Print All Information"
	exit
fi

FirstDir=$PWD
IsNewCMSSW=0
DASDir=/tmp/cmsDASdir_`date +"%y%m%d%H%M%S"`_${RANDOM}
if [ "$CMSSW_BASE" == "" ]; then
   IsNewCMSSW=1
   mkdir $DASDir
   cd $DASDir
   CMSSW_Version=CMSSW_7_2_0
   export SCRAM_ARCH=slc6_amd64_gcc481
   export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
   source $VO_CMS_SW_DIR/cmsset_default.sh
   scramv1 project CMSSW $CMSSW_Version
   cd $CMSSW_Version/src
   eval `scramv1 runtime -sh`
fi

OptSummary=0
OptSite=0
OptFile=0
OptList=0
OptSmall=0
OptCFG=0
OptAll=1
Datasets=""
for Arg in $@
do
	if [ "${Arg:0:1}" == "-" ]; then
		if [ "${Arg}" == "-summary"  ]; then OptAll=0; OptSummary=1 ;  fi
		if [ "${Arg}" == "-site"     ]; then OptAll=0; OptSite=1    ;  fi
		if [ "${Arg}" == "-file"     ]; then OptAll=0; OptFile=1    ;  fi
		if [ "${Arg}" == "-list"     ]; then OptAll=0; OptList=1    ;  fi
		if [ "${Arg}" == "-small"    ]; then OptAll=0; OptSmall=1   ;  fi
		if [ "${Arg}" == "-cfg"      ]; then OptAll=0; OptCFG=1     ;  fi
		if [ "${Arg}" == "-all"      ]; then OptAll=0; OptAll=1     ;  fi
	else
		Datasets="${Datasets} ${Arg}"
	fi 
done

myTempDir=$HOME/.cmsDAS
if [ ! -d $myTempDir ]; then
	mkdir -p $myTempDir
fi

for dataset in $Datasets
do
	dasString=`dirString $dataset`
	if [ "${dataset:(-4)}" == "USER" ]; then instance="instance=prod/phys03"; fi
	#tempFile="/tmp/cmsDAS_dataset_`date +"%y%m%d_%H%M%S"`_${RANDOM}"
	tempFile="$myTempDir/cmsDAS_${dasString}.das"
	tempSiteFile="$myTempDir/cmsDAS_${dasString}.site"
	listFile=${tempFile/cmsDAS_/cmsDASList_}
	rm -rf $listFile
	if [ ! -f $tempFile ]; then
		echo "Searching... DAS"
		das_client.py --query="file dataset=${dataset} ${instance} | grep file.name, file.nevents, file.size" --limit=0 >& $tempFile
	else 
		ls $tempFile
	fi
	if  [ ! -f $tempSiteFile ] ; then
		echo "Searching ... Sites"
		das_client.py --query="site dataset=${dataset} ${instance}" --limit=0 >& $tempSiteFile
	else 
		ls $tempSiteFile
	fi
	sites=`cat $tempSiteFile`
	sites=${sites//'N/A'/}
	sitesStr=""
	for site in $sites
	do
		sitesStr="${sitesStr} ${site}"
	done
	URL=""
	for site in $sites
	do
		if [ "${site}" == "T3_KR_KISTI" ] || [ "${site}" == "cms-se.sdfarm.kr" ]; then URL="root://cms-xrdr.sdfarm.kr:1094///cms/data/xrd/"; fi
	done
	for site in $sites
	do
		if [ "${site}" == "T2_KR_KNU" ] || [ "${site}" == "cluster142.knu.ac.kr" ]; then URL="root://cluster142.knu.ac.kr/"; fi
	done

	for fileName in `cat $tempFile | awk '{print $1}'`
	do
		echo "${URL}${fileName}" >> ${listFile}
	done


	numFile=`grep ".root" $tempFile | wc -l`

	totalevent=0
	for event in `cat $tempFile | awk '{print $2}'`
	do
		totalevent=`expr $totalevent + $event`
	done

	totalsize=0
	for size in `cat $tempFile | awk '{print $3}'`
	do
		if [ "${size:(-2)}" == "MB" ]; then size=${size/MB}; size=`python -c "print $size * 1024 * 1024"`; fi	
		if [ "${size:(-2)}" == "GB" ]; then size=${size/GB}; size=`python -c "print $size * 1024 * 1024 * 1024"`; fi	
		totalsize=`python -c "print $totalsize + $size"`
	done
	totalsize=`python -c "print $totalsize / 1024.0 / 1024.0 / 1024.0"`
	bc <<< 'scale=5;$totalsize/1024.0/1024.0/1024.0'

	thisSummary=`echo -n "INFO $numFile files $totalevent events $totalsize GB $dataset $sitesStr"`

	smallFile=`cat $tempFile | sort -k 3 -h | head -n 1 | awk '{print $1}'`
	smallSize=`cat $tempFile | sort -k 3 -h | head -n 1 | awk '{print $2}'`
	smallNEvt=`cat $tempFile | sort -k 3 -h | head -n 1 | awk '{print $3}'`
	smallFile="${URL}${smallFile}"
		
	if [ "${OptSummary}" == "1" ]; then  echo $thisSummary; fi
	if [ "${OptSite}" == "1" ]; then  echo "$sitesStr"; fi
	if [ "${OptFile}" == "1" ]; then  cat $tempFile; fi
	if [ "${OptList}" == "1" ]; then  cat $listFile; fi
	if [ "${OptSmall}" == "1" ]; then  echo "$smallFile $smallNEvt $smallSize"; fi
	if [ "${OptCFG}" == "1" ]; then  printCFG $listFile `dirString $dataset`.root; fi
	if [ "${OptAll}" == "1" ];       then            
		echo "### Files"
		cat $tempFile
		echo ""
		echo "### ListOfFiles"
		cat $listFile
		echo ""
		echo "### Smallest File"
		echo "$smallFile $smallNEvt $smallSize"
		echo ""
		#echo "### CFG Argumetns"
		#printCFG $listFile `dirString $dataset`.root
		echo "### Summary"
		echo $thisSummary
		echo ""
	fi

#	rm -rf ${tempFile}
	rm -rf ${listFile}
done

if [ "$IsNewCMSSW" == "1" ]; then
	cd $FirstDir
	rm -rf $DASDir
fi



