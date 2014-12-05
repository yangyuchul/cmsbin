#!/bin/bash

function dirString() {
   dataStr=${1//\//_D_}
   echo ${dataStr/_D_/} 
}

DataSet=$1
RunPy=$2
ReturnOutput=1
dirStr=`dirString $DataSet`

if [ "$3" != "" ]; then ReturnOutput=${3}; fi
if [ "$4" != "" ]; then 
#OutPATH="root://cms-xrdr.sdfarm.kr:1094///cms/data/xrd/store/u/ycyang/condor/ntWp/${dirStr}"
	OutPATH=${4}/${dirStr} 
else
	OutPATH=NULL
fi

if [ "${OutPATH}" == "NULL" ]; then ReturnOutput=1; fi

cat << EOF > configJob.txt
DataSet=${DataSet}
RunPy=${RunPy}
ReturnOutput=${ReturnOutput}
OutPATH=${OutPATH}
EOF


