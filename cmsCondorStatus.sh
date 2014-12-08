#!/bin/bash

dir=$1
totalJobs=""
nJobs=0
nTotalJobs=0
files=`find $dir/log/ -maxdepth 1 -name "*.stdout"`
nRoot=`find $dir/output/ -maxdepth 1 -name "*.root" | wc -l`
nGoodLog=`grep "### END Final cmsRun JobEnd 0 TryNumber" $dir/log/*stdout | wc -l`
for file in $files
do
	((nTotalJobs++))
	bName=`basename $file`
	if [ "`ls -al $file  | awk '{print $5}'`" == "0" ]; then
		name=${bName/outCondor_/}
		name=${name/.stdout/}
		totalJobs="${totalJobs} $name"
		((nJobs++))
		echo "######### CondorJob $name ##########################################################"
		condor_tail -maxbytes 102400 $name
		echo "####################################################################################"
		sleep 2
		echo ""
		echo ""
	fi
done

echo "FinalStatus $nJobs / $nTotalJobs [RooFile $nRoot GoodLog $nGoodLog] RunningJobId : $totalJobs $dir"


