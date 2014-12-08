#!/bin/bash

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

if [ ! -d $HOME/.cmsDAS ]; then mkdir -p $HOME/.cmsDAS; fi

das_client.py --query="dataset dataset=/*13TeV*/*/AODSIM" --limit=0 > $HOME/.cmsDAS/dataset.txt
das_client.py --query="dataset dataset=/*13TeV*/*/MINIAODSIM" --limit=0 >> $HOME/.cmsDAS/dataset.txt

if [ $IsNewCMSSW == "1" ]; then
	rm -rf $DASDir
fi

cat $HOME/.cmsDAS/dataset.txt
