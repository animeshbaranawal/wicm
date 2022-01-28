#!/bin/bash
# shellcheck disable=SC2086
# shellcheck disable=SC2027

SCRIPT_HOME="/media/animeshbaranawal/Work/IISc/DREAMLab/Research/wicm/build/polaris_scripts/giraph"
BASE_SCRIPT_HOME="/media/animeshbaranawal/Work/IISc/DREAMLab/Research/wicm/build/polaris_scripts/giraph/Reddit/runEAT.sh"

algos=("EAT")
sources=("22499")

for a in "${algos[@]}"
do
  for s in "${sources[@]}"
  do
#    bash $SCRIPT_HOME/Reddit/icmd/run"$a".sh $s true newGraphTime.txt rmat
#    bash $SCRIPT_HOME/Reddit/wicmd/run"$a".sh $s 100 20 true newGraphTime.txt rmat 0 40 "0;21;23;24;25;26;27;28;29;31;34;40"
    bash $SCRIPT_HOME/Reddit/snap/run"$a".sh 0 "output/window-1" rmat 0 40 "output/window" 40
    bash $SCRIPT_HOME/Reddit/snap/run"$a"_total.sh 0 newGraphTime.txt rmat
#    bash $BASE_SCRIPT_HOME compare rmat_debug rmat_windowed
#    correct=$(echo "$?")
#    if [[ "$correct" == "0" ]]; then
#      rm -r rmat_debug
#      rm -r rmat_mut
#    else
#      echo "$s $a Not equivalent"
#      exit 1
#    fi

#    for w in "${wicm[@]}"
#    do
#      bash $SCRIPT_HOME/run"$a".sh WICM $s 0 36 $w true graph rmat
#      bash $SCRIPT_HOME/run"$a".sh compare rmat_debug rmat_windowed
#      correct=$(echo "$?")
#      if [[ "$correct" == "0" ]]; then
#        rm -r rmat_windowed
#      else
#        echo "$s $a $w Not equivalent"
#        exit 1
#      fi
#    done
#    rm -r rmat_debug
  done
done
