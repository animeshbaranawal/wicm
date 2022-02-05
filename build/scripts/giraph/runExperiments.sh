#!/bin/bash

SCRIPT_HOME="./scripts/giraph"

algos=("EAT" "SSSP" "TR" "TMST" "FAST")

sources=("22499" "19862")
windows="0;20;30;40"

for a in "${algos[@]}"
do
  echo $a
  echo "========================="
  for s in "${sources[@]}"
  do
    echo $s
    echo "--------------------------"
    
    bash $SCRIPT_HOME/icm/run"$a".sh "$s" false sampleGraph.txt ICM # run native ICM
    if [ ! -f ICM/sorted.txt ]; then # check if output dumped
      >&2 echo "Problem in ICM $a $s"
      exit 1
    fi

    # run WICM(I)
    bash $SCRIPT_HOME/wicmi/run"$a".sh "$s" "sampleGraphMutations/window-1" WICMI 0 40 "$windows" "sampleGraphMutations/window" 40
    if [ ! -f WICMI/sorted.txt ]; then # check if output dumped
      >&2 echo "Problem in WICM(I) $a $s"
      exit 1
    else
      bash $SCRIPT_HOME/compare.sh ICM WICMI # compare output
      correct=$(echo "$?")
      if [[ "$correct" == "0" ]]; then
        echo "$s $a ICM WICM(I) Equivalent"
	      rm -r WICMI
      else
        >&2 echo "$s $a ICM WICM(I) Not equivalent"
	      exit 1
      fi
    fi

    rm -r ICM
  done
done
