#!/bin/bash
while true
do
  sleep 30
  while read -r line
  do
    if [ -n "${line}" ]; then
      echo $line
    fi
    sleep 30
  done < ratings.dat >> agent.log
  n=$((n+1))
done
