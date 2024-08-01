#!/bin/bash

maxsize=98000000
splitlen=30000
recursivelen=2000

split -l $splitlen -a 3 -d ../bioproject.jsonl ./files/bioproject_jsonl_part_

for i in ./files/*
do
        fsize=$(wc -c <"$i")

        if [ $fsize -ge $maxsize ]; then
                fname="${i}_part_"
                split -l $recursivelen -a 3 -d $i $fname
                rm ${i}
        fi
done
