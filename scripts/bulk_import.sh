#!/bin/env bash

HEAD="Content-Type:application/x-ndjson"
URL="localhost:9200/_bulk"

for i in $(ls bs_1_*.jsonl); do
    # echo "curl -s -H $HEAD -XPOST $URL --data-binary \"@$i\""
    curl -s -H $HEAD -XPOST $URL --data-binary "@$i" >logs/$i.logs
done
