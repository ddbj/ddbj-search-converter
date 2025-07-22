set -euxo pipefail

# === Query 1 ===
echo "=== Running Query 1: Search for JGAS000284 and its secondary ID ==="
RESPONSE_1=$(curl -fsSL -X POST "http://ddbj-search-elasticsearch:9200/jga-study/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "bool": {
        "should": [
          { "match": { "identifier": "JGAS000284" } },
          { "match": { "properties.IDENTIFIERS.SECONDARY_ID": "JGAS000284" } }
        ],
        "minimum_should_match": 1
      }
    },
    "_source": ["identifier"],
    "from": 0,
    "size": 10000
  }')
HIT_COUNT_1=$(echo "$RESPONSE_1" | jq '.hits.total.value')

if [ "$HIT_COUNT_1" -gt 0 ]; then
  echo "[OK] Query 1 returned $HIT_COUNT_1 hits."
else
  echo "[ERROR] Query 1 returned no hits."
  exit 1
fi

# === Query 2 ===
echo "=== Running Query 2: Search for JGAS000284 and return specific fields ==="
RESPONSE_2=$(curl -fsSL -X POST "http://ddbj-search-elasticsearch:9200/jga-study/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": { "identifier": "JGAS000284" }
    },
    "_source": ["identifier", "title", "dbXref", "properties"],
    "from": 0,
    "size": 10000
  }')
HIT_COUNT_2=$(echo "$RESPONSE_2" | jq '.hits.total.value')

if [ "$HIT_COUNT_2" -gt 0 ]; then
  echo "[OK] Query 2 returned $HIT_COUNT_2 hits."
else
  echo "[ERROR] Query 2 returned no hits."
  exit 1
fi

# === Query 3 ===
echo "=== Running Query 3: Search for hum0267 in properties.STUDY_ATTRIBUTES.STUDY_ATTRIBUTE.VALUE and NBDC Number in properties.STUDY_ATTRIBUTES.TAG ==="
RESPONSE_3=$(curl -fsSL -X POST "http://ddbj-search-elasticsearch:9200/jga-study/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "bool": {
        "must": [
          { "match": { "properties.STUDY_ATTRIBUTES.STUDY_ATTRIBUTE.VALUE": "hum0267" } },
          { "match": { "properties.STUDY_ATTRIBUTES.STUDY_ATTRIBUTE.TAG": "NBDC Number" } }
        ]
      }
    },
    "_source": ["identifier", "title", "dbXref", "properties"],
    "from": 0,
    "size": 10000
  }')
HIT_COUNT_3=$(echo "$RESPONSE_3" | jq '.hits.total.value')

if [ "$HIT_COUNT_3" -gt 0 ]; then
  echo "[OK] Query 3 returned $HIT_COUNT_3 hits."
else
  echo "[ERROR] Query 3 returned no hits."
  exit 1
fi

# === Query 4 ===
echo "=== Running Query 4: Search for JGAS000284 (JGAD000390) in dbXref.identifier ==="
RESPONSE_4=$(curl -fsSL -X POST "http://ddbj-search-elasticsearch:9200/jga-study/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "nested": {
        "path": "dbXref",
        "query": {
          "term": { "dbXref.identifier": "JGAD000390" }
        }
      }
    },
    "_source": ["identifier", "title", "dbXref", "properties"],
    "from": 0,
    "size": 10000
  }')
HIT_COUNT_4=$(echo "$RESPONSE_4" | jq '.hits.total.value')

if [ "$HIT_COUNT_4" -gt 0 ]; then
  echo "[OK] Query 4 returned $HIT_COUNT_4 hits."
else
  echo "[ERROR] Query 4 returned no hits."
  exit 1
fi

# === Query 5 ===
echo "=== Running Query 5: Search for JGAD000390 ==="
RESPONSE_5=$(curl -fsSL -X POST "http://ddbj-search-elasticsearch:9200/jga-dataset/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": { "identifier": "JGAD000390" }
    },
    "_source": ["identifier", "title"],
    "from": 0,
    "size": 10000
  }')
HIT_COUNT_5=$(echo "$RESPONSE_5" | jq '.hits.total.value')

if [ "$HIT_COUNT_5" -gt 0 ]; then
  echo "[OK] Query 5 returned $HIT_COUNT_5 hits."
else
  echo "[ERROR] Query 5 returned no hits."
  exit 1
fi
