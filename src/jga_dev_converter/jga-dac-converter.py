from lxml import etree
import xml.etree.cElementTree as cElementTree
import datetime
import xmltodict
import requests
import argparse
import os
import json
from typing import NewType, List


FilePath = NewType('FilePath', str)

batch_size = 1


def xml2json(input:FilePath):
    context = etree.iterparse(input, tag="DATASET")
    docs:list[dict] = []
    i = 0
    for events, element in context:
        if element.tag == "DAC":
            doc = {}
            doc["accession"] = "JGAC000001"
            xml_str = etree.tostring(element)
            metadata = xmltodict.parse(xml_str, attr_prefix="", cdata_key="content")
            doc["properties"] = metadata["DAC"]
            doc["identifier"]= "JGAC000001"
            doc["type"] = "jga-dac"
            doc["url"] = "https://ddbj.nig.ac.jp/search/resource/jga-dac/JGAC000001"
            doc["isPartOf"] = "jga"
            doc["organism"] = {"identifier": 9606, "name": "Homo sapiens"}
            doc["distribution"] = [
                {
                    "contentUrl": "https://ddbj.nig.ac.jp/search/resource/jga-dac/JGAC000001.json",
                    "encodingFormat": "JSON",
                    "type": "DataDownload"
                },
                {
                    "contentUrl": "https://ddbj.nig.ac.jp/search/resource/jga-dac/JGAC000001.jsonld",
                    "encodingFormat": "JSON-LD",
                    "type": "DataDownload"
                }
                            ]
            doc["name"] = "JGAC000001"
            doc["dbXrefs"] = [
                            {
                    "identifier": "JGAS000001",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000001"
                },
                {
                    "identifier": "JGAS000002",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000002"
                },
                {
                    "identifier": "JGAS000004",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000004"
                },
                {
                    "identifier": "JGAS000005",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000005"
                },
                {
                    "identifier": "JGAS000006",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000006"
                },
                {
                    "identifier": "JGAS000007",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000007"
                },
                {
                    "identifier": "JGAS000008",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000008"
                },
                {
                    "identifier": "JGAS000009",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000009"
                },
                {
                    "identifier": "JGAS000011",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000011"
                },
                {
                    "identifier": "JGAS000012",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000012"
                },
                {
                    "identifier": "JGAS000013",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000013"
                },
                {
                    "identifier": "JGAS000014",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000014"
                },
                {
                    "identifier": "JGAS000015",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000015"
                },
                {
                    "identifier": "JGAS000016",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000016"
                },
                {
                    "identifier": "JGAS000017",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000017"
                },
                {
                    "identifier": "JGAS000018",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000018"
                },
                {
                    "identifier": "JGAS000021",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000021"
                },
                {
                    "identifier": "JGAS000022",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000022"
                },
                {
                    "identifier": "JGAS000023",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000023"
                },
                {
                    "identifier": "JGAS000024",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000024"
                },
                {
                    "identifier": "JGAS000025",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000025"
                },
                {
                    "identifier": "JGAS000026",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000026"
                },
                {
                    "identifier": "JGAS000027",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000027"
                },
                {
                    "identifier": "JGAS000028",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000028"
                },
                {
                    "identifier": "JGAS000032",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000032"
                },
                {
                    "identifier": "JGAS000035",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000035"
                },
                {
                    "identifier": "JGAS000036",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000036"
                },
                {
                    "identifier": "JGAS000037",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000037"
                },
                {
                    "identifier": "JGAS000038",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000038"
                },
                {
                    "identifier": "JGAS000039",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000039"
                },
                {
                    "identifier": "JGAS000040",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000040"
                },
                {
                    "identifier": "JGAS000041",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000041"
                },
                {
                    "identifier": "JGAS000042",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000042"
                },
                {
                    "identifier": "JGAS000043",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000043"
                },
                {
                    "identifier": "JGAS000044",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000044"
                },
                {
                    "identifier": "JGAS000045",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000045"
                },
                {
                    "identifier": "JGAS000046",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000046"
                },
                {
                    "identifier": "JGAS000047",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000047"
                },
                {
                    "identifier": "JGAS000051",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000051"
                },
                {
                    "identifier": "JGAS000052",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000052"
                },
                {
                    "identifier": "JGAS000053",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000053"
                },
                {
                    "identifier": "JGAS000057",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000057"
                },
                {
                    "identifier": "JGAS000059",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000059"
                },
                {
                    "identifier": "JGAS000060",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000060"
                },
                {
                    "identifier": "JGAS000061",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000061"
                },
                {
                    "identifier": "JGAS000062",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000062"
                },
                {
                    "identifier": "JGAS000064",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000064"
                },
                {
                    "identifier": "JGAS000065",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000065"
                },
                {
                    "identifier": "JGAS000068",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000068"
                },
                {
                    "identifier": "JGAS000070",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000070"
                },
                {
                    "identifier": "JGAS000073",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000073"
                },
                {
                    "identifier": "JGAS000075",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000075"
                },
                {
                    "identifier": "JGAS000076",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000076"
                },
                {
                    "identifier": "JGAS000077",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000077"
                },
                {
                    "identifier": "JGAS000078",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000078"
                },
                {
                    "identifier": "JGAS000079",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000079"
                },
                {
                    "identifier": "JGAS000080",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000080"
                },
                {
                    "identifier": "JGAS000081",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000081"
                },
                {
                    "identifier": "JGAS000082",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000082"
                },
                {
                    "identifier": "JGAS000085",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000085"
                },
                {
                    "identifier": "JGAS000086",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000086"
                },
                {
                    "identifier": "JGAS000087",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000087"
                },
                {
                    "identifier": "JGAS000088",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000088"
                },
                {
                    "identifier": "JGAS000089",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000089"
                },
                {
                    "identifier": "JGAS000090",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000090"
                },
                {
                    "identifier": "JGAS000091",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000091"
                },
                {
                    "identifier": "JGAS000092",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000092"
                },
                {
                    "identifier": "JGAS000093",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000093"
                },
                {
                    "identifier": "JGAS000094",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000094"
                },
                {
                    "identifier": "JGAS000095",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000095"
                },
                {
                    "identifier": "JGAS000096",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000096"
                },
                {
                    "identifier": "JGAS000098",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000098"
                },
                {
                    "identifier": "JGAS000099",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000099"
                },
                {
                    "identifier": "JGAS000101",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000101"
                },
                {
                    "identifier": "JGAS000102",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000102"
                },
                {
                    "identifier": "JGAS000103",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000103"
                },
                {
                    "identifier": "JGAS000104",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000104"
                },
                {
                    "identifier": "JGAS000105",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000105"
                },
                {
                    "identifier": "JGAS000106",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000106"
                },
                {
                    "identifier": "JGAS000107",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000107"
                },
                {
                    "identifier": "JGAS000108",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000108"
                },
                {
                    "identifier": "JGAS000109",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000109"
                },
                {
                    "identifier": "JGAS000111",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000111"
                },
                {
                    "identifier": "JGAS000112",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000112"
                },
                {
                    "identifier": "JGAS000113",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000113"
                },
                {
                    "identifier": "JGAS000114",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000114"
                },
                {
                    "identifier": "JGAS000115",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000115"
                },
                {
                    "identifier": "JGAS000119",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000119"
                },
                {
                    "identifier": "JGAS000120",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000120"
                },
                {
                    "identifier": "JGAS000121",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000121"
                },
                {
                    "identifier": "JGAS000122",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000122"
                },
                {
                    "identifier": "JGAS000123",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000123"
                },
                {
                    "identifier": "JGAS000124",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000124"
                },
                {
                    "identifier": "JGAS000125",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000125"
                },
                {
                    "identifier": "JGAS000126",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000126"
                },
                {
                    "identifier": "JGAS000127",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000127"
                },
                {
                    "identifier": "JGAS000128",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000128"
                },
                {
                    "identifier": "JGAS000129",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000129"
                },
                {
                    "identifier": "JGAS000130",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000130"
                },
                {
                    "identifier": "JGAS000131",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000131"
                },
                {
                    "identifier": "JGAS000132",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000132"
                },
                {
                    "identifier": "JGAS000134",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000134"
                },
                {
                    "identifier": "JGAS000135",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000135"
                },
                {
                    "identifier": "JGAS000136",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000136"
                },
                {
                    "identifier": "JGAS000137",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000137"
                },
                {
                    "identifier": "JGAS000138",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000138"
                },
                {
                    "identifier": "JGAS000139",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000139"
                },
                {
                    "identifier": "JGAS000140",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000140"
                },
                {
                    "identifier": "JGAS000141",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000141"
                },
                {
                    "identifier": "JGAS000142",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000142"
                },
                {
                    "identifier": "JGAS000144",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000144"
                },
                {
                    "identifier": "JGAS000145",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000145"
                },
                {
                    "identifier": "JGAS000146",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000146"
                },
                {
                    "identifier": "JGAS000147",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000147"
                },
                {
                    "identifier": "JGAS000149",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000149"
                },
                {
                    "identifier": "JGAS000150",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000150"
                },
                {
                    "identifier": "JGAS000151",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000151"
                },
                {
                    "identifier": "JGAS000153",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000153"
                },
                {
                    "identifier": "JGAS000155",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000155"
                },
                {
                    "identifier": "JGAS000156",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000156"
                },
                {
                    "identifier": "JGAS000157",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000157"
                },
                {
                    "identifier": "JGAS000158",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000158"
                },
                {
                    "identifier": "JGAS000159",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000159"
                },
                {
                    "identifier": "JGAS000160",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000160"
                },
                {
                    "identifier": "JGAS000162",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000162"
                },
                {
                    "identifier": "JGAS000164",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000164"
                },
                {
                    "identifier": "JGAS000165",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000165"
                },
                {
                    "identifier": "JGAS000166",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000166"
                },
                {
                    "identifier": "JGAS000169",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000169"
                },
                {
                    "identifier": "JGAS000170",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000170"
                },
                {
                    "identifier": "JGAS000171",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000171"
                },
                {
                    "identifier": "JGAS000172",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000172"
                },
                {
                    "identifier": "JGAS000173",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000173"
                },
                {
                    "identifier": "JGAS000174",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000174"
                },
                {
                    "identifier": "JGAS000176",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000176"
                },
                {
                    "identifier": "JGAS000177",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000177"
                },
                {
                    "identifier": "JGAS000178",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000178"
                },
                {
                    "identifier": "JGAS000179",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000179"
                },
                {
                    "identifier": "JGAS000180",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000180"
                },
                {
                    "identifier": "JGAS000182",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000182"
                },
                {
                    "identifier": "JGAS000186",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000186"
                },
                {
                    "identifier": "JGAS000188",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000188"
                },
                {
                    "identifier": "JGAS000189",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000189"
                },
                {
                    "identifier": "JGAS000190",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000190"
                },
                {
                    "identifier": "JGAS000191",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000191"
                },
                {
                    "identifier": "JGAS000192",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000192"
                },
                {
                    "identifier": "JGAS000194",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000194"
                },
                {
                    "identifier": "JGAS000196",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000196"
                },
                {
                    "identifier": "JGAS000197",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000197"
                },
                {
                    "identifier": "JGAS000198",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000198"
                },
                {
                    "identifier": "JGAS000199",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000199"
                },
                {
                    "identifier": "JGAS000200",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000200"
                },
                {
                    "identifier": "JGAS000201",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000201"
                },
                {
                    "identifier": "JGAS000202",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000202"
                },
                {
                    "identifier": "JGAS000203",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000203"
                },
                {
                    "identifier": "JGAS000204",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000204"
                },
                {
                    "identifier": "JGAS000205",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000205"
                },
                {
                    "identifier": "JGAS000206",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000206"
                },
                {
                    "identifier": "JGAS000207",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000207"
                },
                {
                    "identifier": "JGAS000211",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000211"
                },
                {
                    "identifier": "JGAS000214",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000214"
                },
                {
                    "identifier": "JGAS000215",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000215"
                },
                {
                    "identifier": "JGAS000216",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000216"
                },
                {
                    "identifier": "JGAS000217",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000217"
                },
                {
                    "identifier": "JGAS000218",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000218"
                },
                {
                    "identifier": "JGAS000220",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000220"
                },
                {
                    "identifier": "JGAS000221",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000221"
                },
                {
                    "identifier": "JGAS000222",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000222"
                },
                {
                    "identifier": "JGAS000223",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000223"
                },
                {
                    "identifier": "JGAS000224",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000224"
                },
                {
                    "identifier": "JGAS000225",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000225"
                },
                {
                    "identifier": "JGAS000226",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000226"
                },
                {
                    "identifier": "JGAS000227",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000227"
                },
                {
                    "identifier": "JGAS000228",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000228"
                },
                {
                    "identifier": "JGAS000229",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000229"
                },
                {
                    "identifier": "JGAS000230",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000230"
                },
                {
                    "identifier": "JGAS000231",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000231"
                },
                {
                    "identifier": "JGAS000232",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000232"
                },
                {
                    "identifier": "JGAS000233",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000233"
                },
                {
                    "identifier": "JGAS000234",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000234"
                },
                {
                    "identifier": "JGAS000236",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000236"
                },
                {
                    "identifier": "JGAS000237",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000237"
                },
                {
                    "identifier": "JGAS000238",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000238"
                },
                {
                    "identifier": "JGAS000239",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000239"
                },
                {
                    "identifier": "JGAS000240",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000240"
                },
                {
                    "identifier": "JGAS000241",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000241"
                },
                {
                    "identifier": "JGAS000242",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000242"
                },
                {
                    "identifier": "JGAS000243",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000243"
                },
                {
                    "identifier": "JGAS000244",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000244"
                },
                {
                    "identifier": "JGAS000245",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000245"
                },
                {
                    "identifier": "JGAS000246",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000246"
                },
                {
                    "identifier": "JGAS000248",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000248"
                },
                {
                    "identifier": "JGAS000249",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000249"
                },
                {
                    "identifier": "JGAS000253",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000253"
                },
                {
                    "identifier": "JGAS000256",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000256"
                },
                {
                    "identifier": "JGAS000257",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000257"
                },
                {
                    "identifier": "JGAS000258",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000258"
                },
                {
                    "identifier": "JGAS000259",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000259"
                },
                {
                    "identifier": "JGAS000260",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000260"
                },
                {
                    "identifier": "JGAS000262",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000262"
                },
                {
                    "identifier": "JGAS000263",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000263"
                },
                {
                    "identifier": "JGAS000264",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000264"
                },
                {
                    "identifier": "JGAS000265",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000265"
                },
                {
                    "identifier": "JGAS000267",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000267"
                },
                {
                    "identifier": "JGAS000269",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000269"
                },
                {
                    "identifier": "JGAS000270",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000270"
                },
                {
                    "identifier": "JGAS000272",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000272"
                },
                {
                    "identifier": "JGAS000273",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000273"
                },
                {
                    "identifier": "JGAS000274",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000274"
                },
                {
                    "identifier": "JGAS000276",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000276"
                },
                {
                    "identifier": "JGAS000277",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000277"
                },
                {
                    "identifier": "JGAS000278",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000278"
                },
                {
                    "identifier": "JGAS000279",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000279"
                },
                {
                    "identifier": "JGAS000280",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000280"
                },
                {
                    "identifier": "JGAS000281",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000281"
                },
                {
                    "identifier": "JGAS000282",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000282"
                },
                {
                    "identifier": "JGAS000285",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000285"
                },
                {
                    "identifier": "JGAS000286",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000286"
                },
                {
                    "identifier": "JGAS000288",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000288"
                },
                {
                    "identifier": "JGAS000289",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000289"
                },
                {
                    "identifier": "JGAS000290",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000290"
                },
                {
                    "identifier": "JGAS000292",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000292"
                },
                {
                    "identifier": "JGAS000293",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000293"
                },
                {
                    "identifier": "JGAS000294",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000294"
                },
                {
                    "identifier": "JGAS000295",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000295"
                },
                {
                    "identifier": "JGAS000296",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000296"
                },
                {
                    "identifier": "JGAS000297",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000297"
                },
                {
                    "identifier": "JGAS000300",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000300"
                },
                {
                    "identifier": "JGAS000301",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000301"
                },
                {
                    "identifier": "JGAS000303",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000303"
                },
                {
                    "identifier": "JGAS000304",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000304"
                },
                {
                    "identifier": "JGAS000305",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000305"
                },
                {
                    "identifier": "JGAS000307",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000307"
                },
                {
                    "identifier": "JGAS000308",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000308"
                },
                {
                    "identifier": "JGAS000309",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000309"
                },
                {
                    "identifier": "JGAS000310",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000310"
                },
                {
                    "identifier": "JGAS000311",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000311"
                },
                {
                    "identifier": "JGAS000312",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000312"
                },
                {
                    "identifier": "JGAS000313",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000313"
                },
                {
                    "identifier": "JGAS000314",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000314"
                },
                {
                    "identifier": "JGAS000316",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000316"
                },
                {
                    "identifier": "JGAS000317",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000317"
                },
                {
                    "identifier": "JGAS000318",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000318"
                },
                {
                    "identifier": "JGAS000320",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000320"
                },
                {
                    "identifier": "JGAS000321",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000321"
                },
                {
                    "identifier": "JGAS000323",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000323"
                },
                {
                    "identifier": "JGAS000325",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000325"
                },
                {
                    "identifier": "JGAS000327",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000327"
                },
                {
                    "identifier": "JGAS000330",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000330"
                },
                {
                    "identifier": "JGAS000335",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000335"
                },
                {
                    "identifier": "JGAS000341",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000341"
                },
                {
                    "identifier": "JGAS000346",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000346"
                },
                {
                    "identifier": "JGAS000347",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000347"
                },
                {
                    "identifier": "JGAS000348",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000348"
                },
                {
                    "identifier": "JGAS000349",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000349"
                },
                {
                    "identifier": "JGAS000350",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000350"
                },
                {
                    "identifier": "JGAS000359",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000359"
                },
                {
                    "identifier": "JGAS000360",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000360"
                },
                {
                    "identifier": "JGAS000361",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000361"
                },
                {
                    "identifier": "JGAS000367",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000367"
                },
                {
                    "identifier": "JGAS000368",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000368"
                },
                {
                    "identifier": "JGAS000370",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000370"
                },
                {
                    "identifier": "JGAS000377",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000377"
                },
                {
                    "identifier": "JGAS000378",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000378"
                },
                {
                    "identifier": "JGAS000379",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000379"
                },
                {
                    "identifier": "JGAS000381",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000381"
                },
                {
                    "identifier": "JGAS000382",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000382"
                },
                {
                    "identifier": "JGAS000383",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000383"
                },
                {
                    "identifier": "JGAS000385",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000385"
                },
                {
                    "identifier": "JGAS000387",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000387"
                },
                {
                    "identifier": "JGAS000389",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000389"
                },
                {
                    "identifier": "JGAS000412",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000412"
                },
                {
                    "identifier": "JGAS000413",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000413"
                },
                {
                    "identifier": "JGAS000414",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000414"
                },
                {
                    "identifier": "JGAS000415",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000415"
                },
                {
                    "identifier": "JGAS000416",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000416"
                },
                {
                    "identifier": "JGAS000417",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000417"
                },
                {
                    "identifier": "JGAS000418",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000418"
                },
                {
                    "identifier": "JGAS000454",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000454"
                },
                {
                    "identifier": "JGAS000480",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000480"
                },
                {
                    "identifier": "JGAS000482",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000482"
                },
                {
                    "identifier": "JGAS000483",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000483"
                },
                {
                    "identifier": "JGAS000485",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000485"
                },
                {
                    "identifier": "JGAS000486",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000486"
                },
                {
                    "identifier": "JGAS000487",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000487"
                },
                {
                    "identifier": "JGAS000488",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000488"
                },
                {
                    "identifier": "JGAS000489",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000489"
                },
                {
                    "identifier": "JGAS000490",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000490"
                },
                {
                    "identifier": "JGAS000504",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000504"
                },
                {
                    "identifier": "JGAS000505",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000505"
                },
                {
                    "identifier": "JGAS000507",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000507"
                },
                {
                    "identifier": "JGAS000508",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000508"
                },
                {
                    "identifier": "JGAS000510",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000510"
                },
                {
                    "identifier": "JGAS000514",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000514"
                },
                {
                    "identifier": "JGAS000515",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000515"
                },
                {
                    "identifier": "JGAS000516",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000516"
                },
                {
                    "identifier": "JGAS000520",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000520"
                },
                {
                    "identifier": "JGAS000522",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000522"
                },
                {
                    "identifier": "JGAS000523",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000523"
                },
                {
                    "identifier": "JGAS000528",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000528"
                },
                {
                    "identifier": "JGAS000529",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000529"
                },
                {
                    "identifier": "JGAS000530",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000530"
                },
                {
                    "identifier": "JGAS000531",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000531"
                },
                {
                    "identifier": "JGAS000532",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000532"
                },
                {
                    "identifier": "JGAS000533",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000533"
                },
                {
                    "identifier": "JGAS000534",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000534"
                },
                {
                    "identifier": "JGAS000535",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000535"
                },
                {
                    "identifier": "JGAS000537",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000537"
                },
                {
                    "identifier": "JGAS000538",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000538"
                },
                {
                    "identifier": "JGAS000540",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000540"
                },
                {
                    "identifier": "JGAS000541",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000541"
                },
                {
                    "identifier": "JGAS000543",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000543"
                },
                {
                    "identifier": "JGAS000544",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000544"
                },
                {
                    "identifier": "JGAS000545",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000545"
                },
                {
                    "identifier": "JGAS000546",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000546"
                },
                {
                    "identifier": "JGAS000548",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000548"
                },
                {
                    "identifier": "JGAS000549",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000549"
                },
                {
                    "identifier": "JGAS000550",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000550"
                },
                {
                    "identifier": "JGAS000551",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000551"
                },
                {
                    "identifier": "JGAS000553",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000553"
                },
                {
                    "identifier": "JGAS000557",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000557"
                },
                {
                    "identifier": "JGAS000558",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000558"
                },
                {
                    "identifier": "JGAS000559",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000559"
                },
                {
                    "identifier": "JGAS000560",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000560"
                },
                {
                    "identifier": "JGAS000561",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000561"
                },
                {
                    "identifier": "JGAS000562",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000562"
                },
                {
                    "identifier": "JGAS000563",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000563"
                },
                {
                    "identifier": "JGAS000564",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000564"
                },
                {
                    "identifier": "JGAS000565",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000565"
                },
                {
                    "identifier": "JGAS000566",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000566"
                },
                {
                    "identifier": "JGAS000567",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000567"
                },
                {
                    "identifier": "JGAS000568",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000568"
                },
                {
                    "identifier": "JGAS000569",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000569"
                },
                {
                    "identifier": "JGAS000570",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000570"
                },
                {
                    "identifier": "JGAS000572",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000572"
                },
                {
                    "identifier": "JGAS000573",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000573"
                },
                {
                    "identifier": "JGAS000574",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000574"
                },
                {
                    "identifier": "JGAS000575",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000575"
                },
                {
                    "identifier": "JGAS000578",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000578"
                },
                {
                    "identifier": "JGAS000579",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000579"
                },
                {
                    "identifier": "JGAS000580",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000580"
                },
                {
                    "identifier": "JGAS000582",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000582"
                },
                {
                    "identifier": "JGAS000585",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000585"
                },
                {
                    "identifier": "JGAS000586",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000586"
                },
                {
                    "identifier": "JGAS000587",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000587"
                },
                {
                    "identifier": "JGAS000588",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000588"
                },
                {
                    "identifier": "JGAS000591",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000591"
                },
                {
                    "identifier": "JGAS000592",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000592"
                },
                {
                    "identifier": "JGAS000593",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000593"
                },
                {
                    "identifier": "JGAS000596",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000596"
                },
                {
                    "identifier": "JGAS000597",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000597"
                },
                {
                    "identifier": "JGAS000598",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000598"
                },
                {
                    "identifier": "JGAS000599",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000599"
                },
                {
                    "identifier": "JGAS000600",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000600"
                },
                {
                    "identifier": "JGAS000602",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000602"
                },
                {
                    "identifier": "JGAS000603",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000603"
                },
                {
                    "identifier": "JGAS000604",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000604"
                },
                {
                    "identifier": "JGAS000605",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000605"
                },
                {
                    "identifier": "JGAS000609",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000609"
                },
                {
                    "identifier": "JGAS000610",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000610"
                },
                {
                    "identifier": "JGAS000611",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000611"
                },
                {
                    "identifier": "JGAS000613",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000613"
                },
                {
                    "identifier": "JGAS000616",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000616"
                },
                {
                    "identifier": "JGAS000617",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000617"
                },
                {
                    "identifier": "JGAS000618",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000618"
                },
                {
                    "identifier": "JGAS000619",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000619"
                },
                {
                    "identifier": "JGAS000621",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000621"
                },
                {
                    "identifier": "JGAS000622",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000622"
                },
                {
                    "identifier": "JGAS000623",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000623"
                },
                {
                    "identifier": "JGAS000625",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000625"
                },
                {
                    "identifier": "JGAS000626",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000626"
                },
                {
                    "identifier": "JGAS000627",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000627"
                },
                {
                    "identifier": "JGAS000628",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000628"
                },
                {
                    "identifier": "JGAS000638",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000638"
                },
                {
                    "identifier": "JGAS000640",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000640"
                },
                {
                    "identifier": "JGAS000641",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000641"
                },
                {
                    "identifier": "JGAS000642",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000642"
                },
                {
                    "identifier": "JGAS000643",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000643"
                },
                {
                    "identifier": "JGAS000645",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000645"
                },
                {
                    "identifier": "JGAS000646",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000646"
                },
                {
                    "identifier": "JGAS000647",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000647"
                },
                {
                    "identifier": "JGAS000648",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000648"
                },
                {
                    "identifier": "JGAS000651",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000651"
                },
                {
                    "identifier": "JGAS000653",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000653"
                },
                {
                    "identifier": "JGAS000657",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000657"
                },
                {
                    "identifier": "JGAS000659",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000659"
                },
                {
                    "identifier": "JGAS000661",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000661"
                },
                {
                    "identifier": "JGAS000664",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000664"
                },
                {
                    "identifier": "JGAS000665",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000665"
                },
                {
                    "identifier": "JGAS000668",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000668"
                },
                {
                    "identifier": "JGAS000671",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000671"
                },
                {
                    "identifier": "JGAS000672",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000672"
                },
                {
                    "identifier": "JGAS000677",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000677"
                },
                {
                    "identifier": "JGAS000678",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000678"
                },
                {
                    "identifier": "JGAS000679",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000679"
                },
                {
                    "identifier": "JGAS000680",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000680"
                },
                {
                    "identifier": "JGAS000681",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000681"
                },
                {
                    "identifier": "JGAS000683",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000683"
                },
                {
                    "identifier": "JGAS000684",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000684"
                },
                {
                    "identifier": "JGAS000689",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000689"
                },
                {
                    "identifier": "JGAS000690",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000690"
                },
                {
                    "identifier": "JGAS000698",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000698"
                },
                {
                    "identifier": "JGAS000699",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000699"
                },
                {
                    "identifier": "JGAS000700",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000700"
                },
                {
                    "identifier": "JGAS000701",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000701"
                },
                {
                    "identifier": "JGAS000703",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000703"
                },
                {
                    "identifier": "JGAS000707",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000707"
                },
                {
                    "identifier": "JGAS000708",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000708"
                },
                {
                    "identifier": "JGAS000712",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000712"
                },
                {
                    "identifier": "JGAS000718",
                    "type": "jga-study",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-study/JGAS000718"
                },
                {
                    "identifier": "JGAD000001",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000001"
                },
                {
                    "identifier": "JGAD000002",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000002"
                },
                {
                    "identifier": "JGAD000004",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000004"
                },
                {
                    "identifier": "JGAD000005",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000005"
                },
                {
                    "identifier": "JGAD000006",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000006"
                },
                {
                    "identifier": "JGAD000007",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000007"
                },
                {
                    "identifier": "JGAD000008",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000008"
                },
                {
                    "identifier": "JGAD000009",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000009"
                },
                {
                    "identifier": "JGAD000011",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000011"
                },
                {
                    "identifier": "JGAD000012",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000012"
                },
                {
                    "identifier": "JGAD000013",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000013"
                },
                {
                    "identifier": "JGAD000014",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000014"
                },
                {
                    "identifier": "JGAD000015",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000015"
                },
                {
                    "identifier": "JGAD000016",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000016"
                },
                {
                    "identifier": "JGAD000017",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000017"
                },
                {
                    "identifier": "JGAD000018",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000018"
                },
                {
                    "identifier": "JGAD000021",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000021"
                },
                {
                    "identifier": "JGAD000022",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000022"
                },
                {
                    "identifier": "JGAD000023",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000023"
                },
                {
                    "identifier": "JGAD000024",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000024"
                },
                {
                    "identifier": "JGAD000025",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000025"
                },
                {
                    "identifier": "JGAD000026",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000026"
                },
                {
                    "identifier": "JGAD000027",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000027"
                },
                {
                    "identifier": "JGAD000028",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000028"
                },
                {
                    "identifier": "JGAD000032",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000032"
                },
                {
                    "identifier": "JGAD000035",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000035"
                },
                {
                    "identifier": "JGAD000036",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000036"
                },
                {
                    "identifier": "JGAD000037",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000037"
                },
                {
                    "identifier": "JGAD000038",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000038"
                },
                {
                    "identifier": "JGAD000039",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000039"
                },
                {
                    "identifier": "JGAD000040",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000040"
                },
                {
                    "identifier": "JGAD000041",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000041"
                },
                {
                    "identifier": "JGAD000042",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000042"
                },
                {
                    "identifier": "JGAD000043",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000043"
                },
                {
                    "identifier": "JGAD000044",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000044"
                },
                {
                    "identifier": "JGAD000045",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000045"
                },
                {
                    "identifier": "JGAD000046",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000046"
                },
                {
                    "identifier": "JGAD000047",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000047"
                },
                {
                    "identifier": "JGAD000051",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000051"
                },
                {
                    "identifier": "JGAD000052",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000052"
                },
                {
                    "identifier": "JGAD000053",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000053"
                },
                {
                    "identifier": "JGAD000057",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000057"
                },
                {
                    "identifier": "JGAD000059",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000059"
                },
                {
                    "identifier": "JGAD000060",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000060"
                },
                {
                    "identifier": "JGAD000061",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000061"
                },
                {
                    "identifier": "JGAD000062",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000062"
                },
                {
                    "identifier": "JGAD000064",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000064"
                },
                {
                    "identifier": "JGAD000065",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000065"
                },
                {
                    "identifier": "JGAD000068",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000068"
                },
                {
                    "identifier": "JGAD000070",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000070"
                },
                {
                    "identifier": "JGAD000073",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000073"
                },
                {
                    "identifier": "JGAD000075",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000075"
                },
                {
                    "identifier": "JGAD000076",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000076"
                },
                {
                    "identifier": "JGAD000077",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000077"
                },
                {
                    "identifier": "JGAD000078",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000078"
                },
                {
                    "identifier": "JGAD000079",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000079"
                },
                {
                    "identifier": "JGAD000080",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000080"
                },
                {
                    "identifier": "JGAD000081",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000081"
                },
                {
                    "identifier": "JGAD000082",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000082"
                },
                {
                    "identifier": "JGAD000085",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000085"
                },
                {
                    "identifier": "JGAD000086",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000086"
                },
                {
                    "identifier": "JGAD000087",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000087"
                },
                {
                    "identifier": "JGAD000088",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000088"
                },
                {
                    "identifier": "JGAD000089",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000089"
                },
                {
                    "identifier": "JGAD000090",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000090"
                },
                {
                    "identifier": "JGAD000091",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000091"
                },
                {
                    "identifier": "JGAD000092",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000092"
                },
                {
                    "identifier": "JGAD000093",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000093"
                },
                {
                    "identifier": "JGAD000094",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000094"
                },
                {
                    "identifier": "JGAD000095",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000095"
                },
                {
                    "identifier": "JGAD000096",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000096"
                },
                {
                    "identifier": "JGAD000098",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000098"
                },
                {
                    "identifier": "JGAD000099",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000099"
                },
                {
                    "identifier": "JGAD000101",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000101"
                },
                {
                    "identifier": "JGAD000102",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000102"
                },
                {
                    "identifier": "JGAD000103",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000103"
                },
                {
                    "identifier": "JGAD000104",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000104"
                },
                {
                    "identifier": "JGAD000105",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000105"
                },
                {
                    "identifier": "JGAD000106",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000106"
                },
                {
                    "identifier": "JGAD000107",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000107"
                },
                {
                    "identifier": "JGAD000108",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000108"
                },
                {
                    "identifier": "JGAD000109",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000109"
                },
                {
                    "identifier": "JGAD000110",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000110"
                },
                {
                    "identifier": "JGAD000111",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000111"
                },
                {
                    "identifier": "JGAD000112",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000112"
                },
                {
                    "identifier": "JGAD000113",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000113"
                },
                {
                    "identifier": "JGAD000114",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000114"
                },
                {
                    "identifier": "JGAD000115",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000115"
                },
                {
                    "identifier": "JGAD000116",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000116"
                },
                {
                    "identifier": "JGAD000117",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000117"
                },
                {
                    "identifier": "JGAD000118",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000118"
                },
                {
                    "identifier": "JGAD000120",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000120"
                },
                {
                    "identifier": "JGAD000121",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000121"
                },
                {
                    "identifier": "JGAD000122",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000122"
                },
                {
                    "identifier": "JGAD000123",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000123"
                },
                {
                    "identifier": "JGAD000124",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000124"
                },
                {
                    "identifier": "JGAD000125",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000125"
                },
                {
                    "identifier": "JGAD000129",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000129"
                },
                {
                    "identifier": "JGAD000130",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000130"
                },
                {
                    "identifier": "JGAD000131",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000131"
                },
                {
                    "identifier": "JGAD000132",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000132"
                },
                {
                    "identifier": "JGAD000133",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000133"
                },
                {
                    "identifier": "JGAD000134",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000134"
                },
                {
                    "identifier": "JGAD000135",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000135"
                },
                {
                    "identifier": "JGAD000136",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000136"
                },
                {
                    "identifier": "JGAD000137",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000137"
                },
                {
                    "identifier": "JGAD000138",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000138"
                },
                {
                    "identifier": "JGAD000139",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000139"
                },
                {
                    "identifier": "JGAD000140",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000140"
                },
                {
                    "identifier": "JGAD000141",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000141"
                },
                {
                    "identifier": "JGAD000142",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000142"
                },
                {
                    "identifier": "JGAD000143",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000143"
                },
                {
                    "identifier": "JGAD000144",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000144"
                },
                {
                    "identifier": "JGAD000145",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000145"
                },
                {
                    "identifier": "JGAD000146",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000146"
                },
                {
                    "identifier": "JGAD000147",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000147"
                },
                {
                    "identifier": "JGAD000148",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000148"
                },
                {
                    "identifier": "JGAD000149",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000149"
                },
                {
                    "identifier": "JGAD000150",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000150"
                },
                {
                    "identifier": "JGAD000151",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000151"
                },
                {
                    "identifier": "JGAD000152",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000152"
                },
                {
                    "identifier": "JGAD000153",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000153"
                },
                {
                    "identifier": "JGAD000154",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000154"
                },
                {
                    "identifier": "JGAD000155",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000155"
                },
                {
                    "identifier": "JGAD000156",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000156"
                },
                {
                    "identifier": "JGAD000157",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000157"
                },
                {
                    "identifier": "JGAD000158",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000158"
                },
                {
                    "identifier": "JGAD000159",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000159"
                },
                {
                    "identifier": "JGAD000160",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000160"
                },
                {
                    "identifier": "JGAD000161",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000161"
                },
                {
                    "identifier": "JGAD000162",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000162"
                },
                {
                    "identifier": "JGAD000163",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000163"
                },
                {
                    "identifier": "JGAD000164",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000164"
                },
                {
                    "identifier": "JGAD000165",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000165"
                },
                {
                    "identifier": "JGAD000166",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000166"
                },
                {
                    "identifier": "JGAD000167",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000167"
                },
                {
                    "identifier": "JGAD000168",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000168"
                },
                {
                    "identifier": "JGAD000169",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000169"
                },
                {
                    "identifier": "JGAD000170",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000170"
                },
                {
                    "identifier": "JGAD000171",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000171"
                },
                {
                    "identifier": "JGAD000172",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000172"
                },
                {
                    "identifier": "JGAD000173",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000173"
                },
                {
                    "identifier": "JGAD000174",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000174"
                },
                {
                    "identifier": "JGAD000175",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000175"
                },
                {
                    "identifier": "JGAD000176",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000176"
                },
                {
                    "identifier": "JGAD000177",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000177"
                },
                {
                    "identifier": "JGAD000178",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000178"
                },
                {
                    "identifier": "JGAD000179",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000179"
                },
                {
                    "identifier": "JGAD000180",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000180"
                },
                {
                    "identifier": "JGAD000181",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000181"
                },
                {
                    "identifier": "JGAD000182",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000182"
                },
                {
                    "identifier": "JGAD000183",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000183"
                },
                {
                    "identifier": "JGAD000184",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000184"
                },
                {
                    "identifier": "JGAD000185",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000185"
                },
                {
                    "identifier": "JGAD000186",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000186"
                },
                {
                    "identifier": "JGAD000187",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000187"
                },
                {
                    "identifier": "JGAD000188",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000188"
                },
                {
                    "identifier": "JGAD000189",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000189"
                },
                {
                    "identifier": "JGAD000190",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000190"
                },
                {
                    "identifier": "JGAD000191",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000191"
                },
                {
                    "identifier": "JGAD000192",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000192"
                },
                {
                    "identifier": "JGAD000193",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000193"
                },
                {
                    "identifier": "JGAD000194",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000194"
                },
                {
                    "identifier": "JGAD000195",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000195"
                },
                {
                    "identifier": "JGAD000196",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000196"
                },
                {
                    "identifier": "JGAD000197",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000197"
                },
                {
                    "identifier": "JGAD000198",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000198"
                },
                {
                    "identifier": "JGAD000199",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000199"
                },
                {
                    "identifier": "JGAD000200",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000200"
                },
                {
                    "identifier": "JGAD000201",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000201"
                },
                {
                    "identifier": "JGAD000203",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000203"
                },
                {
                    "identifier": "JGAD000204",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000204"
                },
                {
                    "identifier": "JGAD000205",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000205"
                },
                {
                    "identifier": "JGAD000206",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000206"
                },
                {
                    "identifier": "JGAD000207",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000207"
                },
                {
                    "identifier": "JGAD000208",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000208"
                },
                {
                    "identifier": "JGAD000209",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000209"
                },
                {
                    "identifier": "JGAD000210",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000210"
                },
                {
                    "identifier": "JGAD000211",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000211"
                },
                {
                    "identifier": "JGAD000213",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000213"
                },
                {
                    "identifier": "JGAD000214",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000214"
                },
                {
                    "identifier": "JGAD000215",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000215"
                },
                {
                    "identifier": "JGAD000216",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000216"
                },
                {
                    "identifier": "JGAD000217",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000217"
                },
                {
                    "identifier": "JGAD000218",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000218"
                },
                {
                    "identifier": "JGAD000219",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000219"
                },
                {
                    "identifier": "JGAD000220",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000220"
                },
                {
                    "identifier": "JGAD000221",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000221"
                },
                {
                    "identifier": "JGAD000223",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000223"
                },
                {
                    "identifier": "JGAD000224",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000224"
                },
                {
                    "identifier": "JGAD000225",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000225"
                },
                {
                    "identifier": "JGAD000226",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000226"
                },
                {
                    "identifier": "JGAD000228",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000228"
                },
                {
                    "identifier": "JGAD000229",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000229"
                },
                {
                    "identifier": "JGAD000231",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000231"
                },
                {
                    "identifier": "JGAD000233",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000233"
                },
                {
                    "identifier": "JGAD000234",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000234"
                },
                {
                    "identifier": "JGAD000235",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000235"
                },
                {
                    "identifier": "JGAD000236",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000236"
                },
                {
                    "identifier": "JGAD000237",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000237"
                },
                {
                    "identifier": "JGAD000238",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000238"
                },
                {
                    "identifier": "JGAD000240",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000240"
                },
                {
                    "identifier": "JGAD000242",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000242"
                },
                {
                    "identifier": "JGAD000243",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000243"
                },
                {
                    "identifier": "JGAD000244",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000244"
                },
                {
                    "identifier": "JGAD000247",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000247"
                },
                {
                    "identifier": "JGAD000248",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000248"
                },
                {
                    "identifier": "JGAD000249",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000249"
                },
                {
                    "identifier": "JGAD000250",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000250"
                },
                {
                    "identifier": "JGAD000251",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000251"
                },
                {
                    "identifier": "JGAD000252",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000252"
                },
                {
                    "identifier": "JGAD000253",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000253"
                },
                {
                    "identifier": "JGAD000254",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000254"
                },
                {
                    "identifier": "JGAD000256",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000256"
                },
                {
                    "identifier": "JGAD000257",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000257"
                },
                {
                    "identifier": "JGAD000258",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000258"
                },
                {
                    "identifier": "JGAD000259",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000259"
                },
                {
                    "identifier": "JGAD000260",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000260"
                },
                {
                    "identifier": "JGAD000261",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000261"
                },
                {
                    "identifier": "JGAD000263",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000263"
                },
                {
                    "identifier": "JGAD000264",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000264"
                },
                {
                    "identifier": "JGAD000268",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000268"
                },
                {
                    "identifier": "JGAD000270",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000270"
                },
                {
                    "identifier": "JGAD000272",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000272"
                },
                {
                    "identifier": "JGAD000273",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000273"
                },
                {
                    "identifier": "JGAD000274",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000274"
                },
                {
                    "identifier": "JGAD000275",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000275"
                },
                {
                    "identifier": "JGAD000277",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000277"
                },
                {
                    "identifier": "JGAD000279",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000279"
                },
                {
                    "identifier": "JGAD000280",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000280"
                },
                {
                    "identifier": "JGAD000281",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000281"
                },
                {
                    "identifier": "JGAD000282",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000282"
                },
                {
                    "identifier": "JGAD000283",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000283"
                },
                {
                    "identifier": "JGAD000284",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000284"
                },
                {
                    "identifier": "JGAD000285",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000285"
                },
                {
                    "identifier": "JGAD000286",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000286"
                },
                {
                    "identifier": "JGAD000287",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000287"
                },
                {
                    "identifier": "JGAD000288",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000288"
                },
                {
                    "identifier": "JGAD000289",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000289"
                },
                {
                    "identifier": "JGAD000290",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000290"
                },
                {
                    "identifier": "JGAD000291",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000291"
                },
                {
                    "identifier": "JGAD000292",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000292"
                },
                {
                    "identifier": "JGAD000297",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000297"
                },
                {
                    "identifier": "JGAD000300",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000300"
                },
                {
                    "identifier": "JGAD000301",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000301"
                },
                {
                    "identifier": "JGAD000302",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000302"
                },
                {
                    "identifier": "JGAD000303",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000303"
                },
                {
                    "identifier": "JGAD000304",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000304"
                },
                {
                    "identifier": "JGAD000307",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000307"
                },
                {
                    "identifier": "JGAD000308",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000308"
                },
                {
                    "identifier": "JGAD000309",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000309"
                },
                {
                    "identifier": "JGAD000310",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000310"
                },
                {
                    "identifier": "JGAD000311",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000311"
                },
                {
                    "identifier": "JGAD000312",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000312"
                },
                {
                    "identifier": "JGAD000313",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000313"
                },
                {
                    "identifier": "JGAD000314",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000314"
                },
                {
                    "identifier": "JGAD000315",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000315"
                },
                {
                    "identifier": "JGAD000316",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000316"
                },
                {
                    "identifier": "JGAD000318",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000318"
                },
                {
                    "identifier": "JGAD000319",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000319"
                },
                {
                    "identifier": "JGAD000320",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000320"
                },
                {
                    "identifier": "JGAD000321",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000321"
                },
                {
                    "identifier": "JGAD000322",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000322"
                },
                {
                    "identifier": "JGAD000323",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000323"
                },
                {
                    "identifier": "JGAD000324",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000324"
                },
                {
                    "identifier": "JGAD000325",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000325"
                },
                {
                    "identifier": "JGAD000326",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000326"
                },
                {
                    "identifier": "JGAD000327",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000327"
                },
                {
                    "identifier": "JGAD000328",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000328"
                },
                {
                    "identifier": "JGAD000329",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000329"
                },
                {
                    "identifier": "JGAD000330",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000330"
                },
                {
                    "identifier": "JGAD000331",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000331"
                },
                {
                    "identifier": "JGAD000332",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000332"
                },
                {
                    "identifier": "JGAD000334",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000334"
                },
                {
                    "identifier": "JGAD000335",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000335"
                },
                {
                    "identifier": "JGAD000336",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000336"
                },
                {
                    "identifier": "JGAD000337",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000337"
                },
                {
                    "identifier": "JGAD000338",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000338"
                },
                {
                    "identifier": "JGAD000339",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000339"
                },
                {
                    "identifier": "JGAD000340",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000340"
                },
                {
                    "identifier": "JGAD000341",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000341"
                },
                {
                    "identifier": "JGAD000342",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000342"
                },
                {
                    "identifier": "JGAD000343",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000343"
                },
                {
                    "identifier": "JGAD000344",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000344"
                },
                {
                    "identifier": "JGAD000345",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000345"
                },
                {
                    "identifier": "JGAD000346",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000346"
                },
                {
                    "identifier": "JGAD000348",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000348"
                },
                {
                    "identifier": "JGAD000349",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000349"
                },
                {
                    "identifier": "JGAD000355",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000355"
                },
                {
                    "identifier": "JGAD000356",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000356"
                },
                {
                    "identifier": "JGAD000359",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000359"
                },
                {
                    "identifier": "JGAD000360",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000360"
                },
                {
                    "identifier": "JGAD000361",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000361"
                },
                {
                    "identifier": "JGAD000362",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000362"
                },
                {
                    "identifier": "JGAD000363",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000363"
                },
                {
                    "identifier": "JGAD000365",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000365"
                },
                {
                    "identifier": "JGAD000366",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000366"
                },
                {
                    "identifier": "JGAD000367",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000367"
                },
                {
                    "identifier": "JGAD000368",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000368"
                },
                {
                    "identifier": "JGAD000370",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000370"
                },
                {
                    "identifier": "JGAD000371",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000371"
                },
                {
                    "identifier": "JGAD000372",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000372"
                },
                {
                    "identifier": "JGAD000373",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000373"
                },
                {
                    "identifier": "JGAD000375",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000375"
                },
                {
                    "identifier": "JGAD000376",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000376"
                },
                {
                    "identifier": "JGAD000378",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000378"
                },
                {
                    "identifier": "JGAD000379",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000379"
                },
                {
                    "identifier": "JGAD000380",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000380"
                },
                {
                    "identifier": "JGAD000382",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000382"
                },
                {
                    "identifier": "JGAD000383",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000383"
                },
                {
                    "identifier": "JGAD000384",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000384"
                },
                {
                    "identifier": "JGAD000385",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000385"
                },
                {
                    "identifier": "JGAD000386",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000386"
                },
                {
                    "identifier": "JGAD000387",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000387"
                },
                {
                    "identifier": "JGAD000388",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000388"
                },
                {
                    "identifier": "JGAD000391",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000391"
                },
                {
                    "identifier": "JGAD000392",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000392"
                },
                {
                    "identifier": "JGAD000394",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000394"
                },
                {
                    "identifier": "JGAD000395",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000395"
                },
                {
                    "identifier": "JGAD000396",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000396"
                },
                {
                    "identifier": "JGAD000398",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000398"
                },
                {
                    "identifier": "JGAD000399",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000399"
                },
                {
                    "identifier": "JGAD000400",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000400"
                },
                {
                    "identifier": "JGAD000401",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000401"
                },
                {
                    "identifier": "JGAD000402",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000402"
                },
                {
                    "identifier": "JGAD000403",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000403"
                },
                {
                    "identifier": "JGAD000404",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000404"
                },
                {
                    "identifier": "JGAD000405",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000405"
                },
                {
                    "identifier": "JGAD000406",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000406"
                },
                {
                    "identifier": "JGAD000407",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000407"
                },
                {
                    "identifier": "JGAD000410",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000410"
                },
                {
                    "identifier": "JGAD000411",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000411"
                },
                {
                    "identifier": "JGAD000412",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000412"
                },
                {
                    "identifier": "JGAD000414",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000414"
                },
                {
                    "identifier": "JGAD000415",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000415"
                },
                {
                    "identifier": "JGAD000416",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000416"
                },
                {
                    "identifier": "JGAD000418",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000418"
                },
                {
                    "identifier": "JGAD000419",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000419"
                },
                {
                    "identifier": "JGAD000420",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000420"
                },
                {
                    "identifier": "JGAD000421",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000421"
                },
                {
                    "identifier": "JGAD000422",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000422"
                },
                {
                    "identifier": "JGAD000423",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000423"
                },
                {
                    "identifier": "JGAD000424",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000424"
                },
                {
                    "identifier": "JGAD000425",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000425"
                },
                {
                    "identifier": "JGAD000427",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000427"
                },
                {
                    "identifier": "JGAD000428",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000428"
                },
                {
                    "identifier": "JGAD000429",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000429"
                },
                {
                    "identifier": "JGAD000430",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000430"
                },
                {
                    "identifier": "JGAD000431",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000431"
                },
                {
                    "identifier": "JGAD000432",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000432"
                },
                {
                    "identifier": "JGAD000434",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000434"
                },
                {
                    "identifier": "JGAD000436",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000436"
                },
                {
                    "identifier": "JGAD000438",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000438"
                },
                {
                    "identifier": "JGAD000441",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000441"
                },
                {
                    "identifier": "JGAD000446",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000446"
                },
                {
                    "identifier": "JGAD000452",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000452"
                },
                {
                    "identifier": "JGAD000457",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000457"
                },
                {
                    "identifier": "JGAD000458",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000458"
                },
                {
                    "identifier": "JGAD000459",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000459"
                },
                {
                    "identifier": "JGAD000460",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000460"
                },
                {
                    "identifier": "JGAD000461",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000461"
                },
                {
                    "identifier": "JGAD000462",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000462"
                },
                {
                    "identifier": "JGAD000463",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000463"
                },
                {
                    "identifier": "JGAD000464",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000464"
                },
                {
                    "identifier": "JGAD000473",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000473"
                },
                {
                    "identifier": "JGAD000474",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000474"
                },
                {
                    "identifier": "JGAD000475",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000475"
                },
                {
                    "identifier": "JGAD000481",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000481"
                },
                {
                    "identifier": "JGAD000482",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000482"
                },
                {
                    "identifier": "JGAD000484",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000484"
                },
                {
                    "identifier": "JGAD000491",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000491"
                },
                {
                    "identifier": "JGAD000492",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000492"
                },
                {
                    "identifier": "JGAD000493",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000493"
                },
                {
                    "identifier": "JGAD000495",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000495"
                },
                {
                    "identifier": "JGAD000496",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000496"
                },
                {
                    "identifier": "JGAD000497",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000497"
                },
                {
                    "identifier": "JGAD000498",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000498"
                },
                {
                    "identifier": "JGAD000500",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000500"
                },
                {
                    "identifier": "JGAD000502",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000502"
                },
                {
                    "identifier": "JGAD000504",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000504"
                },
                {
                    "identifier": "JGAD000505",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000505"
                },
                {
                    "identifier": "JGAD000506",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000506"
                },
                {
                    "identifier": "JGAD000529",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000529"
                },
                {
                    "identifier": "JGAD000530",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000530"
                },
                {
                    "identifier": "JGAD000531",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000531"
                },
                {
                    "identifier": "JGAD000532",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000532"
                },
                {
                    "identifier": "JGAD000533",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000533"
                },
                {
                    "identifier": "JGAD000534",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000534"
                },
                {
                    "identifier": "JGAD000535",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000535"
                },
                {
                    "identifier": "JGAD000571",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000571"
                },
                {
                    "identifier": "JGAD000597",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000597"
                },
                {
                    "identifier": "JGAD000599",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000599"
                },
                {
                    "identifier": "JGAD000600",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000600"
                },
                {
                    "identifier": "JGAD000602",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000602"
                },
                {
                    "identifier": "JGAD000603",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000603"
                },
                {
                    "identifier": "JGAD000604",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000604"
                },
                {
                    "identifier": "JGAD000605",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000605"
                },
                {
                    "identifier": "JGAD000606",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000606"
                },
                {
                    "identifier": "JGAD000607",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000607"
                },
                {
                    "identifier": "JGAD000621",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000621"
                },
                {
                    "identifier": "JGAD000622",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000622"
                },
                {
                    "identifier": "JGAD000624",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000624"
                },
                {
                    "identifier": "JGAD000625",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000625"
                },
                {
                    "identifier": "JGAD000626",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000626"
                },
                {
                    "identifier": "JGAD000628",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000628"
                },
                {
                    "identifier": "JGAD000629",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000629"
                },
                {
                    "identifier": "JGAD000633",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000633"
                },
                {
                    "identifier": "JGAD000634",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000634"
                },
                {
                    "identifier": "JGAD000635",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000635"
                },
                {
                    "identifier": "JGAD000640",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000640"
                },
                {
                    "identifier": "JGAD000641",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000641"
                },
                {
                    "identifier": "JGAD000642",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000642"
                },
                {
                    "identifier": "JGAD000647",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000647"
                },
                {
                    "identifier": "JGAD000648",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000648"
                },
                {
                    "identifier": "JGAD000649",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000649"
                },
                {
                    "identifier": "JGAD000650",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000650"
                },
                {
                    "identifier": "JGAD000651",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000651"
                },
                {
                    "identifier": "JGAD000652",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000652"
                },
                {
                    "identifier": "JGAD000653",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000653"
                },
                {
                    "identifier": "JGAD000654",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000654"
                },
                {
                    "identifier": "JGAD000656",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000656"
                },
                {
                    "identifier": "JGAD000657",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000657"
                },
                {
                    "identifier": "JGAD000659",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000659"
                },
                {
                    "identifier": "JGAD000660",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000660"
                },
                {
                    "identifier": "JGAD000662",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000662"
                },
                {
                    "identifier": "JGAD000663",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000663"
                },
                {
                    "identifier": "JGAD000664",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000664"
                },
                {
                    "identifier": "JGAD000665",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000665"
                },
                {
                    "identifier": "JGAD000667",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000667"
                },
                {
                    "identifier": "JGAD000668",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000668"
                },
                {
                    "identifier": "JGAD000669",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000669"
                },
                {
                    "identifier": "JGAD000670",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000670"
                },
                {
                    "identifier": "JGAD000671",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000671"
                },
                {
                    "identifier": "JGAD000674",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000674"
                },
                {
                    "identifier": "JGAD000678",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000678"
                },
                {
                    "identifier": "JGAD000679",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000679"
                },
                {
                    "identifier": "JGAD000680",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000680"
                },
                {
                    "identifier": "JGAD000681",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000681"
                },
                {
                    "identifier": "JGAD000682",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000682"
                },
                {
                    "identifier": "JGAD000683",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000683"
                },
                {
                    "identifier": "JGAD000684",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000684"
                },
                {
                    "identifier": "JGAD000685",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000685"
                },
                {
                    "identifier": "JGAD000686",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000686"
                },
                {
                    "identifier": "JGAD000687",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000687"
                },
                {
                    "identifier": "JGAD000688",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000688"
                },
                {
                    "identifier": "JGAD000689",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000689"
                },
                {
                    "identifier": "JGAD000690",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000690"
                },
                {
                    "identifier": "JGAD000691",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000691"
                },
                {
                    "identifier": "JGAD000692",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000692"
                },
                {
                    "identifier": "JGAD000693",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000693"
                },
                {
                    "identifier": "JGAD000694",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000694"
                },
                {
                    "identifier": "JGAD000695",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000695"
                },
                {
                    "identifier": "JGAD000696",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000696"
                },
                {
                    "identifier": "JGAD000698",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000698"
                },
                {
                    "identifier": "JGAD000699",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000699"
                },
                {
                    "identifier": "JGAD000700",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000700"
                },
                {
                    "identifier": "JGAD000701",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000701"
                },
                {
                    "identifier": "JGAD000704",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000704"
                },
                {
                    "identifier": "JGAD000705",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000705"
                },
                {
                    "identifier": "JGAD000706",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000706"
                },
                {
                    "identifier": "JGAD000708",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000708"
                },
                {
                    "identifier": "JGAD000712",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000712"
                },
                {
                    "identifier": "JGAD000713",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000713"
                },
                {
                    "identifier": "JGAD000714",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000714"
                },
                {
                    "identifier": "JGAD000715",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000715"
                },
                {
                    "identifier": "JGAD000719",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000719"
                },
                {
                    "identifier": "JGAD000720",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000720"
                },
                {
                    "identifier": "JGAD000721",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000721"
                },
                {
                    "identifier": "JGAD000722",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000722"
                },
                {
                    "identifier": "JGAD000725",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000725"
                },
                {
                    "identifier": "JGAD000726",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000726"
                },
                {
                    "identifier": "JGAD000727",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000727"
                },
                {
                    "identifier": "JGAD000728",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000728"
                },
                {
                    "identifier": "JGAD000729",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000729"
                },
                {
                    "identifier": "JGAD000731",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000731"
                },
                {
                    "identifier": "JGAD000732",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000732"
                },
                {
                    "identifier": "JGAD000733",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000733"
                },
                {
                    "identifier": "JGAD000734",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000734"
                },
                {
                    "identifier": "JGAD000738",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000738"
                },
                {
                    "identifier": "JGAD000739",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000739"
                },
                {
                    "identifier": "JGAD000740",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000740"
                },
                {
                    "identifier": "JGAD000742",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000742"
                },
                {
                    "identifier": "JGAD000745",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000745"
                },
                {
                    "identifier": "JGAD000746",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000746"
                },
                {
                    "identifier": "JGAD000747",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000747"
                },
                {
                    "identifier": "JGAD000748",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000748"
                },
                {
                    "identifier": "JGAD000750",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000750"
                },
                {
                    "identifier": "JGAD000751",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000751"
                },
                {
                    "identifier": "JGAD000752",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000752"
                },
                {
                    "identifier": "JGAD000754",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000754"
                },
                {
                    "identifier": "JGAD000755",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000755"
                },
                {
                    "identifier": "JGAD000756",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000756"
                },
                {
                    "identifier": "JGAD000757",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000757"
                },
                {
                    "identifier": "JGAD000758",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000758"
                },
                {
                    "identifier": "JGAD000768",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000768"
                },
                {
                    "identifier": "JGAD000770",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000770"
                },
                {
                    "identifier": "JGAD000771",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000771"
                },
                {
                    "identifier": "JGAD000772",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000772"
                },
                {
                    "identifier": "JGAD000773",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000773"
                },
                {
                    "identifier": "JGAD000775",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000775"
                },
                {
                    "identifier": "JGAD000776",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000776"
                },
                {
                    "identifier": "JGAD000777",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000777"
                },
                {
                    "identifier": "JGAD000778",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000778"
                },
                {
                    "identifier": "JGAD000781",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000781"
                },
                {
                    "identifier": "JGAD000783",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000783"
                },
                {
                    "identifier": "JGAD000787",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000787"
                },
                {
                    "identifier": "JGAD000789",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000789"
                },
                {
                    "identifier": "JGAD000791",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000791"
                },
                {
                    "identifier": "JGAD000794",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000794"
                },
                {
                    "identifier": "JGAD000795",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000795"
                },
                {
                    "identifier": "JGAD000798",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000798"
                },
                {
                    "identifier": "JGAD000801",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000801"
                },
                {
                    "identifier": "JGAD000802",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000802"
                },
                {
                    "identifier": "JGAD000807",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000807"
                },
                {
                    "identifier": "JGAD000808",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000808"
                },
                {
                    "identifier": "JGAD000809",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000809"
                },
                {
                    "identifier": "JGAD000810",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000810"
                },
                {
                    "identifier": "JGAD000811",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000811"
                },
                {
                    "identifier": "JGAD000812",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000812"
                },
                {
                    "identifier": "JGAD000813",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000813"
                },
                {
                    "identifier": "JGAD000814",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000814"
                },
                {
                    "identifier": "JGAD000816",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000816"
                },
                {
                    "identifier": "JGAD000817",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000817"
                },
                {
                    "identifier": "JGAD000822",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000822"
                },
                {
                    "identifier": "JGAD000823",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000823"
                },
                {
                    "identifier": "JGAD000831",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000831"
                },
                {
                    "identifier": "JGAD000832",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000832"
                },
                {
                    "identifier": "JGAD000833",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000833"
                },
                {
                    "identifier": "JGAD000834",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000834"
                },
                {
                    "identifier": "JGAD000836",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000836"
                },
                {
                    "identifier": "JGAD000840",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000840"
                },
                {
                    "identifier": "JGAD000841",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000841"
                },
                {
                    "identifier": "JGAD000845",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000845"
                },
                {
                    "identifier": "JGAD000851",
                    "type": "jga-dataset",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-dataset/JGAD000851"
                },
                {
                    "identifier": "JGAP000001",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000001"
                },
                {
                    "identifier": "JGAP000002",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000002"
                },
                {
                    "identifier": "JGAP000003",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000003"
                },
                {
                    "identifier": "JGAP000004",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000004"
                },
                {
                    "identifier": "JGAP000005",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000005"
                },
                {
                    "identifier": "JGAP000006",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000006"
                },
                {
                    "identifier": "JGAP000008",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000008"
                },
                {
                    "identifier": "JGAP000009",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000009"
                },
                {
                    "identifier": "JGAP000011",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000011"
                },
                {
                    "identifier": "JGAP000012",
                    "type": "jga-policy",
                    "url": "https://ddbj.nig.ac.jp/search/resource/jga-policy/JGAP000012"
                }
            ]
            doc["status"] = "public"
            doc["visibility"] = "unrestricted-access"
            doc["dateCreated"] = "2014-07-07T05:00:37+09:00"
            doc["dateModified"] = "2024-08-26T22:02:00+09:00"
            doc["datePublished"] = "2020-09-25T06:52:15+09:00"

            docs.append(doc)
            i +=1

        clear_element(element)
        if i > batch_size:
            dict2jsonl(docs)
            docs = []
    return docs


def clear_element(element):
    element.clear()
    while element.getprevious() is not None:
        try:
            del element.getparent()[0]
        except:
            print("clear_element Error")


def dict2jsonl(docs: List[dict]):
    jsonl_output = "jga-dac_JGAC000001.jsonl"
    with open(jsonl_output, "a") as f:
        for doc in docs:
            # 差分更新でファイル後方からjsonlを分割する場合は通常のESのjsonlとはindexとbodyの配置を逆にする << しない
            header = {"index": {"_index": "jga-dac", "_id": "JGAC000001"}}
            doc.pop("accession")
            f.write(json.dumps(header) + "\n")
            json.dump(doc, f)
            f.write("\n")


if __name__ == "__main__":
    input = "/mnt/data/ddbj/jga-adhoc/jga-dac.xml"
    docs = xml2json(input)
    dict2jsonl(docs)
