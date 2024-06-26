#!/usr/bin/awk -f

time awk -v maxRecs=30000 -v RS='</BioSample>' -v ORS= -v head='<?xml version="1.0" encoding="UTF-8"?>\n<BioSampleSet>\n' -v tail='\n</BioSampleSet>' '
    (NR % maxRecs) == 1 {
        if (out) {
            print tail > out
            close(out)
        }
        out = "split_bs" (++fileNr) ".xml"
        print head > out
    }
    { print $0 RT > out }
    END {
        if (out) {
            print tail > out
            close(out)
        }
    }
' biosample_set.xml
