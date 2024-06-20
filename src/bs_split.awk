#!/usr/bin/awk -f

time awk -v maxRecs=1000000 -v RS='</BioSample>' -v ORS= '
    (NR % maxRecs) == 1 {
        close(out); out="split_bs" (++fileNr) ".xml"
    }
    RT { print $0 RT > out }
' biosample_set.xml