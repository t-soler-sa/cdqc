#!/bin/bash
 
input_file="C:/Users/n740789/OneDrive - Santander Office 365/Documentos/Projects/DataSets/DATAFEED/raw_dataset/20240601_Equities_feed_new_strategies_filtered_old"
output_file="C:/Users/n740789/OneDrive - Santander Office 365/Documentos/Projects/DataSets/DATAFEED/issuers_shared_permid.csv"
 
awk -F, '
BEGIN { OFS = "," }
NR == 1 {
    header = $0
    next
}
{
    issuer_name[$253] = (issuer_name[$253] == "" ? $3 : issuer_name[$253])
    if (issuer_name[$253] != $3) {
        shared_permid[$253] = 1
    }
    data[$253] = (data[$253] == "" ? $0 : data[$253] "\n" $0)
}
END {
    print header > "'$output_file'"
    for (permId in shared_permid) {
        print data[permId] >> "'$output_file'"
    }
}
' "$input_file"