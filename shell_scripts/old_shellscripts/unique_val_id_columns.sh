#!/bin/bash



file="C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\DATAFEED\raw_dataset\20240601_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"

awk -F, 'NR > 1 {isin[$1]++; clarityid[$241]++; permid[$253]++; issuer_name[$3]++}
END {
  print "isin:", length(isin)
  print "ClarityID:", length(clarityid)
  print "permId:", length(permid)
  print "issuer_name:", length(issuer_name)

}' "$file"