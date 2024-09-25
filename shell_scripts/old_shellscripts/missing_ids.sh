#!/bin/bash
 
input_file="C:/Users/n740789/OneDrive - Santander Office 365/Documentos/Projects/DataSets/DATAFEED/raw_dataset/20240601_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
output_file="C:/Users/n740789/OneDrive - Santander Office 365/Documentos/Projects/DataSets/DATAFEED/missing_id_info.csv"

awk -F, 'BEGIN { OFS = "," }
{
    if (NR == 1 || !($241 ~ /^[[:alnum:]]+$/) || !($253 ~ /^[[:alnum:]]+$/)) {
        print $1, $3, $9, $15, $16, $241, $253
    }
}' "$input_file" > "$output_file"