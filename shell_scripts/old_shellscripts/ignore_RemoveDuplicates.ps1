$startTime = Get-Date
 
# Read CSV, remove duplicates, and write to a new file
$csv = Import-Csv -Path "C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\DATAFEED\raw_dataset\20240101_Equities_feed_new_strategies_filtered_old_names_iso_permId.csv"
$dedupedCsv = $csv | Sort-Object -Property permId -Unique
$dedupedCsv | Export-Csv -Path "C:\Users\n740789\OneDrive - Santander Office 365\Documentos\Projects\DataSets\DATAFEED\raw_dataset\20240101_Equities_feed_new_strategies_redux.csv" -NoTypeInformation
 
$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host "Duration: $($duration.ToString("hh\:mm\:ss"))"