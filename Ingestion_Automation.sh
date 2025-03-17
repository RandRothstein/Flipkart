#!/bin/bash

# Get the current date in the format YYYY/M/D (without leading zeros in month and day)
current_date=$(date +"%Y/%-m/%-d")
#current_date=2025/3/12
# Define the URL, minimum file size in KB, and file paths
url="https://storage.googleapis.com/fk-s-accounting/pikachu/missingid/${current_date}/"
min_size_kb=1
max_size_kb=10240000000000
download_path="/home/randrothstein.vc/"
processed_path="/home/randrothstein.vc/"

# Flags to track if a valid file has been processed
invoice_processed=false
accrual_processed=false
groot_processed=false
i2p_processed=false
payment_advisor_processed=false
payment_advisor_transaction_processed=false
invoice_mapping_processed=false

# Function to process files
process_file() {
    local file_type=$1
    local filename=$2
    local awk_command=$3
    local python_command=$4

    local full_url="${url}${filename}"

    echo "Checking ${full_url}..."

    # Get the file size in bytes
    filesize=$(curl -sL -I "$full_url" | grep -i Content-Length | cut -d':' -f2 | tr -d ' ' | tr -dc '0-9')
    curl -s -o "${download_path}${file_type}.csv" "$full_url"
    echo "Downloaded file: $filename (size: $filesize_kb KB)"
    # Check if filesize is not empty and is a valid number
    if [[ -n "$filesize" && "$filesize" =~ ^[0-9]+$ ]]; then
        # Convert filesize to kilobytes
        filesize_kb=$((filesize / 1024))

        # Print file size for debugging
        echo "File: $filename, Size: $filesize bytes ($filesize_kb KB)"

        # Check if the file size is within the valid range
        if [[ $filesize_kb -ge $min_size_kb && $filesize_kb -le $max_size_kb ]]; then
          
            # Process the downloaded file with awk
            awk "$awk_command" "${download_path}${file_type}.csv" > "${processed_path}${file_type}1.csv"

            # Execute the python command (assuming rand_ingest.py is on the same machine)
            $python_command

            # Set the corresponding flag to true
            case "$file_type" in
                invoice) invoice_processed=true ;;
                accrual) accrual_processed=true ;;
                groot) groot_processed=true ;;
                i2p) i2p_processed=true ;;
                payment_advisor) payment_advisor_processed=true ;;
                payment_advisor_transaction) payment_advisor_transaction_processed=true ;;
		invoice_mapping) invoice_mapping_processed=true ;;
            esac

            return 0  # Return from function after processing
        else
            echo "File size of $filename is not within the valid range."
        fi
    else
        echo "No valid size information for $filename."
    fi
}

# Define awk and command patterns for each file type
invoice_awk='BEGIN {FS=","} NR == 1 {for (i=1; i<=NF; i++) {if ($i=="client_ref_id") client_ref_id_col=i; else if ($i == "bu_id") bu_id_col = i; else if ($i == "type") type_col = i;}} NR>1{print $bu_id_col "," $type_col "," $client_ref_id_col}'
accrual_awk='BEGIN {FS=","} NR == 1 {for (i=1; i<=NF; i++) {if ($i=="client_ref_id") client_ref_id_col=i; else if ($i == "bu_id") bu_id_col = i; else if ($i == "type") type_col = i;}} NR>1{print $bu_id_col "," $type_col "," $client_ref_id_col}'
groot_awk='BEGIN {FS=","} NR == 1 {for (i=1; i<=NF; i++) {if ($i == "bu_id") bu_id_col = i; else if($i =="transaction_id") transaction_col = i;}} NR>1{print $bu_id_col "," $transaction_col}'
i2p_awk='BEGIN {FS=","} NR == 1 {for (i=1; i<=NF; i++) {if ($i == "bu_id") bu_id_col = i; else if ($i == "id") transaction_col = i;}} NR>1{print $bu_id_col "," $transaction_col}'
payment_advisor_awk='BEGIN {FS=","} NR == 1 {for (i=1; i<=NF; i++) {if ($i == "bu_id") bu_id_col = i; else if ($i == "id") transaction_col = i;}} NR>1{print $bu_id_col "," $transaction_col}'
payment_advisor_transaction_awk='BEGIN {FS=","} NR == 1 {for (i=1; i<=NF; i++) {if ($i == "bu_id") bu_id_col = i; else if ($i=="payment_advice_header_id") header_id=i; else if ($i == "id") transaction_col = i;}} NR>1{print $bu_id_col ","$header_id"," $transaction_col}'
invoice_mapping_awk='BEGIN {FS=","} NR == 1 {for (i=1; i<=NF; i++) {if ($i == "bu_id") bu_id_col = i; else if($i =="client_ref_id") transaction_col = i;}} NR>1{print $bu_id_col "," $client_ref_id_col}'

# Define python command patterns for each file type

invoice_python="python ${processed_path}rand_ingest.py ${processed_path}invoice1.csv invoice_by_client_ref_id"
accrual_python="python ${processed_path}rand_ingest.py ${processed_path}accrual1.csv accrual_by_client_ref_id"
groot_python="python ${processed_path}rand_ingest.py ${processed_path}groot1.csv groot"
i2p_python="python ${processed_path}rand_ingest.py ${processed_path}i2p1.csv i2p"
payment_advisor_python="python ${processed_path}rand_ingest.py ${processed_path}payment_advisor1.csv advice"
payment_advisor_transaction_python="python ${processed_path}rand_ingest.py ${processed_path}payment_advisor_transaction1.csv transactions"
invoice_mapping_python="python ${processed_path}rand_ingest.py ${processed_path}invoice_mapping1.csv payment_mapping"

# Loop through the sequence of times (0000 to 2359) as four-digit numbers
for i in $(seq -w 0000 0259); do
    # Extract the hour and minutes from the four-digit sequence
    hour=${i:0:2}
    minute=${i:2:2}

    # Validate hour and minute
    if [[ "$hour" =~ ^[0-2][0-9]$ && "$minute" =~ ^[0-5][0-9]$ ]]; then
        # Check invoice files if not yet processed
        if [ "$invoice_processed" == false ]; then
            invoice_filename="invoice_${i}.csv"
            process_file "invoice" "$invoice_filename" "$invoice_awk" "$invoice_python"
        fi

        # Check accrual files if not yet processed
        if [ "$accrual_processed" == false ]; then
            accrual_filename="accrual_${i}.csv"
            process_file "accrual" "$accrual_filename" "$accrual_awk" "$accrual_python"
        fi

        # Check groot files if not yet processed
        if [ "$groot_processed" == false ]; then
            groot_filename="groot_${i}.csv"
            process_file "groot" "$groot_filename" "$groot_awk" "$groot_python"
        fi

        # Check i2p files if not yet processed
        if [ "$i2p_processed" == false ]; then
            i2p_filename="i2p_application_${i}.csv"
            process_file "i2p" "$i2p_filename" "$i2p_awk" "$i2p_python"
        fi

        # Check payment_advisor files if not yet processed
        if [ "$payment_advisor_processed" == false ]; then
            payment_advisor_filename="payment_advisor_${i}.csv"
            process_file "payment_advisor" "$payment_advisor_filename" "$payment_advisor_awk" "$payment_advisor_python"
        fi

        # Check payment_advisor_transaction files if not yet processed
        if [ "$payment_advisor_transaction_processed" == false ]; then
            payment_advisor_transaction_filename="payment_advisor_transaction_${i}.csv"
            process_file "payment_advisor_transaction" "$payment_advisor_transaction_filename" "$payment_advisor_transaction_awk" "$payment_advisor_transaction_python"
        fi
	
	if [ "$invoice_mapping_processed" == false ]; then
            invoice_mapping_filename="gamma_invoice_agg_prod_${i}.csv"
            process_file "invoice_mapping" "$invoice_mapping_filename" "$invoice_mapping_awk" "$invoice_mapping_python"
        fi
        # Exit the script if all file types have been processed
        if [[ "$invoice_processed" == true && "$accrual_processed" == true && "$groot_processed" == true && "$i2p_processed" == true && "$payment_advisor_processed" == true && "$payment_advisor_transaction_processed" == true ]]; then
            echo "All file types have been processed. Exiting."
            exit 0
        fi
    fi
done
exit 0

