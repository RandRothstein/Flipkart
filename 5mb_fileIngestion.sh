#!/bin/bash

# Set the current date and time (without leading zero on day)
current_date_time=$(date +'%b %-d %H:%M:%S')
current_date=$(date +'%b %-d' | sed 's/^0//')  # Current date in format "Mar 3"

# Define the source and destination paths
remote_host1="10.53.243.64"
remote_host2="10.52.66.201"
remote_host3="10.51.19.107"
remote_path="/tmp/"
local_path="/home/randrothstein.vc/"
log_file="/tmp/upload_logs.txt"

# Initialize a counter for processed files
total_processed=0

# Create/clear the log file
> "$log_file"

# Create a temporary HTML file to store table data
output_html="/tmp/processed_files_table.html"
cat <<EOT > "$output_html"
<!DOCTYPE html>
<html>
<head>
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
        }
        table, th, td {
            border: 1px solid black;
        }
        th, td {
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #f2f2f2;
        }
    </style>
</head>
<body>
<h2>Processed Files Table</h2>
<table>
    <tr>
        <th>File Name</th>
        <th>Processing Start Date</th>
        <th>Remote Host</th>
        <th>Status</th>
    </tr>
EOT

# Fetch the list of files from the remote servers, checking for the current date
file_list1=$(ssh randrothstein.vc@$remote_host1 "cd $remote_path; ls -lrt | awk '\$9 ~ /^fki/ && \$9 ~ /\.json$/ && \$6 \" \" \$7 == \"$current_date\" {print \$9}'")
file_list2=$(ssh randrothstein.vc@$remote_host2 "cd $remote_path; ls -lrt | awk '\$9 ~ /^fki/ && \$9 ~ /\.json$/ && \$6 \" \" \$7 == \"$current_date\" {print \$9}'")
file_list3=$(ssh randrothstein.vc@$remote_host3 "cd $remote_path; ls -lrt | awk '\$9 ~ /^fki/ && \$9 ~ /\.json$/ && \$6 \" \" \$7 == \"$current_date\" {print \$9}'")

# Function to process files from a remote host
process_files() {
    local host=$1
    local file_list=$2

    # Check if any files are found
    if [ -n "$file_list" ]; then
        for file in $file_list; do
            # Copy the file to the local machine
	    current_date_time=$(date +'%b %-d %H:%M:%S')
            echo "Copying $file from $host to local path $local_path at $current_date_time"
            scp randrothstein.vc@$host:$remote_path$file $local_path

            # Check if the copy was successful
            if [ $? -eq 0 ]; then
                echo "Successfully copied $file to $local_path at $current_date_time"

                # Process the file on the second remote server
                echo "Processing $file on the second remote server at $current_date_time"
                scp $local_path$file randrothstein.vc@10.49.3.102:/home/randrothstein.vc/
                
                ssh randrothstein.vc@10.49.3.102 << EOF >> "$log_file"
                fdp-batch-ingestor -d $file -u fkint/apl/finance_reporting/invoice -v3.6 -tJSON
                exit
EOF

                # Determine if the file was successfully uploaded
                if grep -q "Records has been uploaded successfully" "$log_file"; then
                    status="Successfully Uploaded"
                else
                    status="Upload Failed"
                fi

                # Append data to the HTML table
                echo "    <tr><td>$file</td><td>$current_date_time</td><td>$host</td><td>$status</td></tr>" >> "$output_html"
                total_processed=$((total_processed + 1))  # Increment the counter
            else
                echo "Failed to copy $file from $host at $current_date_time"
                echo "    <tr><td>$file</td><td>$current_date_time</td><td>$host</td><td>Copy Failed</td></tr>" >> "$output_html"
            fi
        done
    else
        echo "No files found for the current date ($current_date_time) on $host."
    fi
}

# Process files from all remote hosts
process_files $remote_host1 "$file_list1"
process_files $remote_host2 "$file_list2"
process_files $remote_host3 "$file_list3"
current_date_time=$(date +'%b %-d %H:%M:%S')
# Close the HTML table and add the total count
cat <<EOT >> "$output_html"
</table>
<p><strong>Total number of files processed:</strong> $total_processed</p>
<p>Automation script completed at $current_date_time.</p>
<br><br>
<p><strong>Note</strong>: This is automated alerts, please don't reply all. In case of any clarification please reach out to <a href="mailto:managed-mec@flipkart.com">managed-mec@flipkart.com</a>.</p>
<br><br>
<p>Regards,<br>
MEC Team</p>
</body>
</html>
EOT

# Print the location of the HTML file
echo -e "\nProcessed files table has been saved to: $output_html"

