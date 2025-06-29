import mysql.connector
import pandas as pd
from datetime import datetime
import os
import time

# Database connection details
db1_host = "master.wallst-apl-fkmp-staging-register-u2.prod.altair.fkcloud.in"
db1_user = "v-apl--uk75teUkQ"
db1_password = "rMLMQFHHF-yZ3YzERp1B"  # Replace with your actual password
db1_name = "shovel_production"

# Get the current date
current_date = datetime.now().strftime("%Y%m%d")

# Query to be executed
query = """
SELECT status, COUNT(*)
FROM ingestion_entities
WHERE group_search_key = %s
GROUP BY status;
"""

def get_ingestion_status(group_id_prefix, csv_filename):
    # Generate the group ID
    group_id = f'{group_id_prefix}{current_date}'
    #time.sleep(30) 
    cursor = None
    cnx = None
    try:
        # Connect to the database
        cnx = mysql.connector.connect(
            host=db1_host, user=db1_user, password=db1_password, database=db1_name
        )
        cursor = cnx.cursor()

        # Execute the query with the group_id parameter
        cursor.execute(query, (group_id,))
        rows = cursor.fetchall()

        # Initialize result dictionary
        result = {
            "Group ID": group_id,
            "DP Missing Id Count": 0,
            "Processed": 0,
            "Created": 0,
            "Picked": 0,
            "Failed": 0
        }

        for row in rows:
            status, count = row
            if status.lower() == 'processed':
                result["Processed"] = count
            elif status.lower() == 'created':
                result["Created"] = count
            elif status.lower() == 'picked':
                result["Picked"] = count
            elif status.lower() == 'failed':
                result["Failed"] = count

        # Count the lines in the CSV file
        dp_count = 0
        csv_path = f'/home/randrothstein.vc/{csv_filename}'
        if os.path.exists(csv_path):
            with open(csv_path, 'r') as file:
                dp_count = max(0, sum(1 for line in file) - 1)  # Ensure it shows 0 if no rows
        result["DP Missing Id Count"] = dp_count

        return result

    except mysql.connector.Error as error:
        print(f"Error: {error}")
        return {
            "Group ID": group_id,
            "DP Missing Id Count": "N/A",
            "Processed": "N/A",
            "Created": "N/A",
            "Picked": "N/A",
            "Failed": "N/A"
        }

    finally:
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

def main():
    group_id_prefixes = [
        ('accrual_by_client_ref_id_', 'accrual.csv'),
        ('groot_', 'groot.csv'),
        ('advice_', 'advice.csv'),
        ('i2p_', 'i2p.csv'),
        ('invoice_by_client_ref_id_', 'invoice.csv'),
        ('payment_advisr_transaction_', 'payment_advisor_transaction.csv'),
        ('payment_mapping_', 'invoice_mapping.csv')
    ]

    results = []
    
    for prefix, csv_filename in group_id_prefixes:
        result = get_ingestion_status(prefix, csv_filename)
        results.append(result)

    # Create a DataFrame and save it to an HTML file
    df = pd.DataFrame(results)
    with open('ingestion_status.html', 'w') as file:
        file.write(df.to_html(index=False, border=1, classes='dataframe'))

    print("Ingestion Status Table has been saved to 'ingestion_status.html'")
    
    # Clear the contents of the CSV files
    for _, csv_filename in group_id_prefixes:
        csv_path = f'/home/randrothstein.vc/{csv_filename}'
        if os.path.exists(csv_path):
            with open(csv_path, 'w') as file:
                file.truncate(0)

# Run the script
main()
