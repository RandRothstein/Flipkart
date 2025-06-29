import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Define the email parameters
sender_email = "MEC_Automation@flipkart.com"
receiver_emails = ["apl-reporting@flipkart.com", "apl-oncall@flipkart.com", "apldev@flipkart.com", "dcc@flipkart.com", "managed-mec@flipkart.com", 
"wallstreet-alerts@flipkart.com", "verma.nitin@flipkart.com", "varun.kapoor@flipkart.com", "accounting-product@flipkart.com"]

subject = "Mec Automation:Missing Id ingestion status"
smtp_server = "10.19.1.70"
smtp_port = 25  # Using TLS/STARTTLS
smtp_username = "apl-reporting"
smtp_password = "fb04130b"

# Read the ingestion_status.html content
with open("ingestion_status.html", "r") as file:
    html_content = file.read()

# Create the email message
message = MIMEMultipart("alternative")
message["From"] = sender_email
message["To"] = ", ".join(receiver_emails)  # Join emails with commas
message["Subject"] = subject

# Create the complete HTML content with "Thank you" message outside the table
html_content_combined = f"""
<html>
<body>
<p>Max Process limit parameter values:</p>
<ul>
    <li>Invoice entity max_size = approx ~ 150000 rows</li>
    <li>Accrual entity max_size = approx ~ 150000 rows</li>
    <li>I2p entity max_size = approx ~ 150000 rows</li>
    <li>Groot entity max_size = approx ~ 10000 rows</li>
    <li>Payment_advisor_transaction entity max_size = approx ~ 10000 rows</li>
    <li>Advice entity max_size = approx ~ 10000 rows</li>
</ul>
{html_content}
<br><br>
<p><strong>Note</strong>: This is automated alerts, please don't reply all. In case of any clarification please reach out to <a href="mailto:managed-mec@flipkart.com">managed-mec@flipkart.com</a>.</p>
<br><br>
<p>Regards,<br>
MEC Team</p>
</body>
</html>
"""

# Attach the HTML part to the message
message.attach(MIMEText(html_content_combined, "html"))

# Send the email
try:
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.login(smtp_username, smtp_password)
        server.sendmail(sender_email, receiver_emails, message.as_string())
        print("Email sent successfully.")
except smtplib.SMTPAuthenticationError as e:
    print(f"Authentication error: {e}")
except smtplib.SMTPException as e:
    print(f"SMTP error occurred: {e}")
except Exception as e:
    print(f"Error: {e}")
