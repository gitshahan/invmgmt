import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient


def send_email(subject, body, to_email, from_email, cc_emails=None):
    # Create the MIMEText message object
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    if cc_emails:
        msg['Cc'] = ', '.join(cc_emails)
        to_addrs = [to_email] + cc_emails
    else:
        to_addrs = [to_email]

    msg.attach(MIMEText(body, 'plain'))

    # Set up the SMTP server
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, get_app_pass())
        text = msg.as_string()
        server.sendmail(from_email, to_addrs, text)
        server.quit()
    except Exception as e:
        print(f"Failed to send email: {e}")


def get_app_pass():
    cid = os.getenv("CLIENT_ID")
    key_vault_url = "https://gapppasskv.vault.azure.net/"
    credential = ManagedIdentityCredential(client_id=cid)
    client = SecretClient(vault_url=key_vault_url, credential=credential)
    secret_name = "apppass"
    retrieved_secret = client.get_secret(secret_name)
    return retrieved_secret.value


# if __name__ == '__main__':
#     send_email(subject="Joey", body="The email received successfully", to_email="shahan.mehboob@outlook.com",
#                from_email="shahan@autonicals.org")
