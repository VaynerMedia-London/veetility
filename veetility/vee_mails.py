#%%
import pandas as pd
import email
import imaplib
import boto3
import io
from datetime import datetime
today = datetime.today().date()

#%%
class VEEmail:
    
    def __init__(self, username, password):
        imap_server = 'imap.gmail.com'
        con = imaplib.IMAP4_SSL(imap_server)
        con.login(username, password)

        con.select('"[Gmail]/All Mail"')
        self.con = con
    
    def search_email(self, from_email=None, subject=None, sent_today=False):
        """
        Args:
            from_email (str): The exact email address of the sender of the email
            subject (str): A substring or the exact string you want to search the subject of an email
            sent_today (bool): Filter the mail by data that was sent today
        
        Returns: 
            email_ids (list): List of email ids of emails that meet the search criteria
        """

        search_string = ''

        if from_email != None:
            search_string += f'(FROM "{from_email}")'
        
        if subject != None:
            search_string += ' ' + f'(SUBJECT "{subject}")'
        
        if sent_today != False:
            search_string += ' ' + f'(SENTON "{today.strftime("%d-%b-%Y")}")'
    
        status, email_ids = self.con.search(None, search_string)

        if status != 'OK':
            print(f"Status = {status}")
            return []

        return email_ids
    
    def get_email_body(self, email_id):
        '''
        Args:
        email_id: Email ID

        Returns:
        email_body: 
        '''
        status, response = self.con.fetch(email_id, '(RFC822)')
        if status != 'OK':
            return None

        email_msg = email.message_from_bytes(response[0][1])
        if email_msg.is_multipart():
            for part in email_msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    return part.get_payload(decode=True).decode()
                elif content_type == "text/html":
                    return part.get_payload(decode=True).decode()

        else:
            return email_msg.get_payload(decode=True).decode()
    
    def download_s3_file(self, url, aws_access_key_id, aws_secret_access_key):
        '''
        Download file from S3.
        :param url: S3 URL
        :return: file content as string
        '''
        # Extracting bucket and key from the provided URL
        parsed_url = url.split('/')
        bucket = parsed_url[3]
        key = "/".join(parsed_url[4:])

        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read().decode('utf-8')

    def extract_csv_to_df(self, email_id ):
        """Not finished"""
   

        _, email_data = self.con.fetch(email_id, '(RFC822)')
        try:
            raw_email = email_data[0][1]
            msg = email.message_from_bytes(raw_email)
            # print(msg['Subject'])
        except:
            continue

        if msg.get_content_maintype() == 'multipart':
            for part in msg.walk():
                print(part.get_content_type())
                # try:
                #     attachment = part.get_payload(decode=True)
                #     print(attachment)
                # except:
                #      print("failed")
                if part.get_content_type() == 'text/csv':
                    attachment = part.get_payload(decode=True)
                    csv_data = attachment.decode('utf-8').splitlines()
                    df = pd.read_csv(io.StringIO('\n'.join(csv_data)), sep=',')
                    df_list.append(df)
        
        return df

        
