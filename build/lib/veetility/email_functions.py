import pandas as pd
import email
import imaplib
import boto3
import io

#We have a google automations macro that deletes all emails after 30 days

def auth(user,password,imap_url):
    '''
    Sets up the auth
    :param user: email address
    :param password: password
    :param imap_url: imap url
    :return: connection object
    '''
    con = imaplib.IMAP4_SSL(imap_url)
    con.login(user,password)
    return con

def search_email(connection, from_email, subject):
    '''
    Search for emails matching the FROM and SUBJECT criteria.
    :param connection: IMAP connection
    :param from_email: FROM email address
    :param subject: SUBJECT
    :return: list of matching email IDs
    '''
    # Step 1: Search by FROM criterion
    status, from_ids = connection.search(None, '(FROM "{}")'.format(from_email))
    if status != 'OK':
        return []
    
    matching_ids = []

    # Step 2: Filter the results by SUBJECT
    for e_id in from_ids[0].split():
        status, data = connection.fetch(e_id, '(BODY[HEADER])')
        if status != 'OK':
            continue
        header_data = email.message_from_bytes(data[0][1])
        email_subject = header_data['Subject']
        if subject in email_subject:
            matching_ids.append(e_id)
    
    return matching_ids

def search_most_recent_email(connection, from_email, subject):
    '''
    Search for the most recent email matching the FROM and SUBJECT criteria.
    :param connection: IMAP connection
    :param from_email: FROM email address
    :param subject: SUBJECT
    :return: the most recent email ID that matches the criteria, or None if no match is found
    '''
    # Step 1: Search by FROM criterion
    status, from_ids = connection.search(None, '(FROM "{}")'.format(from_email))
    if status != 'OK':
        return None
    
    # Sort IDs numerically to get the most recent emails first
    sorted_ids = sorted(from_ids[0].split(), key=int, reverse=True)
    
    # Step 2: Filter the results by SUBJECT
    for e_id in sorted_ids:
        status, data = connection.fetch(e_id, '(BODY[HEADER])')
        if status != 'OK':
            continue
        header_data = email.message_from_bytes(data[0][1])
        email_subject = header_data['Subject']
        if subject in email_subject:
            return e_id  # Return the most recent email ID that matches
    
    return None  # No match found

def get_email_body(connection, email_id):
    '''
    Get email body from its ID.
    :param connection: IMAP connection
    :param email_id: Email ID
    :return: email body
    '''
    status, response = connection.fetch(email_id, '(RFC822)')
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
    
def extract_s3_link(body):
    '''
    Extract S3 link from email body.
    :param body: email body
    :return: S3 link
    '''
    return body.split(": ")[1].strip()

def download_s3_file(url, aws_access_key_id, aws_secret_access_key):
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

def parse_csv(csv_content, header_row):
    '''
    Parse the downloaded CSV content to only save specific rows, starting from the header_row and excluding the last row.
    :param csv_content: CSV content as a string
    :param header_row: the row that contains the specific column headers
    :return: pandas DataFrame containing the content of the parsed CSV
    '''
    # Finding the start and end of the required content
    start_index = csv_content.index(header_row)
    end_index = csv_content.rindex('\n', 0, csv_content.rindex('\n'))

    # Extracting the required content
    csv_content = csv_content[start_index:end_index]

    # Reading the CSV content into a pandas DataFrame
    parsed_df = pd.read_csv(io.StringIO(csv_content))

    return parsed_df


def mark_email(connection, email_id, label):
    '''
    Add label to an email.
    :param connection: IMAP connection
    :param email_id: Email ID
    :param label: Label to add
    '''
    connection.store(email_id, '+X-GM-LABELS', label)