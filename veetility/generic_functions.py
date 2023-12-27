#%%
import pygsheets
import requests
import json
import os
import mimetypes
from io import BytesIO
import pandas as pd
# import generic_credentials
#from snowflake.snowpark import Session
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
#%%

#%%
SLACK_CONN_ID = 'slack'


def task_fail_slack_alert(context, channel_id=None, ec2_url=None):
    """Send a Slack alert for a failed task in an Airflow DAG.

    This function constructs and sends a Slack message when a task in an Airflow DAG fails. 
    It includes details like the task ID, DAG ID, execution time, and a URL to the task's logs.
    The Slack channel ID and EC2 URL can be optionally specified; otherwise, they are retrieved 
    from Airflow Variables. The Slack webhook token is fetched from Airflow Connections.

    Args:
        context (dict): The context passed from the Airflow task, containing information like 
                        the task instance and execution date.
        channel_id (str, optional): Slack channel ID where the alert should be sent. If not 
                                    provided, it's fetched from Airflow Variables.
        ec2_url (str, optional): The base URL for the EC2 instance where logs are stored. If not 
                                 provided, it's fetched from Airflow Variables.

    Returns:
        None: The function sends a message to Slack and does not return anything.

    Raises:
        AirflowException: If the Slack alert fails to send for any reason.

    Examples:
        # Typical usage within an Airflow DAG
        fail_alert = task_fail_slack_alert(
            context=context,
            channel_id='your_slack_channel_id',
            ec2_url='http://your.ec2.url'
        )"""
    
    
    if channel_id == None:
        channel_id = Variable.get("slack_channel_id")
    if ec2_url == None:
        ec2_url = Variable.get("ec2_url")

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    ti = context.get('task_instance')
    execution_date = context.get(
        'execution_date').isoformat().replace('+', '%2B')
    base_url = ec2_url
    log_url = f"{base_url}/log?dag_id={ti.dag_id}&task_id={ti.task_id}&execution_date={execution_date}&map_index=-1"

    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        task=ti.task_id,
        dag=ti.dag_id,
        exec_date=context.get('data_interval_start').strftime('%Y-%m-%d %H:%M:%S'),
        log_url=log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel_id,
        username='airflow',
    )
    return failed_alert.execute(context=context)


def slack_error_notification(webhook, message='BLANK MESSSAGE'):
    '''
    Create a slack webhook to send a
    notification to a slack channel
    '''
    webhook = webhook
    payload = {'text': message}
    requests.post(webhook, data=json.dumps(payload))


def write_to_gsheet(service_file_path, spreadsheet_id, sheet_name, data_df):
    ''' 
    This function takes data_df and writes it
    under spreadsheet_id and sheet_name using your
    credentials under service_file_path
    '''
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        sh.add_worksheet(sheet_name)
    except BaseException:
        pass
    wks_write = sh.worksheet_by_title(sheet_name)
    try:
        wks_write.clear('A1', None, '*')
    except BaseException:
        pass
    wks_write.set_dataframe(
        data_df,
        (1, 1),
        copy_head=True,
        encoding='utf-8',
        extend=True)
    if wks_write.rows > 1:
        wks_write.frozen_rows = 1

def send_file_to_slack(
        file_path,
        message,
        channel_id,
        webhook):
    """
    Send a file (CSV, log or other) to a Slack channel using a webhook.

    Args:
        file_path (str): The path of the file to be sent.
        message (str): The message to be included with the file.
        channel_id (str, optional): The ID of the Slack channel to send the file to.
                                    Defaults to generic_credentials.channel_id.
        webhook (str, optional): The webhook URL for sending the file to Slack.
                                 Defaults to generic_credentials.webhook.
    """
    # Read the file content and prepare the payload
    file_content = None
    file_name = os.path.basename(file_path)
    file_type, _ = mimetypes.guess_type(file_path)

    # Read and convert CSV file to a string buffer
    if file_type == 'text/csv':
        df = pd.read_csv(file_path)
        str_buffer = BytesIO()
        df.to_csv(str_buffer, index=False)
        file_content = str_buffer.getvalue()

    # Read other file types as binary
    else:
        with open(file_path, 'rb') as f:
            file_content = f.read()

    # Prepare the payload and send the file
    payload = {
        "channels": channel_id,
        "initial_comment": message,
        "filetype": file_type,
        "filename": file_name,
        "file": file_content,
    }

    headers = {
        'Content-type': 'multipart/form-data'
    }

    response = requests.post(webhook, files=payload, headers=headers)

    # Check for errors in the response
    if response.status_code != 200:
        raise ValueError(
            f"Request to slack returned an error {response.status_code}, "
            f"the response is:\n{response.text}"
        )



