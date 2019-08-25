# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 14:08:10 2019

@author: Will Jackson
"""

import boto3
import paramiko
from datetime import datetime
import time
import os

# EC2 access credentials
ec2_client = boto3.client('ec2')
ec2_resource = boto3.resource('ec2')
instance_ids = ['i-0d1fa7089eef1311e']

# Start ETL Main instance
ec2_client.start_instances(InstanceIds=instance_ids, DryRun=False)

time.sleep(30)


# Check ETL Main instance status
status_response = ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
status = status_response[0]['InstanceState']['Name']
print(status)

hostname_response = ec2_resource.meta.client.describe_instances(InstanceIds = instance_ids)['Reservations'][0]['Instances']
hostname = hostname_response[0]['PublicDnsName']
print(hostname)

key_location = os.path.join(os.path.expanduser('~'), 'bin', 'Tickets_Key_5_Open.pub')
print(key_location)

if status == 'running':

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.connect(hostname, username='ubuntu', key_filename='C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Key Files/Tickets Key 5 Open.pub')
    ssh.connect(hostname, username='ubuntu', key_filename=key_location)


    stdin_test, stdout_test, stderr_test = ssh.exec_command('python3 ~/bin/test.py > ~/bin/test_log.txt')
    stdout_test.readlines()
    print('test.py finished running at ' + str(datetime.now()))


    ssh.close()

# Stop ETL Main instance
ec2_client.stop_instances(InstanceIds=instance_ids, DryRun=False)


