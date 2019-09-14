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

rds_client = boto3.client('rds')

instance_ids = ['i-0d1fa7089eef1311e']

# Check ETL main instance status
status_response = ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
if not status_response:
    print('The main ETL instance was stopped at ' + str(datetime.now()))
else:
    print('The main ETL instance was running at the start of the job')

# Check RDS instance status
rds_status_response = rds_client.describe_db_instances(DBInstanceIdentifier='ticketsdb')
rds_instance_status = rds_status_response['DBInstances'][0]['DBInstanceStatus']
if rds_instance_status == 'available':
    print('The main RDS MYSQL DB was running at ' + str(datetime.now()))
else:
    print('The main RDS MYSQL DB was stopped at the start of the job')


def test():
    # Start ETL Main instance
    ec2_client.start_instances(InstanceIds=instance_ids, DryRun=False)

    # Start RDS Main instance
    # rds_client.start_db_instance(DBInstanceIdentifier = 'ticketsdb')
    
    time.sleep(120)

    # Check ETL Main instance status
    status_response = ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
    ec2_instance_status = status_response[0]['InstanceState']['Name']
    print('The main ETL instance was ' + ec2_instance_status + ' at ' + str(datetime.now()))

    # Check RDS Main instance status
    rds_status_response = rds_client.describe_db_instances(DBInstanceIdentifier='ticketsdb')
    rds_instance_status = rds_status_response['DBInstances'][0]['DBInstanceStatus']
    print('The main RDS MYSQL DB was ' + rds_instance_status + ' at ' + str(datetime.now()))


    # key_location = os.path.join(os.path.expanduser('~'), 'bin', 'Tickets_Key_5_Open.pub')
    key_location = "C:/Users/bswxj01/Desktop/Event_Ticket_Pricing/Key Files/Tickets Key 5 Open.pub"

    # if ec2_instance_status == 'running' and rds_instance_status == 'available':
    if ec2_instance_status == 'running':

        # Get ETL Main instance hostname
        hostname_response = ec2_resource.meta.client.describe_instances(InstanceIds=instance_ids)['Reservations'][0][
            'Instances']
        hostname = hostname_response[0]['PublicDnsName']

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # ssh.connect('ec2-52-37-11-38.us-west-2.compute.amazonaws.com', username='ubuntu', key_filename='C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Key Files/Tickets Key 5 Open.pub')
        ssh.connect(hostname, username='ubuntu', key_filename=key_location)

        stdin_seatgeek, stdout_seatgeek, stderr_seatgeek = ssh.exec_command('python3 ~/bin/SeatGeek_API.py > ~/bin/SeatGeek_log.txt')
        stdout_seatgeek.readlines()
        print('Seatgeek_API.py finished running at '+ str(datetime.now()))


        stdin_stubhub, stdout_stubhub, stderr_stubhub = ssh.exec_command('python3 ~/bin/Stubhub_API.py > ~/bin/Stubhub_log.txt')
        stdout_stubhub.readlines()
        print('Stubhub_API.py finished running at '+ str(datetime.now()))


        stdin_ticketmaster, stdout_ticketmaster, stderr_ticketmaster = ssh.exec_command('python3 ~/bin/Ticketmaster_API.py > ~/bin/Ticketmaster_log.txt')
        stdout_ticketmaster.readlines()
        print('Ticketmaster_API.py finished running at '+ str(datetime.now()))


        stdin_eventbrite, stdout_eventbrite, stderr_eventbrite = ssh.exec_command('python3 ~/bin/EventBrite_API.py > ~/bin/Eventbrite_log.txt')
        stdout_eventbrite.readlines()
        print('Eventbrite_API.py finished running at '+ str(datetime.now()))

        ssh.close()

    else:

        time.sleep(120)

        # Get ETL Main instance hostname
        hostname_response = ec2_resource.meta.client.describe_instances(InstanceIds=instance_ids)['Reservations'][0][
            'Instances']
        hostname = hostname_response[0]['PublicDnsName']

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # ssh.connect('ec2-52-37-11-38.us-west-2.compute.amazonaws.com', username='ubuntu', key_filename='C:/Users/wjack/Desktop/Event_Ticket_Pricing/Event_Ticket_Pricing/Key Files/Tickets Key 5 Open.pub')
        ssh.connect(hostname, username='ubuntu', key_filename=key_location)

        stdin_seatgeek, stdout_seatgeek, stderr_seatgeek = ssh.exec_command('python3 ~/bin/SeatGeek_API.py > ~/bin/SeatGeek_log.txt')
        stdout_seatgeek.readlines()
        print('Seatgeek_API.py finished running at '+ str(datetime.now()))


        stdin_stubhub, stdout_stubhub, stderr_stubhub = ssh.exec_command('python3 ~/bin/Stubhub_API.py > ~/bin/Stubhub_log.txt')
        stdout_stubhub.readlines()
        print('Stubhub_API.py finished running at '+ str(datetime.now()))


        stdin_ticketmaster, stdout_ticketmaster, stderr_ticketmaster = ssh.exec_command('python3 ~/bin/Ticketmaster_API.py > ~/bin/Ticketmaster_log.txt')
        stdout_ticketmaster.readlines()
        print('Ticketmaster_API.py finished running at '+ str(datetime.now()))


        stdin_eventbrite, stdout_eventbrite, stderr_eventbrite = ssh.exec_command('python3 ~/bin/EventBrite_API.py > ~/bin/Eventbrite_log.txt')
        stdout_eventbrite.readlines()
        print('Eventbrite_API.py finished running at '+ str(datetime.now()))


        ssh.close()


    # Stop ETL Main instance
    ec2_client.stop_instances(InstanceIds=instance_ids, DryRun=False)
    # rds_client.stop_db_instance(DBInstanceIdentifier = 'ticketsdb')

    # Check ETL Main instance status
    status_response = ec2_resource.meta.client.describe_instance_status(InstanceIds=instance_ids)['InstanceStatuses']
    if not status_response:
        print('The main ETL instance was stopped at ' + str(datetime.now()))
    else:
        print('The main ETL instance ended running')

    # Check RDS instance status
    rds_status_response = rds_client.describe_db_instances(DBInstanceIdentifier='ticketsdb')
    rds_instance_status = rds_status_response['DBInstances'][0]['DBInstanceStatus']
    if rds_instance_status == 'available':
        print('The main RDS MYSQL DB was running at ' + str(datetime.now()))
    else:
        print('The main RDS MYSQL DB was stopped at the end of the job')

test()



