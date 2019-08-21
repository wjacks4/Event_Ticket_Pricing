# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 14:08:10 2019

@author: Will Jackson
"""

import boto3
import sys

ec2 = boto3.client('ec2')

# test instance stopping for the t2 micro instance
ids = ['i-043080e892586e1d2']

# Do a dryrun first to verify permissions
# ec2.start_instances(InstanceIds=ids, DryRun=True)

ec2.start_instances(InstanceIds=ids, DryRun=False)
