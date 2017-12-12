#!/usr/bin/env python
"""Script to upload packages to s3 and notify repoupdate-daemon."""
import os
import optparse
import boto
import boto.sns
import boto.sqs
import boto.sqs.message
from boto.sqs.jsonmessage import json
import time
import sys

parser = optparse.OptionParser()
parser.add_option('--bucket', default='packages.example.com')
parser.add_option('--repopath', default='development/x86_64')
parser.add_option('--region', default='us-east-1')
parser.add_option('--sns-topic', default='arn:aws:sns:us-east-1:123:packages-new')
options, args = parser.parse_args()

sns = boto.sns.connect_to_region(options.region)
bucket = boto.s3.connect_to_region(options.region).get_bucket(options.bucket, validate=False)

sqs = boto.sqs.connect_to_region(options.region)
status_queue = sqs.create_queue('myqueue')
sqs.set_queue_attribute(status_queue, 'Policy', '''{
  "Version": "2012-10-17",
  "Id": "arn:aws:sqs:us-east-1:317085423413:myqueue/SQSDefaultPolicy",
  "Statement": [
    {
      "Sid": "Sid1513085565204",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "SQS:SendMessage",
      "Resource": "arn:aws:sqs:us-east-1:317085423413:myqueue",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:sns:us-east-1:317085423413:repo_status"
        }
      }
    }
  ]
}''')

status_queue.set_message_class(boto.sqs.message.RawMessage)

subscription = sns.subscribe('arn:aws:sns:us-east-1:317085423413:repo_status', 'sqs', status_queue.arn)
subscription_arn = subscription['SubscribeResponse']['SubscribeResult']['SubscriptionArn']

rpms = []

for rpmfile in args:
    filename = os.path.split(rpmfile)[1]
    key = bucket.new_key(os.path.join(options.repopath, filename))
    key.set_contents_from_filename(rpmfile)
    rpms.append(filename)
    sns.publish(options.sns_topic, filename, options.repopath)
exit_status = 0

while len(rpms) > 0:
    messages = status_queue.get_messages(10, 10)
    for m in messages:
        body = json.loads(m.get_body())
        status = body['Subject']
        rpmfile = body['Message']
        if status == 'ok' and rpmfile in rpms:
            print "file %s published successful" % (rpmfile)
            rpms.remove(rpmfile)
        if status == 'nok' and rpmfile in rpms:
            print "file %s published unsuccessful" % (rpmfile)
            rpms.remove(rpmfile)
            exit_status = exit_status + 1

        sqs.delete_message(status_queue,m)
    time.sleep(10)

sns.unsubscribe(subscription_arn)
sqs.delete_queue(status_queue)

sys.exit(exit_status)
