#!/usr/bin/python
# -*- coding: utf-8 -*-

import boto3
import os


def lambda_handler(event, context):
    return deleteMyData(event['timestamps'])


def insertInMyData(timestamps, sensor_data, dyn_resource=None):

    if dyn_resource is None:
        dyn_resource = boto3.resource('dynamodb')

    table = dyn_resource.Table('MyData2')
    return table.put_item(Item={'timestamps': timestamps,
                          'sensor_data': sensor_data})


def updateMyData(timestamps, sensor_data, dyn_resource=None):

    if dyn_resource is None:
        dyn_resource = boto3.resource('dynamodb')

    table = dyn_resource.Table('MyData2')
    return table.update_item(Key={'timestamps': timestamps},
                             UpdateExpression='set sensor_data=:d',
                             ExpressionAttributeValues={':d': sensor_data},
                             ReturnValues='UPDATED_NEW')


def queryMyData(timestamps, dyn_resource=None):

    if dyn_resource is None:
        dyn_resource = boto3.resource('dynamodb')

    table = dyn_resource.Table('MyData2')
    return table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('timestamps'
                       ).eq(timestamps))


def deleteMyData(timestamps, dyn_resource=None):

    if dyn_resource is None:
        dyn_resource = boto3.resource('dynamodb')

    table = dyn_resource.Table('MyData2')
    return table.delete_item(Key={'timestamps': timestamps})
