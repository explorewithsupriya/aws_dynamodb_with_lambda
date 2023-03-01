package com.test.documentdbwithJava.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.net.URI;
import java.util.HashMap;

import java.util.Map;
import java.util.Set;

// Handler value: example.Handler
public class LambdaHandler implements RequestHandler<Map<String, String>, Void> {

	@Override
	public Void handleRequest(Map<String, String> event, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("ENVIRONMENT VARIABLES: " + System.getenv());
		logger.log("EVENT: " + event);
		logger.log("EVENT TYPE: " + event.getClass());

		DynamoDbClient client = DynamoDbClient.builder().region(Region.AP_SOUTH_1)
				.credentialsProvider(StaticCredentialsProvider
						.create(AwsBasicCredentials.create(System.getenv("aws_key"), System.getenv("aws_pass"))))
				.build();
		System.out.println(client.serviceName());
		if (null != event.get("actionType") && event.get("actionType").toString().equals("update")) {
			updateTableItem(client, event.get("tableName").toString(), event.get("keyAttributeName").toString(),
					event.get("keyAttributeValue").toString(), event.get("targetAttributeName").toString(),
					event.get("targetAttributeValue").toString());
		} else {
			scanItems(client, event.get("tableName").toString());
		}
		return null;
	}

	public static void updateTableItem(DynamoDbClient ddb, String tableName, String key, String keyVal, String name,
			String updateVal) {

		HashMap<String, AttributeValue> itemKey = new HashMap<>();
		itemKey.put(key, AttributeValue.builder().s(keyVal).build());

		HashMap<String, AttributeValueUpdate> updatedValues = new HashMap<>();
		updatedValues.put(name, AttributeValueUpdate.builder().value(AttributeValue.builder().s(updateVal).build())
				.action(AttributeAction.PUT).build());

		UpdateItemRequest request = UpdateItemRequest.builder().tableName(tableName).key(itemKey)
				.attributeUpdates(updatedValues).build();

		try {
			ddb.updateItem(request);
		} catch (ResourceNotFoundException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		} catch (DynamoDbException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		System.out.println("The Amazon DynamoDB table was updated!");
	}

	public static void scanItems(DynamoDbClient ddb, String tableName) {

		try {
			ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();

			ScanResponse response = ddb.scan(scanRequest);
			for (Map<String, AttributeValue> item : response.items()) {
				Set<String> keys = item.keySet();
				for (String key : keys) {
					System.out.println("The key name is " + key + "\n");
					System.out.println("The value is " + item.get(key).s());
				}
			}

		} catch (DynamoDbException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
