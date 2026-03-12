package com.amazonaws.services.dynamodbv2.streamsadapter.ddblocal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import com.amazonaws.services.dynamodbv2.streamsadapter.adapter.DynamoDBStreamsClientRecord;
import com.amazonaws.services.dynamodbv2.streamsadapter.adapter.DynamoDBStreamsGetRecordsResponseAdapter;

/**
 * Integration tests for the DynamoDB Streams Kinesis Adapter APIs using DynamoDB Local.
 * These tests verify end-to-end adapter behavior against a real DynamoDB instance,
 * complementing the unit tests which use mocks.
 */
public class DynamoDBStreamsApiIntegrationTest extends DynamoDBStreamsLocalTestBase {

    @Test
    public void testDynamoDBStreamApisSanity() throws Exception {
        final String tableName = "SanityTable";
        String streamArn = createTableWithStream(tableName);
        assertNotNull(streamArn);

        putItem(tableName, "1", "hello");
        putItem(tableName, "2", "world");

        // ListStreams
        ListStreamsResponse listResponse = adapterClient.listStreams(
                software.amazon.awssdk.services.kinesis.model.ListStreamsRequest.builder().build()).get();
        assertNotNull(listResponse.streamNames());
        assertFalse(listResponse.streamNames().isEmpty());

        // DescribeStream
        DescribeStreamResponse describeResponse = adapterClient.describeStream(
                software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder()
                        .streamName(streamArn).build()).get();
        assertNotNull(describeResponse.streamDescription());
        assertFalse(describeResponse.streamDescription().shards().isEmpty());

        // GetShardIterator
        String shardId = describeResponse.streamDescription().shards().get(0).shardId();
        GetShardIteratorResponse iteratorResponse = adapterClient.getShardIterator(
                software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest.builder()
                        .streamName(streamArn)
                        .shardId(shardId)
                        .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                        .build()).get();
        assertNotNull(iteratorResponse.shardIterator());

        // GetDynamoDBStreamsRecords
        DynamoDBStreamsGetRecordsResponseAdapter recordsResponse = adapterClient.getDynamoDBStreamsRecords(
                software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest.builder()
                        .shardIterator(getShardIteratorFromStreams(streamArn))
                        .build()).get();
        assertNotNull(recordsResponse);
        assertEquals(2, recordsResponse.records().size());
    }

    @Test
    public void testGetDynamoDBStreamsGetRecords() throws Exception {
        final String tableName = "GetRecordsTestTable";
        String streamArn = createTableWithStream(tableName);
        putItem(tableName, "key1", "val1");

        String shardIterator = getShardIteratorFromStreams(streamArn);
        DynamoDBStreamsGetRecordsResponseAdapter response = adapterClient.getDynamoDBStreamsRecords(
                software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest.builder()
                        .shardIterator(shardIterator)
                        .build()).get();

        assertEquals(1, response.records().size());

        KinesisClientRecord kinesisRecord = response.records().get(0);
        assertTrue(kinesisRecord instanceof DynamoDBStreamsClientRecord);
        DynamoDBStreamsClientRecord clientRecord = (DynamoDBStreamsClientRecord) kinesisRecord;

        assertEquals("key1", clientRecord.getRecord().dynamodb().keys().get("id").s());
        assertEquals("val1", clientRecord.getRecord().dynamodb().newImage().get("data").s());
        assertNotNull(clientRecord.sequenceNumber());
        assertNotNull(clientRecord.approximateArrivalTimestamp());
        assertNotNull(response.millisBehindLatest());
        assertTrue(response.millisBehindLatest() >= 0);
        assertNotNull(response.nextShardIterator());
    }

    @Test
    public void testDescribeStreamResponseMapping() throws Exception {
        final String tableName = "MappingTable";
        String streamArn = createTableWithStream(tableName);

        DescribeStreamResponse response = adapterClient.describeStream(
                software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder()
                        .streamName(streamArn).build()).get();

        assertEquals(streamArn, response.streamDescription().streamName());
        assertEquals("ENABLED", response.streamDescription().streamStatusAsString());

        assertFalse(response.streamDescription().shards().isEmpty());
        software.amazon.awssdk.services.kinesis.model.Shard shard =
                response.streamDescription().shards().get(0);
        assertNotNull(shard.shardId());
        assertNotNull(shard.sequenceNumberRange());
        assertNotNull(shard.sequenceNumberRange().startingSequenceNumber());
        assertNotNull(shard.hashKeyRange());

        assertFalse(response.streamDescription().hasMoreShards());
    }

    @Test
    public void testGetRecordsThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () ->
                adapterClient.getRecords(
                        software.amazon.awssdk.services.kinesis.model.GetRecordsRequest.builder()
                                .shardIterator("any-iterator")
                                .build()));
    }

    @Test
    public void testDescribeStreamWithInvalidArn() {
        assertThrows(ResourceNotFoundException.class, () -> {
            try {
                adapterClient.describeStream(
                        software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder()
                                .streamName("arn:aws:dynamodb:us-east-1:123456789012:table/NonExistent/stream/2024-01-01T00:00:00.000")
                                .build()).get();
            } catch (java.util.concurrent.ExecutionException e) {
                throw e.getCause();
            }
        });
    }
}
