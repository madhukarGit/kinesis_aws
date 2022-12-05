package com.sdk.aws.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdk.aws.domain.OrderGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.util.HashMap;
import java.util.Map;

public class KinesisProducerIndividualRecords {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisProducerIndividualRecords.class);

    @Autowired
    private ObjectMapper objectMapper;

    public void produceIndividualRecord(){
        LOG.info("Starting PutRecord Producer..");

        var client = KinesisClient.builder().build();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            LOG.info("Shutting down program");
            client.close();
        },"Producer-shutdown"));

        Map<String,String> partitionSequences =new HashMap<>();
        int i = 0;
        while (i < 10){
            var order = OrderGenerator.makeOrder();
            LOG.info(String.format("Generated %s",order));
            try{
                var partitionKey =order.getSellerID();
                var builder = PutRecordRequest.builder()
                        .partitionKey(partitionKey)
                        .streamName("order-stream")
                        .data(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(order)));
                if(partitionSequences.containsKey(partitionKey)){
                    builder =builder.sequenceNumberForOrdering(partitionSequences.get(partitionKey));
                }
                var putRequest =builder.build();
                PutRecordResponse response = client.putRecord(putRequest);
                partitionSequences.put(partitionKey,response.sequenceNumber());
                LOG.info(String.format("Produced Record %s to Shard %s", response.sequenceNumber(), response.shardId()));
                i++;
            }catch (Exception e){

            }

            try{
                Thread.sleep(300);
            }catch (InterruptedException e){

            }
        }
    }
}
