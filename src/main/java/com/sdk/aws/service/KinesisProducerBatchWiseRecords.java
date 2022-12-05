package com.sdk.aws.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdk.aws.domain.Order;
import com.sdk.aws.domain.OrderGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class KinesisProducerBatchWiseRecords {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisProducerBatchWiseRecords.class);

    @Autowired
    private ObjectMapper objectMapper;

    public void processRecordsBatchWise(){
        LOG.info("Starting process of Batch wise records Producer");
        var client = KinesisClient.builder().build();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            LOG.info("Shutting down program");
            client.close();
        },"producer-shutdown"));

        Map<PutRecordsRequestEntry, Order> requestEntryOrderMap = new LinkedHashMap<>();
        int i = 0;
        while(i < 50){
            var order = OrderGenerator.makeOrder();
            LOG.info("Generated %s", order);
            try{
                var entry = PutRecordsRequestEntry.builder()
                        .partitionKey(order.getOrderID())
                        .data(SdkBytes.fromByteArray(objectMapper.writeValueAsBytes(order)))
                        .build();
                requestEntryOrderMap.put(entry,order);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            if(requestEntryOrderMap.size() > 20){
                List<PutRecordsRequestEntry> entries = new ArrayList<>(requestEntryOrderMap.keySet());
                var putRequest = PutRecordsRequest.builder()
                        .streamName("order-stream")
                        .records(entries)
                        .build();
                PutRecordsResponse response = null;
                try{
                    response = client.putRecords(putRequest);
                }catch (KinesisException e){
                    continue;
                }

                int k = 0;
                for(PutRecordsResultEntry entry:response.records()){
                    PutRecordsRequestEntry requestEntry = entries.get(k++);
                    Order o = requestEntryOrderMap.get(requestEntry);
                    if(entry.errorCode() != null){
                        LOG.error(String.format("failed to produce %s ",o));
                    }else {
                        LOG.info(String.format("Produced %s sequence %s to shard %s ",o,entry.sequenceNumber()
                        ,entry.shardId()));
                    }
                }
                requestEntryOrderMap.clear();

                try{
                    Thread.sleep(300);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
            i++;
        }
    }
}
