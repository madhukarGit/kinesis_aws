package com.sdk.aws.service;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.sdk.aws.domain.OrderGenerator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Service
public class KPLProducerKinesis {
    private static final Logger LOG = LoggerFactory.getLogger(KPLProducerKinesis.class);

    @Autowired
    private ObjectMapper objectMapper;

    public void kplProducerStreamKineses(){
        LOG.info("Starting kinesis Producer Library app");

        var producerConfig = new KinesisProducerConfiguration().setRegion(Region.US_EAST_1.toString());
        producerConfig.setAggregationEnabled(false);
        var producer = new KinesisProducer(producerConfig);
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            LOG.info("Shutting down program");
            producer.flush();
        },"producer-shutdown"));

        List<Future<UserRecordResult>> putFutures = new LinkedList<>();
        int i = 0;
        while (true){
            var order = OrderGenerator.makeOrder();
            LOG.info(String.format("Generated %s",order));

            ByteBuffer data = null;
            try{
                data = ByteBuffer.wrap(objectMapper.writeValueAsBytes(order));
            }catch (JsonParseException joe){

            } catch (JsonProcessingException e) {
                continue;
            }
            ListenableFuture<UserRecordResult> future = producer
                    .addUserRecord("kinesis_producer_library_stream",order.getOrderID(),data);
            putFutures.add(future);
            Futures.addCallback(future, new FutureCallback<UserRecordResult>() {
                @Override
                public void onSuccess(@Nullable UserRecordResult userRecordResult) {
                    LOG.info(String.format("Produced user record to shard %s at position %s",userRecordResult.getShardId(),
                            userRecordResult.getSequenceNumber()));
                }

                @Override
                public void onFailure(Throwable throwable) {
                    LOG.error("Failed");
                }
            }, MoreExecutors.directExecutor());

            if(++i % 100 == 0){
                try{
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
