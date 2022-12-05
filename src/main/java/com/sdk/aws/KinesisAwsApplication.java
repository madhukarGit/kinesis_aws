package com.sdk.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdk.aws.domain.OrderGenerator;
import com.sdk.aws.service.KPLProducerKinesis;
import com.sdk.aws.service.KinesisProducerBatchWiseRecords;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class KinesisAwsApplication implements CommandLineRunner {

	@Autowired
	private KinesisProducerBatchWiseRecords kinesisProducerBatchWiseRecords;

	private static final Logger LOG = LoggerFactory.getLogger(KinesisAwsApplication.class);

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private KPLProducerKinesis kplProducerKinesis;

	public static void main(String[] args) {
		SpringApplication.run(KinesisAwsApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		kplProducerKinesis.kplProducerStreamKineses();
	}
}
