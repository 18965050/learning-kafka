package com.demo.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class MultiThreadHLConsumer {

	private ExecutorService			executor;
	private final ConsumerConnector	consumer;
	private final String			topic;

	public MultiThreadHLConsumer(String zookeeper, String groupId, String topic) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "largest");
		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
		this.topic = topic;
	}

	public void testConsumer(int threadCount) {
		Map<String, Integer> topicCount = new HashMap<String, Integer>();
		// Define thread count for each topic
		topicCount.put(topic, new Integer(threadCount));
		// Here we have used a single topic but we can also add
		// multiple topics to topicCount MAP
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
		List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
		// Launching the thread pool
		executor = Executors.newFixedThreadPool(threadCount);
		// Creating an object messages consumption
		int count = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			final int threadNumber = count;
			executor.submit(new Runnable() {
				public void run() {
					ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
					while (consumerIte.hasNext())
						System.out.println("Thread Number " + threadNumber + ": "
								+ new String(consumerIte.next().message()));
					System.out.println("Shutting down Thread Number: " + threadNumber);
				}
			});
			count++;
		}
		// if (consumer != null)
		// consumer.shutdown();
		// if (executor != null)
		// executor.shutdown();
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public static void main(String[] args) {
		String topic = "partitiontopic";
		int threadCount = 3;
		MultiThreadHLConsumer simpleHLConsumer = new MultiThreadHLConsumer("test-213:2181", "testgroup", topic);
		simpleHLConsumer.testConsumer(threadCount);

		try {
			Thread.sleep(10000);
		} catch (InterruptedException ie) {
		}
		simpleHLConsumer.shutdown();
	}

}
