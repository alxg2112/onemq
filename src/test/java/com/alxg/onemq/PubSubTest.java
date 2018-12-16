package com.alxg.onemq;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import com.google.common.collect.Lists;
import lombok.extern.log4j.Log4j2;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.alxg.onemq.pubsub.publisher.Publisher;
import com.alxg.onemq.pubsub.publisher.PublisherProperties;
import com.alxg.onemq.pubsub.subscriber.Subscriber;
import com.alxg.onemq.pubsub.subscriber.SubscriberProperties;

/**
 * @author Alexander Gryshchenko
 */
@Log4j2
@RunWith(Parameterized.class)
public class PubSubTest {

	private static final String TOPIC_NAME = "A";
	private static final String TOPIC_CONN_STRING = "tcp://localhost:5562";
	private static final String SYNC_CONN_STRING = "tcp://localhost:5561";
	private static final int IO_THREADS = 4;
	private static final int MESSAGE_COUNT = 1_000_000;
	private static final int SEND_TIMEOUT_MILLIS = 1000;
	private static final int RECEIVE_TIMEOUT_MILLIS = 1000;
	private static final boolean SYNC_ENABLED = true;
	private static final boolean ENVELOPE_ENABLED = false;

	private static final int TEST_RUNS_PER_MODE = 5;
	private static final long TEST_TIMEOUT_MILLIS = 10_000L;

	private final Subscriber subscriber;
	private final Publisher publisher;

	@Parameterized.Parameters
	public static List<Object[]> params() {
		Object[] subscriberConnectsPublisherBinds = {SocketMode.CONNECT, SocketMode.BIND};
		Object[] subscriberBindsPublisherConnects = {SocketMode.BIND, SocketMode.CONNECT};
		List<Object[]> params = Lists.newArrayList();
		params.addAll(Collections.nCopies(TEST_RUNS_PER_MODE, subscriberConnectsPublisherBinds));
		params.addAll(Collections.nCopies(TEST_RUNS_PER_MODE, subscriberBindsPublisherConnects));
		return params;
	}

	public PubSubTest(SocketMode subscriberSocketMode, SocketMode publisherSocketMode) {
		LOGGER.debug("Subscriber socket mode: {}, publisher socket mode: {}",
				subscriberSocketMode,
				publisherSocketMode);
		this.subscriber = new Subscriber(subscriberProperties(subscriberSocketMode));
		this.publisher = new Publisher(publisherProperties(publisherSocketMode));
	}

	@Test(timeout = TEST_TIMEOUT_MILLIS)
	public void test() throws ExecutionException, InterruptedException {

		CountDownLatch messagesPending = new CountDownLatch(MESSAGE_COUNT);

		Runnable subscribe = () -> {
			subscriber.messages().subscribe(msg -> {
				messagesPending.countDown();
				LOGGER.trace("Messages remaining: {}", messagesPending.getCount());
			});
			subscriber.start();
			try {
				messagesPending.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				subscriber.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		CountDownLatch publisherStarted = new CountDownLatch(1);

		Runnable publish = () -> {
			publisher.start();
			publisherStarted.countDown();

			for (int i = 0; i < MESSAGE_COUNT; i++) {
				while (!publisher.send(String.valueOf(i))) {
					// Try to send
				}
			}
			try {
				messagesPending.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			try {
				publisher.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		};

		ExecutorService executorService = Executors.newFixedThreadPool(2);

		List<Future<?>> futures = Lists.newArrayList();
		for (Runnable runnable : Arrays.asList(subscribe, publish)) {
			futures.add(executorService.submit(runnable));
		}

		publisherStarted.await();
		long start = System.currentTimeMillis();

		for (Future<?> future : futures) {
			future.get();
		}
		long finish = System.currentTimeMillis();
		LOGGER.debug("Throughput: {} msg/sec", 1000D * MESSAGE_COUNT / (finish - start));
		executorService.shutdown();
	}

	@After
	public void cleanup() throws Exception {
		subscriber.close();
		publisher.close();
	}

	private SubscriberProperties subscriberProperties(SocketMode socketMode) {
		SubscriberProperties subscriberProperties = new SubscriberProperties();
		subscriberProperties.setIoThreads(IO_THREADS);
		subscriberProperties.setTopics(Collections.singleton(TOPIC_NAME));
		subscriberProperties.setMessageAddress(TOPIC_CONN_STRING);
		subscriberProperties.setSyncAddress(SYNC_CONN_STRING);
		subscriberProperties.setReceiveTimeoutMillis(RECEIVE_TIMEOUT_MILLIS);
		subscriberProperties.setSyncEnabled(SYNC_ENABLED);
		subscriberProperties.setEnvelopeEnabled(ENVELOPE_ENABLED);
		subscriberProperties.setSocketMode(socketMode);
		return subscriberProperties;
	}

	private PublisherProperties publisherProperties(SocketMode socketMode) {
		PublisherProperties publisherProperties = new PublisherProperties();
		publisherProperties.setIoThreads(IO_THREADS);
		publisherProperties.setTopic(TOPIC_NAME);
		publisherProperties.setMessageAddress(TOPIC_CONN_STRING);
		publisherProperties.setSyncAddress(SYNC_CONN_STRING);
		publisherProperties.setSendTimeoutMillis(SEND_TIMEOUT_MILLIS);
		publisherProperties.setSyncEnabled(SYNC_ENABLED);
		publisherProperties.setEnvelopeEnabled(ENVELOPE_ENABLED);
		publisherProperties.setSocketMode(socketMode);
		return publisherProperties;
	}
}
