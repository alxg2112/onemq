package com.alxg.onemq;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.log4j.Log4j2;

import com.alxg.onemq.pubsub.subscriber.Subscriber;
import com.alxg.onemq.pubsub.subscriber.SubscriberProperties;

/**
 * @author Alexander Gryshchenko
 */
@Log4j2
public class SubscriberTest {

	private static final int IO_THREADS = 4;
	private static final String TOPIC_NAME = "nginx";
	private static final String MESSAGE_ADDRESS = "tcp://0.0.0.0:5563";
	private static final int MESSAGE_RECEIVE_TIMEOUT_MILLIS = 1000;
	private static final SocketMode SUBSCRIBER_SOCKET_MODE = SocketMode.BIND; // log_zmq module seems to use connect, so we bind
	private static final int INCOMING_MESSAGES_COUNT = 100_000;
	private static final int REPORT_INTERVAL_SECONDS = 1;


	public static void main(String[] args) throws Exception {
		SubscriberProperties subscriberProperties = new SubscriberProperties();
		subscriberProperties.setIoThreads(IO_THREADS);
		subscriberProperties.setTopics(Collections.emptySet());
		subscriberProperties.setMessageAddress(MESSAGE_ADDRESS);
		subscriberProperties.setReceiveTimeoutMillis(MESSAGE_RECEIVE_TIMEOUT_MILLIS);
		subscriberProperties.setSocketMode(SUBSCRIBER_SOCKET_MODE);

		try (Subscriber subscriber = new Subscriber(subscriberProperties)) {
			CountDownLatch messagesRemaining = new CountDownLatch(INCOMING_MESSAGES_COUNT);
			CountDownLatch startTimer = new CountDownLatch(1);

			subscriber.messages().subscribe(msg -> {
				startTimer.countDown();
				messagesRemaining.countDown();
			});

			subscriber.start();
			subscriber.addSubscription(TOPIC_NAME);

			startTimer.await();
			long start = System.currentTimeMillis();

			AtomicLong messagesConsumedAtMomentOfLastReport = new AtomicLong(0);

			ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
			scheduledExecutorService.scheduleAtFixedRate(() -> {
				long messagesConsumed = INCOMING_MESSAGES_COUNT - messagesRemaining.getCount();
				long messagesConsumedSinceLastReport = messagesConsumed - messagesConsumedAtMomentOfLastReport.get();
				messagesConsumedAtMomentOfLastReport.set(messagesConsumed);
				double throughputMsgPerSec = 1D * messagesConsumedSinceLastReport / REPORT_INTERVAL_SECONDS;
				LOGGER.debug("Throughput (last {} seconds): {} msg/sec, messages consumed: {}, remaining: {}",
						REPORT_INTERVAL_SECONDS,
						throughputMsgPerSec,
						messagesConsumed,
						messagesRemaining.getCount());
			}, REPORT_INTERVAL_SECONDS, REPORT_INTERVAL_SECONDS, TimeUnit.SECONDS);

			messagesRemaining.await();
			scheduledExecutorService.shutdown();

			long finish = System.currentTimeMillis();
			LOGGER.debug("[Final report] Throughput: {} msg/sec, messages consumed: {}",
					1000D * INCOMING_MESSAGES_COUNT / (finish - start),
					INCOMING_MESSAGES_COUNT);
		}
	}
}
