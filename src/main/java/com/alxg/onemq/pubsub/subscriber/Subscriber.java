package com.alxg.onemq.pubsub.subscriber;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.util.Strings;
import org.zeromq.ZMQ;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import com.alxg.onemq.SocketMode;
import com.alxg.onemq.pubsub.PubSubSocketWrapper;

/**
 * Reactive ZeroMQ subscriber.
 * <p>
 * Enables guaranteed message delivery via synchronization feature.
 * <p>
 * This implementation is <b>thread-safe</b>.
 *
 * @author Alexander Gryshchenko
 */
@Log4j2
public class Subscriber extends PubSubSocketWrapper<SubscriberProperties> {

	private final Supplier<String> receiveMessage;
	private final Set<String> subscriptionTopics = Sets.newConcurrentHashSet();
	private final Queue<String> pendingTopicsToSubscribe = Queues.newConcurrentLinkedQueue();
	private final Queue<String> pendingTopicsToUnsubscribe = Queues.newConcurrentLinkedQueue();
	private final Subject<String, String> messageSubject = PublishSubject.create();
	private final ExecutorService messageConsumeExecutor = Executors.newSingleThreadExecutor();
	private final CountDownLatch socketShutdown = new CountDownLatch(1);
	private final AtomicBoolean started = new AtomicBoolean(false);
	private final AtomicReference<String> topicsPattern = new AtomicReference<>(null);

	public Subscriber(SubscriberProperties properties) {
		super(properties);
		subscriptionTopics.addAll(properties.getTopics());
		topicsPattern.set(createTopicsPattern(subscriptionTopics));
		receiveMessage = properties.isEnvelopeEnabled() ? receiveEnvelope() : receiveAtomic();
	}

	/**
	 * Start consuming messages from the dedicated topic.
	 * <p>
	 * If sync is enabled, it happens upon call to this method.
	 */
	public void start() {
		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("Subscriber has already been started");
		} else if (closed) {
			throw new IllegalStateException("Subscriber has been closed");
		}
		init();
		consumeMessages();
		LOGGER.debug("Started. {} to {}, topics: {}.",
				(properties.getSocketMode() == SocketMode.BIND ? "Bound" : "Connected"),
				properties.getMessageAddress(),
				properties.getTopics());
		if (!properties.isSyncEnabled()) {
			LOGGER.warn("Sync is disabled, some messages may be lost");
		}
	}

	public synchronized void addSubscription(String topic) {
		if (subscriptionTopics.contains(topic) || pendingTopicsToSubscribe.contains(topic)) {
			return;
		}
		pendingTopicsToSubscribe.add(topic);
	}

	public synchronized void removeSubscription(String topic) {
		if (!subscriptionTopics.contains(topic) || pendingTopicsToUnsubscribe.contains(topic)) {
			return;
		}
		pendingTopicsToUnsubscribe.add(topic);
	}

	public Set<String> getSubscriptionTopics() {
		return ImmutableSet.copyOf(subscriptionTopics);
	}

	@Override
	protected ZMQ.Socket createSyncer() {
		ZMQ.Socket syncer = context.socket(ZMQ.REQ);
		syncer.connect(properties.getSyncAddress());
		return syncer;
	}

	@Override
	protected void sync() {
		startPublishingHandshake();
		LOGGER.debug("Synced");
	}

	private void startPublishingHandshake() {
		socket.recv(ZMQ.NOBLOCK);
		syncer.send(SYNC_MESSAGE);
		syncer.recv();
		LOGGER.trace("Ready to consume messages");
	}

	private void consumeMessages() {
		messageConsumeExecutor.submit(() -> {
			while (!closed) {
				checkForSubscriptionChanges();
				String message = receiveMessage.get();
				if (message == null) {
					continue;
				}
				LOGGER.trace("Received message: '{}'", message);
				messageSubject.onNext(message);
			}
			socketShutdown.countDown();
		});
	}

	private void checkForSubscriptionChanges() {
		if (!pendingTopicsToSubscribe.isEmpty()) {
			synchronized (this) {
				String pendingTopicToSubscribe;
				while ((pendingTopicToSubscribe = pendingTopicsToSubscribe.poll()) != null) {
					subscriptionTopics.add(pendingTopicToSubscribe);
					socket.subscribe(pendingTopicToSubscribe);
				}
				topicsPattern.set(createTopicsPattern(subscriptionTopics));
			}
		}
		if (!pendingTopicsToUnsubscribe.isEmpty()) {
			synchronized (this) {
				String pendingTopicToUnsubscribe;
				while ((pendingTopicToUnsubscribe = pendingTopicsToUnsubscribe.poll()) != null) {
					subscriptionTopics.add(pendingTopicToUnsubscribe);
					socket.unsubscribe(pendingTopicToUnsubscribe);
				}
				topicsPattern.set(createTopicsPattern(subscriptionTopics));
			}
		}
	}

	private Supplier<String> receiveEnvelope() {
		return () -> {
			String topic = socket.recvStr();
			if (topic == null) {
				return null;
			}
			return socket.recvStr();
		};
	}

	private Supplier<String> receiveAtomic() {
		return () -> {
			String prefixedMessage = socket.recvStr();
			if (prefixedMessage == null) {
				return null;
			}
			return prefixedMessage.replaceFirst(topicsPattern.get(), Strings.EMPTY);
		};
	}

	private String createTopicsPattern(Set<String> subscriptionTopics) {
		return '(' + Joiner.on('|').join(subscriptionTopics) + ')';
	}

	public Observable<String> messages() {
		return messageSubject;
	}

	public void close() throws Exception {
		closed = true;
		socketShutdown.await();
		socket.close();
		LOGGER.debug("Closed subscriber socket");
		messageConsumeExecutor.shutdown();
		if (syncer != null) {
			syncer.close();
		}
		monitorShutdown.await();
		monitor.close();
		monitorExecutor.shutdown();
		context.term();
		LOGGER.debug("Terminated context");
	}
}
