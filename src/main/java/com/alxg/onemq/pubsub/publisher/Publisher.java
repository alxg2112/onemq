package com.alxg.onemq.pubsub.publisher;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import lombok.extern.log4j.Log4j2;
import org.zeromq.ZMQ;

import com.alxg.onemq.SocketMode;
import com.alxg.onemq.pubsub.PubSubSocketWrapper;

/**
 * ZeroMQ publisher.
 * <p>
 * Enables guaranteed message delivery via synchronization feature.
 * <p>
 * This implementation is <b>thread-safe</b>.
 *
 * @author Alexander Gryshchenko
 */
@Log4j2
public class Publisher extends PubSubSocketWrapper<PublisherProperties> {

	private final Function<String, Boolean> sendMessage;
	private final AtomicBoolean started = new AtomicBoolean(false);
	private volatile boolean ready = false;

	public Publisher(PublisherProperties properties) {
		super(properties);
		this.sendMessage = properties.isEnvelopeEnabled() ? sendEnvelope() : sendAtomic();
	}

	/**
	 * This method must be called before sending messages via this {@link Publisher}.
	 * <p>
	 * If sync is enabled, it happens upon call to this method.
	 */
	public void start() {
		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("Publisher has already been started");
		} else if (closed) {
			throw new IllegalStateException("Publisher has been closed");
		}
		init();
		ready = true;
		LOGGER.debug("Started. {} to {}, topic: {}.",
				(properties.getSocketMode() == SocketMode.BIND ? "Bound" : "Connected"),
				properties.getMessageAddress(),
				properties.getTopic());
		if (!properties.isSyncEnabled()) {
			LOGGER.warn("Sync is disabled, some messages may be lost");
		}
	}

	@Override
	protected ZMQ.Socket createSyncer() {
		ZMQ.Socket syncer = context.socket(ZMQ.REP);
		syncer.bind(properties.getSyncAddress());
		return syncer;
	}

	@Override
	protected void sync() {
		startPublishingHandshake();
		LOGGER.debug("Synced");
	}

	private void startPublishingHandshake() {
		syncer.recv();
		syncer.send(SYNC_MESSAGE);
		LOGGER.trace("Ready to publish messages");
	}

	/**
	 * Publish message to the dedicated topic.
	 *
	 * @param message a message to send
	 * @return whether message was sent successfully
	 */
	public synchronized boolean send(String message) {
		if (!started.get()) {
			throw new IllegalStateException("Publisher has been not started yet");
		}

		if (!ready) {
			throw new IllegalArgumentException("Publisher is not ready yet");
		}

		if (closed) {
			throw new IllegalStateException("Publisher has already been closed");
		}

		return sendMessage.apply(message);
	}

	private Function<String, Boolean> sendEnvelope() {
		return message -> {
			if (!socket.sendMore(properties.getTopic())) {
				return false;
			}
			if (!socket.send(message)) {
				return false;
			}
			LOGGER.trace("Sent message '{}'", message);
			return true;
		};
	}

	private Function<String, Boolean> sendAtomic() {
		return message -> {
			if (!socket.send(properties.getTopic() + message)) {
				return false;
			}
			LOGGER.trace("Sent message '{}'", message);
			return true;
		};
	}

	@Override
	public synchronized void close() throws Exception {
		closed = true;
		socket.close();
		LOGGER.debug("Closed publisher socket");
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
