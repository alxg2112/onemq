package com.alxg.onemq.pubsub;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.zeromq.ZMQ;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import com.alxg.onemq.SocketFactory;
import com.alxg.onemq.SocketMode;
import com.alxg.onemq.SocketProperties;
import com.alxg.onemq.ZmqEvent;

/**
 * @author Alexander Gryshchenko
 */
public abstract class PubSubSocketWrapper<T extends SocketProperties> implements AutoCloseable {

	protected static final byte[] SYNC_MESSAGE = new byte[0];
	private static final String MONITOR_ADDRESS = "inproc://monitor.socket";
	private static final int MONITOR_RECEIVE_TIMEOUT = 1000;

	protected final T properties;
	protected final ZMQ.Context context;
	protected final ZMQ.Socket socket;
	protected final ZMQ.Socket syncer;
	protected final ZMQ.Socket monitor;
	protected final ExecutorService monitorExecutor = Executors.newSingleThreadExecutor();
	protected final CountDownLatch monitorShutdown = new CountDownLatch(1);

	private final CountDownLatch handshakeEvent = new CountDownLatch(1);
	private final Subject<ZmqEvent, ZmqEvent> eventSubject = PublishSubject.create();

	protected volatile boolean closed = false;

	protected PubSubSocketWrapper(T properties) {
		this.properties = properties;
		this.context = ZMQ.context(properties.getIoThreads());
		this.socket = SocketFactory.newInstance(context, properties);
		this.syncer = properties.isSyncEnabled() ? createSyncer() : null;
		this.monitor = context.socket(ZMQ.PAIR);
	}

	protected abstract ZMQ.Socket createSyncer();

	protected void init() {
		initMonitor();
		startMonitoring();

		initSocket();

		if (properties.isSyncEnabled()) {
			awaitForHandshakeEvent();
			sync();
		}
	}

	private void initMonitor() {
		socket.monitor(MONITOR_ADDRESS, ZMQ.EVENT_ALL);
		monitor.setReceiveTimeOut(MONITOR_RECEIVE_TIMEOUT);
		monitor.connect(MONITOR_ADDRESS);
	}

	private void startMonitoring() {
		monitorExecutor.submit(() -> {
			while (!closed) {
				ZMQ.Event event = ZMQ.Event.recv(monitor);
				if (event == null) {
					continue;
				}
				int eventCode = event.getEvent();
				ZmqEvent zmqEvent = ZmqEvent.fromCode(eventCode);
				if (zmqEvent == ZmqEvent.HANDSHAKE_PROTOCOL) {
					handshakeEvent.countDown();
				}
				eventSubject.onNext(zmqEvent);
			}
			monitorShutdown.countDown();
		});
	}

	public Observable<ZmqEvent> events() {
		return eventSubject;
	}

	private void awaitForHandshakeEvent() {
		try {
			handshakeEvent.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	protected abstract void sync();

	private void initSocket() {
		if (properties.getSocketMode() == SocketMode.BIND) {
			socket.bind(properties.getMessageAddress());
		} else if (properties.getSocketMode() == SocketMode.CONNECT) {
			socket.connect(properties.getMessageAddress());
		}
	}
}
