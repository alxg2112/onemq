package com.alxg.onemq;

import java.util.Objects;

import org.zeromq.ZMQ;

import com.alxg.onemq.pubsub.publisher.PublisherProperties;
import com.alxg.onemq.pubsub.subscriber.SubscriberProperties;

/**
 * @author Alexander Gryshchenko
 */
public class SocketFactory {

	public static ZMQ.Socket newInstance(ZMQ.Context context, SocketProperties properties) {
		ZMQ.Socket socket;

		if (properties instanceof SubscriberProperties) {
			SubscriberProperties subscriberProperties = (SubscriberProperties) properties;
			socket = context.socket(ZMQ.SUB);
			socket.setRcvHWM(subscriberProperties.getReceiveQueueSize());
			socket.setReceiveTimeOut(subscriberProperties.getReceiveTimeoutMillis());
			subscriberProperties.getTopics().stream()
					.filter(Objects::nonNull)
					.forEach(socket::subscribe);
		} else if (properties instanceof PublisherProperties) {
			PublisherProperties publisherProperties = (PublisherProperties) properties;
			socket = context.socket(ZMQ.PUB);
			socket.setSndHWM(publisherProperties.getSendQueueSize());
			socket.setSendTimeOut(publisherProperties.getSendTimeoutMillis());
		} else {
			throw new IllegalArgumentException("Cannot create socket from given properties instance, class "
					+ properties.getClass() + " is not supported.");
		}

		return socket;
	}

	private SocketFactory() {

	}
}
