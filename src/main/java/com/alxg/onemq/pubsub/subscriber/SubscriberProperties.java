package com.alxg.onemq.pubsub.subscriber;

import java.util.HashSet;
import java.util.Set;

import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.alxg.onemq.SocketProperties;

/**
 * @author Alexander Gryshchenko
 */
@Data
public class SubscriberProperties extends SocketProperties {

	private int receiveTimeoutMillis = DEFAULT_SEND_TIMEOUT;
	private int receiveQueueSize = DEFAULT_RECEIVE_QUEUE_SIZE;
	private Set<String> topics = new HashSet<>();

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
