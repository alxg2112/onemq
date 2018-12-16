package com.alxg.onemq.pubsub.publisher;

import lombok.Data;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.alxg.onemq.SocketProperties;

/**
 * @author Alexander Gryshchenko
 */
@Data
public class PublisherProperties extends SocketProperties {

	private int sendTimeoutMillis = DEFAULT_SEND_TIMEOUT;
	private int sendQueueSize = DEFAULT_SEND_QUEUE_SIZE;
	private String topic;

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
