package com.alxg.onemq;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Alexander Gryshchenko
 */
@AllArgsConstructor
@Getter
public enum ZmqEvent {
	CONNECTED(1, "Connection established"),
	CONNECT_DELAYED(1 << 1, "Synchronous connect failed, it's being polled"),
	CONNECT_RETRIED(1 << 2, "Asynchronous connect / reconnection attempt"),
	LISTENING(1 << 3, "Socket bound to an address, ready to accept connections"),
	BIND_FAILED(1 << 4, "Socket could not bind to an address"),
	ACCEPTED(1 << 5, "Connection accepted to bound interface"),
	ACCEPT_FAILED(1 << 6, "Could not accept client connection"),
	CLOSED(1 << 7, "Connection closed"),
	CLOSE_FAILED(1 << 8, "Connection couldn't be closed"),
	DISCONNECTED(1 << 9, "Broken session"),
	MONITOR_STOPPED(1 << 10, "Monitor stopped"),
	HANDSHAKE_PROTOCOL(1 << 15, "Handshaked");

	private final int eventCode;
	private final String info;

	public static ZmqEvent fromCode(int eventCode) {
		for (ZmqEvent zmqEvent : ZmqEvent.values()) {
			if (zmqEvent.getEventCode() == eventCode) {
				return zmqEvent;
			}
		}
		return null;
	}
}
