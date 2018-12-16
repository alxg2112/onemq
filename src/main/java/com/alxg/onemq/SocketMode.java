package com.alxg.onemq;

/**
 * Determines whether socket should {@link org.zeromq.ZMQ.Socket#bind} or
 * {@link org.zeromq.ZMQ.Socket#connect}.
 *
 * @author Alexander Gryshchenko
 */
public enum SocketMode {
	BIND,
	CONNECT
}
