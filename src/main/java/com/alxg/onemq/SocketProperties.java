package com.alxg.onemq;

import lombok.Data;

/**
 * @author Alexander Gryshchenko
 */
@Data
public abstract class SocketProperties {

	protected static final int DEFAULT_IO_THREADS = 1;
	protected static final int DEFAULT_SEND_TIMEOUT = 1000;
	protected static final int DEFAULT_SEND_QUEUE_SIZE = 0; // Unlimited
	protected static final int DEFAULT_RECEIVE_QUEUE_SIZE = 0; // Unlimited
	protected static final boolean DEFAULT_SYNC_ENABLED = false;
	protected static final boolean DEFAULT_ENVELOPE_ENABLED = false;


	private int ioThreads = DEFAULT_IO_THREADS;
	private SocketMode socketMode;
	private String messageAddress;
	private String syncAddress;
	private boolean syncEnabled = DEFAULT_SYNC_ENABLED;
	private boolean envelopeEnabled = DEFAULT_ENVELOPE_ENABLED;
}
