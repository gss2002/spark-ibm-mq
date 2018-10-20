package org.apache.spark.streaming;

public final class MQLockObject implements Runnable {
	static final Object qLock = new Object();

	public MQLockObject() {
		super();
	}

	@Override
	public void run() {

	}
}
