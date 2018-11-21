package org.apache.spark.streaming;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Hashtable;
import java.io.IOException;

/**
 * Created by gsenia
 */
public class IBMMQReceiver extends Receiver<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String host = null;
	int port = -1;
	String qmgrName = null;
	String queueName = null;
	String channel = null;
	String username = null;
	String password = null;
	MQQueue queue = null;
	int queueDepth = 0;
	int openOptions = 0;
	MQQueueManager qmgr;
	MQMessage rcvMessage;
	int strLen;
	byte[] strData;
	String msgText;
	JSONArray jsonOut;
	JSONObject jsonKey;
	JSONObject jsonValue;
	long messagePutMs;
	int waitInterval = 5000;
	long threadWait = 5000L;
	int seqNo;
	boolean keepMessages = true;
	int mqRateLimit = -1;

	public IBMMQReceiver(String host, int port, String qmgrName, String channel, String queueName, String userName,
			String password, String waitInterval, String keepMessages, String mqRateLimit) {
		super(StorageLevel.MEMORY_AND_DISK_SER());
		this.host = host;
		this.port = port;
		this.qmgrName = qmgrName;
		this.queueName = queueName;
		this.channel = channel;
		this.username = userName;
		this.password = password;
		if (keepMessages != null) {
			if (!(keepMessages.equalsIgnoreCase(""))) {
				this.keepMessages = Boolean.parseBoolean(keepMessages);
			}
		}
		if (mqRateLimit != null) {
			if (!(mqRateLimit.equalsIgnoreCase(""))) {
				this.mqRateLimit = Integer.parseInt(mqRateLimit);
			}
		}
		if (waitInterval != null) {
			if (!(waitInterval.equalsIgnoreCase(""))) {
				this.waitInterval = Integer.parseInt(waitInterval);
				this.threadWait = Long.parseLong(waitInterval);
			}
		}
		if (this.keepMessages) {
			this.openOptions = MQConstants.MQOO_INPUT_SHARED | MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE
					| MQConstants.MQOO_BROWSE;
		} else {
			this.openOptions = MQConstants.MQOO_INPUT_SHARED | MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE;
		}
	}

	public void onStart() {
		// Start the thread that receives data over a connection
		new Thread() {
			@Override
			public void run() {
				initConnection();
				receive();
			}
		}.start();
	}

	public void onStop() {
		disconnectMq();
	}

	private void disconnectMq() {
		try {

			if (queue != null) {
				if (queue.isOpen()) {
					queue.close();
				}

			}
			if (qmgr != null) {
				if (qmgr.isConnected()) {
					qmgr.close();
					qmgr.disconnect();
				}
			}
		} catch (MQException me) {
			// TODO Auto-generated catch block
			System.err.println("Exception Reason Code: " + me.reasonCode);
		} finally {
			try {
				if (queue != null) {
					if (queue.isOpen()) {
						queue.close();
					}

				}
				if (qmgr != null) {
					if (qmgr.isConnected()) {
						qmgr.close();
						qmgr.disconnect();
					}
				}
			} catch (MQException mqe) {
				// TODO Auto-generated catch block
				System.err.println("Exception Reason Code: " + mqe.reasonCode);
			}
		}
	}

	private void reconnectMq() {
		disconnectMq();
		initConnection();
	}

	/** Create a MQ connection and receive data until receiver is stopped */
	private void receive() {
		MQLockObject mqLock = new MQLockObject();
		MQGetMessageOptions gmo = new MQGetMessageOptions();
		if (keepMessages) {
			gmo.options = gmo.options | MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
					| MQConstants.MQGMO_BROWSE_FIRST;
		} else {
			gmo.options = gmo.options | MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2;
		}
		gmo.matchOptions = MQConstants.MQMO_NONE;
		gmo.waitInterval = this.waitInterval;

		try {
			int lastSeqNo = 0;
			long lastTs = 0;
			int messagecounter = 0;
			while (true) {
				queueDepth = queue.getCurrentDepth();
				if (queueDepth != 0) {
					try {
						rcvMessage = new MQMessage();
						queue.get(rcvMessage, gmo);
						strLen = rcvMessage.getMessageLength();
						strData = new byte[strLen];
						rcvMessage.readFully(strData);
						messagePutMs = rcvMessage.putDateTime.getTimeInMillis();
						seqNo = rcvMessage.messageSequenceNumber;
						if (lastTs == messagePutMs && seqNo == 1) {
							seqNo = lastSeqNo + seqNo;
						}
						msgText = new String(strData);
						jsonOut = new JSONArray();
						jsonKey = new JSONObject();
						jsonValue = new JSONObject();
						jsonKey.put("key", Long.toString(messagePutMs) + "_" + seqNo);
						jsonValue.put("value", msgText);
						jsonOut.put(jsonKey);
						jsonOut.put(jsonValue);
						store(jsonOut.toString());
						lastTs = messagePutMs;
						lastSeqNo = seqNo;
						messagecounter++;
						if (messagecounter == mqRateLimit) {
							synchronized (mqLock) {
								try {
									messagecounter = 0;
									mqLock.wait(threadWait);
								} catch (InterruptedException ie) {
									ie.printStackTrace();
								}
							}
						}
						// move cursor to next message
						if (keepMessages) {
							gmo.options = MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
									| MQConstants.MQGMO_WAIT | MQConstants.MQGMO_BROWSE_NEXT;
						} else {
							gmo.options = MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
									| MQConstants.MQGMO_WAIT;
						}
					} catch (MQException e) {
						if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
							System.out.println("No Messages Available on Queue: " + e.reasonCode);
							synchronized (mqLock) {
								try {
									mqLock.wait(threadWait);
								} catch (InterruptedException ie) {
									ie.printStackTrace();
								}
							}
						} else {
							System.out.println("MQ Reason Code: " + e.reasonCode);
							reconnectMq();
						}
					} catch (IOException ioe) {
						System.err.println("Error: " + ioe.getMessage());
						reconnectMq();
					}
				} else {
					synchronized (mqLock) {
						try {
							mqLock.wait(threadWait);
						} catch (InterruptedException ie) {
							ie.printStackTrace();
						}
					}
				}
			}

		} catch (Throwable t) {
			// restart if there is any other error
			restart("Error receiving data from Queue or QMGR", t);
		}

	}

	public void initConnection() {
		final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
		final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");
		if (IBM_JAVA) {
			System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "true");
		} else {
			System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "false");
		}
		Hashtable<String, Object> properties = new Hashtable<String, Object>();
		properties.put(MQConstants.HOST_NAME_PROPERTY, host);
		properties.put(MQConstants.PORT_PROPERTY, port);
		properties.put(MQConstants.CHANNEL_PROPERTY, channel);
		properties.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, "true");
		properties.put(MQConstants.USER_ID_PROPERTY, username);
		properties.put(MQConstants.PASSWORD_PROPERTY, password);

		/**
		 * Connect to a queue manager
		 */
		try {
			qmgr = new MQQueueManager(qmgrName, properties);

			if (qmgr != null) {
				queue = qmgr.accessQueue(queueName, openOptions);
				if (queue != null) {
					if (queue.isOpen()) {
						queueDepth = queue.getCurrentDepth();
						System.out.println("Connected to Host: " + host + ":" + port + " :: QMGR: " + qmgrName
								+ " :: Queue: " + queueName + " :: Queue Depth: " + queueDepth);
					}
				}
			}
		} catch (MQException e) {
			// TODO Auto-generated catch block
			System.out.println("MQ ReasonCode: " + e.reasonCode + " :: " + e.getErrorCode());
		}
	}

	@Override
	public StorageLevel storageLevel() {
		return StorageLevel.MEMORY_AND_DISK_SER();
	}

}
