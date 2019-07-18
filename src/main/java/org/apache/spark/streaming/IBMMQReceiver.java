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

import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.Seq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Hashtable;
import java.io.IOException;
import scala.collection.JavaConversions;

public class IBMMQReceiver extends Receiver<String> {

	public static final Log LOG = LogFactory.getLog(IBMMQReceiver.class.getName());
	public static final MQLockObject mqLock = new MQLockObject();

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
	long recordCountsRcvdLast = 0;
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
	long lastCheckTime = 0;
	long timeMillis = 60000;
	int maxUMsg = 10000;
	long currentTime;
	long elapsedTime;
	long currentCountTime;
	long elapsedCountTime;
	long lastCountTime = 0;
	boolean haltStatus = false;
	boolean readQueue = true;
	long recordCountsRcvd = 0;
	long recordCountsCmited = 0;
	long recordCountsCmitFail = 0;

	int mqccsid = 0;
	long noMessagesCounter = 0;
	ArrayBuffer<String> msgBuffer;
	ArrayList<String> msgList;
	boolean syncPoint = false;

	public IBMMQReceiver(String host, int port, String qmgrName, String channel, String queueName, String userName,
			String password, String waitInterval, String keepMessages, String mqRateLimit, int mqCCSID) {
		super(StorageLevel.MEMORY_AND_DISK_SER());

		this.mqccsid = mqCCSID;
		this.msgList = new ArrayList<String>();
		this.msgBuffer = new ArrayBuffer<String>();
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
			System.out.println(Calendar.getInstance().getTime() + " - Exception Reason Code: " + me.reasonCode);
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
				System.err.println(Calendar.getInstance().getTime() + " - Exception Reason Code: " + mqe.reasonCode);
			}
		}
	}

	private void reconnectMq() {
		disconnectMq();
		System.out.println(Calendar.getInstance().getTime() + " - QMGR Disconnected Waiting 600 Seconds to Reconnect");
		threadWait(600000L);
		initConnection();
	}

	/** Create a MQ connection and receive data until receiver is stopped */
	private void receive() {
		MQGetMessageOptions gmo = new MQGetMessageOptions();
		if (keepMessages) {
			gmo.options = gmo.options | MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
					| MQConstants.MQGMO_BROWSE_FIRST;
		} else {
			gmo.options = gmo.options | MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2;
			if (syncPoint) {
				gmo.options = gmo.options | MQConstants.MQGMO_SYNCPOINT;
			}
		}
		gmo.matchOptions = MQConstants.MQMO_NONE;
		gmo.waitInterval = this.waitInterval;

		try {
			int lastSeqNo = 0;
			long lastTs = 0;
			int messagecounter = 0;
			while (true) {
				if (!(qmgr.isConnected())) {
					reconnectMq();
				}
				if (!(queue.isOpen())) {
					reconnectMq();
				}
				haltFileExists();
				if (haltStatus) {
					readQueue = false;
				} else {
					readQueue = true;
				}
				int queueNotInhibited = queue.getInhibitGet();
				if (queueNotInhibited == MQConstants.MQQA_GET_INHIBITED) {
					readQueue = false;
				}
				produceCounts();
				if (readQueue) {
					queueDepth = queue.getCurrentDepth();
					if (queueDepth != 0) {
						try {
							rcvMessage = new MQMessage();
							if (mqccsid != 0) {
								rcvMessage.characterSet = mqccsid;
							}
							queue.get(rcvMessage, gmo);
							recordCountsRcvd++;
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
							LOG.debug("MQ MsgKey: " + Long.toString(messagePutMs) + "_" + seqNo);
							LOG.debug("MQ MsgValue: " + msgText);
							jsonOut.put(jsonKey);
							jsonOut.put(jsonValue);
							msgList.add(jsonOut.toString());
							lastTs = messagePutMs;
							lastSeqNo = seqNo;
							messagecounter++;
							// move cursor to next message
							if (msgList.size() > maxUMsg / 2) {
								commitMessages();
							}
							if (msgList.size() > queueDepth) {
								commitMessages();
							}
							if (keepMessages) {
								gmo.options = MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
										| MQConstants.MQGMO_WAIT | MQConstants.MQGMO_BROWSE_NEXT;
							} else {
								gmo.options = MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
										| MQConstants.MQGMO_WAIT;
								if (syncPoint) {
									gmo.options = gmo.options | MQConstants.MQGMO_SYNCPOINT;
								}
							}
						} catch (MQException e) {
							if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
								if (msgList.size() > 0) {
									commitMessages();
								}
								noMessagesCounter++;
								threadWait();
							} else if (e.reasonCode == MQConstants.MQRC_SYNCPOINT_LIMIT_REACHED) {
								if (msgList.size() > 0) {
									commitMessages();
								}
								noMessagesCounter++;
								threadWait();
							} else {
								System.out.println(
										Calendar.getInstance().getTime() + " - MQ Reason Code: " + e.reasonCode);
								reconnectMq();
							}
						} catch (IOException ioe) {
							System.out.println(Calendar.getInstance().getTime() + " - Error: " + ioe.getMessage());
							reconnectMq();
						}
					} else {
						if (msgList.size() > 0) {
							commitMessages();
						}
						threadWait();
					}
				} else {
					if (msgList.size() > 0) {
						commitMessages();
					}
					threadWait();
				}
			}

		} catch (Throwable t) {
			// restart if there is any other error
			restart(Calendar.getInstance().getTime() + " - Error receiving data from Queue or QMGR", t);
		}

	}

	public void threadWait() {
		synchronized (mqLock) {
			try {
				mqLock.wait(threadWait);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
	}

	public void threadWait(long timeout) {
		synchronized (mqLock) {
			try {
				mqLock.wait(timeout);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
	}

	public void commitMessages() {
		int commitMessages = 0;
		try {
			Seq<String> seqofMessages = JavaConversions.asScalaBuffer(msgList).seq();
			msgBuffer.append(seqofMessages);
			commitMessages = msgBuffer.size();
			if (msgBuffer.size() == msgList.size()) {
				store(msgBuffer);
				if (syncPoint && (!keepMessages)) {
					recordCountsCmited = recordCountsCmited + commitMessages;
					qmgr.commit();
				}
				msgList.clear();
				msgBuffer.clear();
			} else {
				LOG.error("msgBuffer.size(): != msgList.size(): " + msgBuffer.size() + "!=" + msgList.size());
				msgList.clear();
				msgBuffer.clear();
				if (syncPoint && (!keepMessages)) {
					qmgr.backout();
					recordCountsCmitFail = recordCountsCmitFail + commitMessages;
				}
			}
		} catch (Throwable se) {
			msgList.clear();
			msgBuffer.clear();
			try {
				if (syncPoint && (!keepMessages)) {
					produceCounts();
					System.out.println(Calendar.getInstance().getTime()
							+ " - Rolling Back due to Exception - DISCARD COMMIT: " + commitMessages);
					qmgr.backout();
					recordCountsCmitFail = recordCountsCmitFail + commitMessages;
				}
			} catch (MQException e) {
				// TODO Auto-generated catch block
				System.out.println(Calendar.getInstance().getTime() + " - MQ Reason Code: " + e.reasonCode);
				reconnectMq();
			}
			produceCounts();
			reportError(
					Calendar.getInstance().getTime() + " - WriteAheadLog/Spark Failure - DISCARDED " + commitMessages,
					se);
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
				if (qmgr.isConnected()) {
					if (qmgr.getSyncpointAvailability() == MQConstants.MQSP_AVAILABLE) {
						System.out.println("QMGR SyncPoint Enabled");
						syncPoint = true;
					} else {
						System.out.println("QMGR SyncPoint NOT AVAILABLE on QMGR");
					}

					queue = qmgr.accessQueue(queueName, openOptions);
					if (queue != null) {
						if (queue.isOpen()) {
							if (queue.getInhibitGet() == MQConstants.MQQA_GET_ALLOWED)
								queueDepth = queue.getCurrentDepth();
							System.out.println(Calendar.getInstance().getTime() + " - Connected to Host: " + host + ":"
									+ port + " :: QMGR: " + qmgrName + " :: Queue: " + queueName + " :: Queue Depth: "
									+ queueDepth);
						} else {
							reconnectMq();
						}
					}

				} else {
					reconnectMq();
				}
			} else {
				reconnectMq();
			}
		} catch (MQException e) {
			// TODO Auto-generated catch block
			reconnectMq();
			System.out.println(
					Calendar.getInstance().getTime() + " - MQ ReasonCode: " + e.reasonCode + " :: " + e.getErrorCode());
		}
	}

	@Override
	public StorageLevel storageLevel() {
		return StorageLevel.MEMORY_AND_DISK_SER();
	}

	public Boolean haltFileExists() throws IOException {
		currentTime = System.currentTimeMillis();
		elapsedTime = currentTime - lastCheckTime;
		if ((elapsedTime > timeMillis) || haltStatus) {
			lastCheckTime = System.currentTimeMillis();
			String fileName = "/user/" + System.getProperty("user.name") + "/" + queueName + ".halt";
			Configuration conf = new Configuration();
			FileSystem fs;
			fs = FileSystem.get(conf);
			if (fs.exists(new Path(fileName))) {
				if (!(haltStatus)) {
					System.out.println(Calendar.getInstance().getTime() + " - Halting " + queueName);
				}
				haltStatus = true;
			} else {
				if (haltStatus) {
					System.out.println(Calendar.getInstance().getTime() + " - Unhalting " + queueName);
				}
				haltStatus = false;
			}
		}
		return haltStatus;
	}

	public void produceCounts() {
		currentCountTime = System.currentTimeMillis();
		elapsedCountTime = currentCountTime - lastCountTime;
		if (elapsedCountTime > timeMillis) {
			lastCountTime = System.currentTimeMillis();
			if (recordCountsRcvd != recordCountsRcvdLast) {
				recordCountsRcvdLast = recordCountsRcvd;
				try {

					int qmgrMaxMsgLength = qmgr.getMaximumMessageLength();
					int queueDepth = queue.getCurrentDepth();
					int queueMaxDepth = queue.getMaximumDepth();
					int queueMaxMsgLength = queue.getMaximumMessageLength();
					int queueGetInhibited = queue.getInhibitGet();
					int queuePutInhibited = queue.getInhibitPut();
					int queueOpenInputCount = queue.getOpenInputCount();
					int queueOpenOutputCount = queue.getOpenOutputCount();
					int queueShareAbility = queue.getShareability();
					int qmgrCCSID = qmgr.getCharacterSet();
					int msgCCSID = mqccsid;

					System.out.println(Calendar.getInstance().getTime() + " - Connected to Host: " + host + ":" + port
							+ " :: QMGR: " + qmgrName + " :: Queue: " + queueName + " :: Queue Depth: " + queueDepth
							+ " :: queueMaxDepth: " + queueMaxDepth + " :: queueMaxMsgLength:" + queueMaxMsgLength
							+ " :: queueGetInhibited: " + queueGetInhibited + " :: queuePutInhibited: "
							+ queuePutInhibited + " :: queueOpenInputCount: " + queueOpenInputCount
							+ " :: queueOpenOutputCount: " + queueOpenOutputCount + " :: queueShareAbility: "
							+ queueShareAbility + " :: qmgrMaxMsgLength: " + qmgrMaxMsgLength + " :: qmgrCCSID: "
							+ qmgrCCSID + " :: rcvMessageCCSID: " + msgCCSID + " :: recordsRcvd: " + recordCountsRcvd
							+ ":: recordsCmited: " + recordCountsCmited + ":: recordsFailedToCmit: "
							+ recordCountsCmitFail + " :: MQ Recv Halted: " + haltStatus
							+ " :: Number of MQ 2033 (No Messages on Queue): " + noMessagesCounter);
					noMessagesCounter = 0;
				} catch (MQException e) {
					// TODO Auto-generated catch block
					reconnectMq();
					System.out.println(Calendar.getInstance().getTime() + " - Problem producing MQ Counts");
					e.printStackTrace();
				}
			}
		}
	}

}
