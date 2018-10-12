package org.apache.spark.streaming;

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
public class IBMMQReciever extends Receiver<String> {

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
	int openOptions = MQConstants.MQOO_INPUT_SHARED | MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE
			| MQConstants.MQOO_BROWSE;
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
	int seqNo;
	boolean keepMessages = true;

	public IBMMQReciever(String host, int port, String qmgrName, String channel, String queueName, String userName,
			String password, String waitInterval, String keepMessages) {
		super(StorageLevel.MEMORY_ONLY_2());
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
		if (waitInterval != null) {
			if (!(waitInterval.equalsIgnoreCase(""))) {
				this.waitInterval = Integer.parseInt(waitInterval);
			}
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
		try {

			if (queue != null) {
				if (queue.isOpen()) {
					queue.close();
				}

			}
			if (qmgr != null) {
				if (qmgr.isConnected()) {
					qmgr.close();
				}
			}
		} catch (MQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/** Create a MQ connection and receive data until receiver is stopped */
	private void receive() {
		System.out.println("Started receiving messages from MQ");
		MQGetMessageOptions gmo = new MQGetMessageOptions();
		gmo.options = gmo.options | MQConstants.MQGMO_CONVERT | MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2
				| MQConstants.MQGMO_BROWSE_FIRST;
		if (!(keepMessages)) {
			gmo.options = gmo.options | MQConstants.MQGMO_MSG_UNDER_CURSOR;
		}
		gmo.matchOptions = MQConstants.MQMO_NONE;
		gmo.waitInterval = this.waitInterval;

		try {
			if (queueDepth != 0) {
				int lastSeqNo = 0;
				long lastTs = 0;
				boolean messagesExist = true;
				while (messagesExist) {
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

						// move cursor to next message
						gmo.options = MQConstants.MQGMO_CONVERT |MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2 | MQConstants.MQGMO_WAIT
								| MQConstants.MQGMO_BROWSE_NEXT;
						if (!(keepMessages)) {
							gmo.options = gmo.options | MQConstants.MQGMO_MSG_UNDER_CURSOR;
						}
					} catch (MQException e) {
						if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
							System.out.println("No Messages Available on Queue");
						}
						messagesExist = false;
						stop("No Messages Avaialble on Queue to Read");
					} catch (IOException ioe) {
						System.out.println("Error: " + ioe.getMessage());
					}
				}
			} else {
				stop("No Messages Available on Queue to Read");
			}
			
		} catch (Throwable t) {
			// restart if there is any other error
			restart("Error receiving data from Queue or QMGR", t);
		}

	}

	public void initConnection() {
	    final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
	    final boolean IBM_JAVA = JAVA_VENDOR_NAME.contains("IBM");
		System.out.println("Initiating Connection to QMGR and Queue");
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

			if (qmgr.isConnected()) {
				System.out.println("Connected to QMGR: " + qmgrName);
			}
			if (qmgr != null) {
				queue = qmgr.accessQueue(queueName, openOptions);
				if (queue != null) {
					if (queue.isOpen()) {
						System.out.println("Connected to Queue: " + queueName);
						queueDepth = queue.getCurrentDepth();
						System.out.println("Queue Depth: " + queueDepth);
					}
				}
			}
		} catch (MQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		System.out.println("Queue Connection Established successfully!");

	}

	@Override
	public StorageLevel storageLevel() {
		return StorageLevel.MEMORY_ONLY_2();
	}

}
