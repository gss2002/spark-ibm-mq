package org.apache.spark.streaming;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.json.JSONObject;

import javax.jms.BytesMessage;
import javax.jms.Message;
import java.util.Hashtable;
import java.io.ByteArrayOutputStream;

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
	int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE;
	MQGetMessageOptions gmo;
	MQQueueManager qmgr = null;
	MQMessage rcvMessage;
	int strLen;
	byte[] strData;
	String msgText = null;
	JSONObject jsonOut;
	long messagePutMs;

	public IBMMQReciever(String host, int port, String qmgrName, String channel, String queueName, String userName, String password) {
		super(StorageLevel.MEMORY_ONLY_2());
		this.host = host;
		this.port = port;
		this.qmgrName = qmgrName;
		this.queueName = queueName;
		this.channel = channel;
		this.username = userName;
		this.password = password;
		this.gmo = new MQGetMessageOptions();

		// The following is needed to provide backward compatibility with MQ V6 Java
		// code
		// to properly handle the "format" property of MQHRF2 from JMS messages
		gmo.options = gmo.options + MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2;
		/*
		 * if (qGMOConvert) { gmo.options = gmo.options + MQConstants.MQGMO_CONVERT; }
		 */

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
			queue = null;
			qmgr = null;
		}

		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself isStopped() returns false
	}

	/** Create a MQ connection and receive data until receiver is stopped */
	private void receive() {
		System.out.print("Started receiving messages from MQ");

		try {
			if (queueDepth != 0) {
				for (int i = 0; i < queueDepth; i++) {
					rcvMessage = null;
					msgText = null;
					rcvMessage = new MQMessage();
					if (queue.isOpen()) {
						queue.get(rcvMessage, gmo);
						strLen = rcvMessage.getMessageLength();
						strData = new byte[strLen];
						rcvMessage.readFully(strData);
						messagePutMs = rcvMessage.putDateTime.getTimeInMillis();
						msgText = new String(strData);
						jsonOut = new JSONObject();
						jsonOut.put(Long.toString(messagePutMs), msgText);
						store(jsonOut.toString());
					}

				}
			}

			/*
			 * JMSMessage receivedMessage = null;
			 * 
			 * while (!isStopped() && enumeration.hasMoreElements()) {
			 * 
			 * receivedMessage = (JMSMessage) enumeration.nextElement(); String userInput =
			 * convertStreamToString(receivedMessage); //
			 * System.out.println("Received data :'" + userInput + "'"); store(userInput); }
			 * 
			 * // Restart in an attempt to connect again when server is active again //
			 * restart("Trying to connect again");
			 */
			stop("No More Messages To read !");
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

			System.out.println("Queue Connection is Closed");

		} catch (Exception e) {
			e.printStackTrace();
			restart("Trying to connect again");
		} catch (Throwable t) {
			// restart if there is any other error
			restart("Error receiving data", t);
		} finally {
			queue = null;
			qmgr = null;
			restart("Trying to connect again");
		}

	}

	public void initConnection() {
		System.out.println("Initiating Queue Connection!");
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
				System.out.println("Connected to QMGR: " +qmgrName);
			}
			if (qmgr != null) {
				queue = qmgr.accessQueue(queueName, openOptions);
				if (queue != null) {
					if (queue.isOpen()) {
						System.out.println("Connected to Queue: " +queueName);
						queueDepth = queue.getCurrentDepth();
						System.out.println("Queue Depth: "+queueDepth);

					}
				}
			}
		} catch (MQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			queue = null;
			qmgr = null;
		}

		/*
		 * MQQueueConnectionFactory conFactory= new MQQueueConnectionFactory();
		 * conFactory.setHostName(host); conFactory.setPort(port);
		 * conFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
		 * conFactory.setQueueManager(qm); conFactory.setChannel(channel);
		 * 
		 * qCon= (MQQueueConnection) conFactory.createQueueConnection("username",
		 * "password"); MQQueueSession qSession=(MQQueueSession)
		 * qCon.createQueueSession(false, 1); MQQueue queue=(MQQueue)
		 * qSession.createQueue(qn); qCon.start();
		 * 
		 * MQQueueBrowser browser = (MQQueueBrowser) qSession.createBrowser(queue);
		 * enumeration= browser.getEnumeration();
		 */

		System.out.println("Queue Connection Established successfully!");

	}

	@Override
	public StorageLevel storageLevel() {
		return StorageLevel.MEMORY_ONLY_2();
	}

	/**
	 * Convert stream to string
	 *
	 * @param jmsMsg
	 * @return
	 * @throws Exception
	 */
	private static String convertStreamToString(final Message jmsMsg) throws Exception {
		String stringMessage = "";
		BytesMessage bMsg = (BytesMessage) jmsMsg;
		byte[] buffer = new byte[40620];
		int byteRead;
		ByteArrayOutputStream bout = new java.io.ByteArrayOutputStream();
		while ((byteRead = bMsg.readBytes(buffer)) != -1) {
			bout.write(buffer, 0, byteRead);
		}
		bout.flush();
		stringMessage = new String(bout.toByteArray());
		bout.close();
		return stringMessage;
	}

}
