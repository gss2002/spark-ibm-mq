# Spark Streaming Reciever for Native IBM MQ Client Queue Access. (Non JMS)
#### How to use the Native IBM MQ Client Reciever

JavaStreamingContext ssc = new JavaStreamingContext(sparkconf, new Duration(1000));
JavaDStream<String> mqStream = ssc.receiverStream(new IBMMQReciever("mqp1.example.com", 1414, "qmgrName", "MQ.CLIENT.CHANNEL.NAME", "userName", "password", "mqTimeWaitMS", "keepMessagesTrue/False"));
mqStream.foreach(rdd -> {
   rdd.foreach(record -> {
   	JSONArray json = new JSONArray(record)
   	String key ((JSONObject) json.get(0)).getString("key");
   	String value ((JSONObject) json.get(1)).getString("value");
   	System.out.println("Key: "+key" :: Value:"+value);
   });
});

ssc.start();
ssc.awaitTermination();
 

#### Set Spark Streaming Job with the following parameters if using this to keep order from say DB2QREP or some other Queue based Data Replication Tool
spark.executor.instances=1
spark.cores.max=1
spark.streaming.receiver.maxRate=1000
spark.cleaner.ttl=120
spark.streaming.concurrentJobs=1
spark.serializer=org.apache.spark.serializer.JavaSerializer
spark.executor.cores=1