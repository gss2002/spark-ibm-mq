# How to use the Native IBM MQ Client Receiver with Spark Streaming  
#### How to use the Native IBM MQ Client Receiver  
 
Non-CheckPoint Model:  

```
JavaStreamingContext ssc = new JavaStreamingContext(sparkconf, new Duration(1000));  
JavaDStream<String> mqStream = ssc.receiverStream(new IBMMQReceiver("mqp1.example.com", 1414, "qmgrName", "MQ.CLIENT.CHANNEL.NAME", "userName", "password", "mqTimeWaitMS", "keepMessagesTrue/False"));  
  
mqStream.foreach(rdd -> {  
    rdd.foreach(record -> {  
        JSONArray json = new JSONArray(record);  
        String key ((JSONObject) json.get(0)).getString("key");  
        String value ((JSONObject) json.get(1)).getString("value");  
        System.out.println("Key: "+key" :: Value:"+value);  
    });  
});  
  
ssc.start();  
ssc.awaitTermination();
  
```
 
 
Checkpoint/WriteAheadLogs Model:  
  
  ```
  sparkConf = new SparkConf().setAppName(jobName);
  if (sparkUser.equalsIgnoreCase("")) {
     sparkUser = System.getProperty("user.name");
  }
  sparkCheckPoint = "/user/"+sparkUser+"/sparkstreaming/"+jobName;
  JavaStreamingContext ssc = JavaStreamingContext.getOrCreate(sparkCheckPoint,
     new Function0<JavaStreamingContext>() {
        private static final long serialVersionUID = 1L;
        @Override
        public JavaStreamingContext call() throws Exception {
           JavaSparkContext jsc = new JavaSparkContext(sparkConf);
           JavaStreamingContext ssc = new JavaStreamingContext(jsc, new Duration(sparkBatchDuration));
           ssc.checkpoint(sparkCheckPoint);
           sparkUser = ssc.sparkContext().sparkUser();
           JavaDStream<String> mqStream = ssc.receiverStream(new IBMMQReceiver(qmgrHost, Integer.parseInt(qmgrPort), qmgrName, qmgrChannel, mqQueue, mqUser, mqPassword, mqWait, mqKeepMessages, mqRateLimit));
           mqStream.foreach(rdd -> {  
              rdd.foreach(record -> {  
        	        JSONArray json = new JSONArray(record);  
                 String key ((JSONObject) json.get(0)).getString("key");  
                 String value ((JSONObject) json.get(1)).getString("value");  
                 System.out.println("Key: "+key" :: Value:"+value);  
              });  
           });          
           return ssc;
       }
     });       
     ssc.start();  
     ssc.awaitTermination();
                 
```

#### Set Spark Streaming Job with the following parameters if using this to keep order from say DB2QREP or some other Queue based Data Replication Tool  
spark.executor.instances=1  
spark.cores.max=2  
spark.streaming.receiver.maxRate=1000  
spark.cleaner.ttl=120  
spark.streaming.concurrentJobs=1  
spark.serializer=org.apache.spark.serializer.JavaSerializer  
spark.executor.cores=2  
spark.hadoop.fs.hdfs.impl.disable.cache=true  
spark.dynamicAllocation.enabled=false  
spark.streaming.backpressure.enabled=true  

##### Use Sparks writeAheadLog Feature when using IBM MQ if not messages can be lost
spark.streaming.receiver.blockStoreTimeout=300  
spark.streaming.receiver.writeAheadLog.enable=true  
spark.streaming.driver.writeAheadLog.closeFileAfterWrite=true  
spark.streaming.receiver.writeAheadLog.closeFileAfterWrite=true  

##### For long running Spark or Flink Jobs on YARN the following yarn-site.xml entry needs to be set  
yarn.resourcemanager.proxy-user-privileges.enabled=true  



