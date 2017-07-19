/screen - Shows All Screens & Movies
/screen/{ScreenName} - Shows Specific Screen
/screen/{ScreenName}/{ShowName} - Display Specific Scow in a Specific Screen
/screen/{ScreenName}/{ShowName}/{Date} - Display Show Details for a given Day & Screen

Spark Issues Resolved

1	Spark Submit & Classpath	
	•	How Spark Submit works
	•	Cluster Mode Vs Client Mode
	•	Driver behavior in client mode – when running in client mode all the driver related configuration need to be set –driver* options, as the driver will start before initialization of SparkConf object. Where as in Cluster mode Driver runs on Yarn Cluster and driver options can be specified in SparkConf.
	•	--files argument just uploads files to Spark Workspace not classpath.
	•	Need to set –conf parameters in order to use uploaded files.

2	Dynamic Executors	
	•	Spark can maintain life-cycle of executors with Dynamic Executors enabled. But there is a caveat, if we don’t the limit to dynamic executors, it will request / create executors exponentially causing job failures.
	•	The resolution is limit the same with max.dynamic.executors  property
3	Streaming & Parallel Processing	
	•	Even though we configure the appropriate executers and cores, parallel processing will not happen unless we have fine-tuned spark.streaming.blockInterval property. With this property we can instruct Spark to create a task for the specified duration. 
	•	Ex: Block Interval = 10ms, Kafka Stream Duration = 30s will create a 30000/10 30,000 tasks.

4	Class Conflicts: This will happen when we multiple jars which include different versions of class files.	
	Solution to the problem is:
	•	Distribute the correct jar to Spark working directory using –files.
	•	Specify the jar in the classpath
	•	Configure Spark Context to load user class path first 
		--conf spark.executor.userClassPathFirst=true 
		--conf spark.driver.userClassPathFirst=true
