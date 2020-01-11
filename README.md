-----------------------------------------------------------
Project: datAcron (http://ai-group.ds.unipi.gr/datacron/)
Task: 2.1 Trajectory detection & summarization
Module: Synopses Generator
Version: 0.7. 
Description: Source code for Trajectory detection and summarization built on Apache Flink 0.10.2 (streaming) platform.
Author: Kostas Patroumpas (UPRC)
Created: 21/10/2016
Revised: 10/07/2017
-----------------------------------------------------------

---------------------
SOFTWARE REQUIREMENTS
---------------------

The following software must have been installed prior of executing the source code:

i)   Java SDK ver. 1.8.x

ii)  Apache Maven ver. 3.0.4 (or higher)

iii) Apache Kafka ver. 0.10.1.0 for Scala 2.10 or Scala 2.11.

iv)  Apache Flink ver. 0.10.2.

v)   Apache Avro ver. 1.8.1 for Java (avro-1.8.1.jar; avro-tools-1.8.1.jar; jackson-core-asl-1.9.13.jar; jackson-mapper-asl-1.9.13.jar).


---------------------------------------------------------
Linux installation as carried out on a typical server:
---------------------------------------------------------

Assuming that: 

-- Java installed at /usr/lib/jvm/        -- Java ver. 1.8 was installed

-- Maven installed at /usr/share/maven/   -- Maven ver 3.0.5 currently installed

The following Java-related parameters must be set:

export JAVA_HOME=/usr/lib/jvm/java-8-oracle

export JRE_HOME=/usr/lib/jvm/java-8-oracle/jre 	

export PATH=$PATH:/usr/lib/jvm/java-8-oracle/bin:/usr/lib/jvm/java-8-oracle/jre/bin

export PATH=/usr/share/maven/bin:$PATH

-- Verify that variables are set:

printenv PATH

-------------------------------------------------------------------------

-- JAVA SDK must have been installed. Verify version:

java -version


--MAVEN must have been installed. Run this Linux command to verify its version:

mvn -v


------------------------
Apache FLINK
------------------------

-- Assuming that a local installation of Apache Flink (after just extracting the downloaded .tgz file) is placed in this directory:

cd /opt/flink-0.10.2


-- Starting Flink job manager locally: 

bin/start-local.sh


-- Check services actually running:

jps


-- Check the JobManagers web frontend (Replace <MY_HOST> with the DNS IP address of your localhost): 

http://<MY_HOST>:8081 


-- Stopping Flink job manager:

bin/stop-local.sh


-- To enable more task slots that each TaskManager offers (each slot runs one parallel pipeline), modify Flink configuration in file 'flink-conf.yaml', e.g., by setting:

taskmanager.numberOfTaskSlots: 8


****************************************************************
Controlling Flink jobs
****************************************************************

--List scheduled and running jobs (including their JobIDs):

/opt/flink-0.10.2/bin/flink list


--List scheduled jobs (including their JobIDs):

/opt/flink-0.10.2/bin/flink list -s


--List running jobs (including their JobIDs):

/opt/flink-0.10.2/bin/flink list -r


--List running Flink jobs inside Flink YARN session:

/opt/flink-0.10.2/bin/flink list -m yarn-cluster -yid <yarnApplicationID> -r


--Cancel a Flink job:

/opt/flink-0.10.2/bin/flink cancel <jobID>


--Cancel a job with a savepoint:

/opt/flink-0.10.2/bin/flink cancel -s [targetDirectory] <jobID>


-- View streaming output results from Flink:

tail -f /opt/flink-0.10.2/log/flink-*-jobmanager-*.out


****************************************************************
KAFKA Settings
****************************************************************

-- Installed kafka on this directory:

cd /opt/confluent-3.2.0/

-- IMPORTANT! Must change advertised listener into the DNS IP address (in file "server.properties"). Replace <MY_HOST> with the DNS IP address of your localhost:

advertised.listeners=PLAINTEXT://<MY_HOST>:9092

-- Allow deletion of topics (in file "server.properties") in order to run multiple simulations against the same datasets:
delete.topic.enable=true

-- Handling kafka:

-- Start zookeeper server:
cd /opt/confluent-3.2.0/bin
zookeeper-server-start ../etc/kafka/zookeeper.properties

-- Start broker:
cd /opt/confluent-3.2.0/bin
kafka-server-start ../etc/kafka/server.properties 


-- Create topics like this:
-- IMPORTANT: change <MY_INPUT_TOPIC> <MY_OUTPUT_TOPIC> and into topic names of your choice, and <MY_HOST> with the DNS IP address of your localhost:

/opt/confluent-3.2.0/bin/kafka-topics --create --zookeeper <MY_HOST>:2181 --replication-factor 1 --partitions 1 --topic <MY_INPUT_TOPIC>

/opt/confluent-3.2.0/bin/kafka-topics --create --zookeeper <MY_HOST>:2181 --replication-factor 1 --partitions 3 --topic <MY_OUTPUT_TOPIC>


-- Drop existing topics like this:
-- IMPORTANT: change <MY_INPUT_TOPIC> <MY_OUTPUT_TOPIC> and into topic names of your choice, and <MY_HOST> with the DNS IP address of your localhost:

/opt/confluent-3.2.0/bin/kafka-topics --delete --zookeeper <MY_HOST>:2181 --topic <MY_INPUT_TOPIC>

/opt/confluent-3.2.0/bin/kafka-topics --delete --zookeeper <MY_HOST>:2181 --topic <MY_OUTPUT_TOPIC>


************************************************************************
AVRO -- Skip this step if you do not make any changes in the .avsc files
************************************************************************

* Assuming that Avro tools (avro-1.8.1.jar; avro-tools-1.8.1.jar; jackson-core-asl-1.9.13.jar; jackson-mapper-asl-1.9.13.jar) are installed in: /opt/flink-0.10.2/lib

-- IMPORTANT! Resulting JAVA schemata for critical points and annotations (for both use cases) are already included in the source code. Hence, there is not needed to execute the following steps unless you make changes in the original schemata

* Assuming that Avro schema specifications (files *.avsc) are created in: /opt/datacron/implementation/trajectory_synopses/src/main/resources

* Build a POJO (Plain Old Java Object) from a specification in file 'critical_point.avsc': 

java -jar /opt/flink-0.10.2/lib/avro-tools-1.8.1.jar compile schema /opt/datacron/implementation/trajectory_synopses/src/main/resources/critical_point.avsc /trajectory_synopses/src/main/resources

=> This results into "critical_point.java" and "critical_point_annotation.java" files that must be included in the project.

-- CAUTION! In order to compile this .java schemata with SCALA code, both .java files must be put along with the SCALA source files in the project (i.e., in the same directory).


************************************************************************
TESTS
************************************************************************

-------------------------------------------------------------
1) Compiling the source code
-------------------------------------------------------------

-- Created a Scala project with a MAVEN archetype as defined in file: pom.xml
-- 'trajectory_synopses' is the name of the directory where source and target code will be stored.

-- Building the project using MAVEN:

cd /opt/datacron/implementation/trajectory_synopses/

mvn clean package -Pbuild-jar



-------------------------------------------------
2) CONFIGURATION for executing the compiled code
-------------------------------------------------

-- IMPORTANT! Execution is controlled by a configuration file '*_config.properties'.
-- Such configuration files must always be placed under directory '/opt/datacron/config/trajectory_synopses/'. Application ALWAYS looks at this path for configuring its execution.

-- Main configurations regarding input, output, and parametrization for each use case:

* aviation_config.properties => Configuration for the AVIATION use case.

* maritime_config.properties => Configuration for the MARITIME use case.

-- CAUTION! Rename your properties file into 'aviation_config.properties' or 'maritime_config.properties' in order to use it for the respective use case without changing the source code.


------------------------------------------------------------
3) INPUT: Pushing data from a CSV file into a Kafka stream:
------------------------------------------------------------

-- CAUTION! Input data must conform to the AVRO schema for locations (file: location.avsc) for each use case.
-- Simulation according to original timestamp values (with sleep parameter set to 0 in order not to sleep between successive timestamps):

-- MARITIME dataset:

/opt/flink-0.10.2/bin/flink run -c eu.datacron.synopses.maritime.StreamOriginalRateSimulator /opt/datacron/implementation/trajectory_synopses/target/datacron_trajectory_synopses-0.7.jar ais_messages localhost:9092 0

-- AVIATION dataset:

/opt/flink-0.10.2/bin/flink run -c eu.datacron.synopses.aviation.StreamOriginalRateSimulator /opt/datacron/implementation/trajectory_synopses/target/datacron_trajectory_synopses-0.7.jar adsb_messages localhost:9092 0


---------------------------------------------------------
4) OUTPUT: Monitoring output as a Kafka (derived) stream:
---------------------------------------------------------

-- Output Kafka topics include critical points and notifications (as two separate derived streams), as specified in the configuration file. Both output streams conform to the AVRO schema for critical points (file: critical_point.avsc) for each use case.

-- Replace <MY_OUTPUT_TOPIC> with an existing Kafka topic where output is collected:

/opt/confluent-3.2.0/bin/kafka-console-consumer --bootstrap-server <MY_HOST>:9092 --topic <MY_OUTPUT_TOPIC> 

-- or from the beginning (how many data is retained depends on Kafka retention time parameter):

/opt/confluent-3.2.0/bin/kafka-console-consumer --bootstrap-server <MY_HOST>:9092 --topic <MY_OUTPUT_TOPIC> --from-beginning 

-- Results may be written to a JSON file:

/opt/confluent-3.2.0/bin/kafka-console-consumer --bootstrap-server <MY_HOST>:9092 --topic <MY_OUTPUT_TOPIC> --from-beginning > /opt/datacron/data/output/test1.json


-- List all Kafka topics:

/opt/confluent-3.2.0/bin/kafka-topics --zookeeper <MY_HOST>:2181 --list


-----------------------------------------------------------------
5) PROCESSING: Producing CRITICAL POINTS in a streaming fashion:
-----------------------------------------------------------------

-- processing data admitted from an input Kafka AVRO stream:

-- MARITIME:

/opt/flink-0.10.2/bin/flink run -c eu.datacron.synopses.maritime.TrajectoryStreamManager /opt/datacron/implementation/trajectory_synopses/target/datacron_trajectory_synopses-0.7.jar


-- AVIATION:

/opt/flink-0.10.2/bin/flink run -c eu.datacron.synopses.aviation.TrajectoryStreamManager /opt/datacron/implementation/trajectory_synopses/target/datacron_trajectory_synopses-0.7.jar

-- Once this (local) process in up-and-running and fed with input streaming data as detailed in (3) above, results will start to be emitted by Kafka in each of the output topics.

------------
Contributors
------------

Kostas Patroumpas, Nikos Pelekis, Yannis Theodoridis; Data Science Lab., University of Piraeus

--------------
Acknowledgment
--------------

This work was partially supported by the European Union’s Horizon 2020 research and innovation programme under grant agreement No 687591 (DATACRON)

-------------
Citation info
-------------

- (Aviation domain) Kostas Patroumpas, Nikos Pelekis, Yannis Theodoridis (2018) On-the-fly mobility event detection over aircraft trajectories. Proceedings of SIGSPATIAL/GIS, Seattle - WA, USA. ACM Press. 
- (Maritime domain) Kostas Patroumpas, Elias Alevizos, Alexander Artikis, Marios Vodas, Nikos Pelekis, Yannis Theodoridis (2017) Online event recognition from moving vessel trajectories. GeoInformatica 21(2): 389-427.



