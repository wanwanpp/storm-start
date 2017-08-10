package com.wp.storm.kafka.example;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class KafkaTopology {
	public static void main(String[] args) throws
		AlreadyAliveException, InvalidTopologyException {
		// zookeeper hosts for the Kafka cluster
		ZkHosts zkHosts = new ZkHosts("192.168.1.114:2181");

		// Create the KafkaSpout configuartion
		// Second argument is the topic name
		// Third argument is the zookeeper root for Kafka
		// Fourth argument is consumer group id
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,
				"words_topic", "", "id7");

		// Specify that the kafka messages are String
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		// We want to consume all the first messages in the topic everytime
		// we run the topology to help in debugging. In production, this
		// property should be false
		kafkaConfig.forceFromStart = true;

		// Now we create the topology
		TopologyBuilder builder = new TopologyBuilder();

		// set the kafka spout class
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);

		// configure the bolts
		builder.setBolt("SentenceBolt", new SentenceBolt(), 1).globalGrouping("KafkaSpout");
		builder.setBolt("PrinterBolt", new PrinterBolt(), 1).globalGrouping("SentenceBolt");


		// create an instance of LocalCluster class for executing topology in local mode.
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();

		// Submit topology for execution
		cluster.submitTopology("KafkaToplogy", conf, builder.createTopology());


		try {
			// Wait for some time before exiting
			System.out.println("Waiting to consume from kafka");
			Thread.sleep(10000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}

		// kill the KafkaTopology
		cluster.killTopology("KafkaToplogy");

		// shut down the storm test cluster
		cluster.shutdown();
	}
}
