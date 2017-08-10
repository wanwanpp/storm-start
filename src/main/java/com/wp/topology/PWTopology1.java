package com.wp.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.wp.bolt.PrintBolt;
import com.wp.bolt.WriteBolt;
import com.wp.spout.PWSpout;

public class PWTopology1 {

	public static void main(String[] args) throws Exception {
		//
		Config cfg = new Config();
		cfg.setNumWorkers(2);
		cfg.setDebug(true);
		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PWSpout());
		builder.setBolt("print-bolt", new PrintBolt()).shuffleGrouping("spout");
		builder.setBolt("write-bolt", new WriteBolt()).shuffleGrouping("print-bolt");
		
		
		//1 本地模式
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("top1", cfg, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("top1");
		cluster.shutdown();
		
		//2 集群模式
//		StormSubmitter.submitTopology("top1", cfg, builder.createTopology());
		
	}
}
