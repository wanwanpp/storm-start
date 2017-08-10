package com.wp.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.wp.bolt.SpliterBolt;
import com.wp.bolt.WriterBolt;
import com.wp.spout.MessageSpout;

public class MessageTopology {
	
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MessageSpout());
		builder.setBolt("split-bolt", new SpliterBolt()).shuffleGrouping("spout");
		builder.setBolt("write-bolt", new WriterBolt()).shuffleGrouping("split-bolt");
        //本地配置
        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        System.out.println(cluster);
        cluster.submitTopology("message", config, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("message");
        cluster.shutdown();
	}
}
