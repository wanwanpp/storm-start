package com.wp.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.wp.util.Utils;

import java.util.Map;

public class WordSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	 
	private int index = 0;
	
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
	
	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForSeconds(1);
	}
	

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	@Override
	public void ack(Object msgId) {
	}
	
	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void activate() {
		
	}

	@Override
	public void close() {
		
	}

	@Override
	public void deactivate() {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
