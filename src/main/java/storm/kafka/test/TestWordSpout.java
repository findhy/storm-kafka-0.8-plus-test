package storm.kafka.test;

import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 测试定时发射单词
 * @author sunwei_oversea
 *
 */
public class TestWordSpout extends BaseRichSpout{
	
	public static Logger LOG = LoggerFactory.getLogger(TestWordSpout.class);
	SpoutOutputCollector _collector;

	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		LOG.info("FINDHY begin to spout open");
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		LOG.info("FINDHY begin to spout nextTuple");
		//Utils.sleep(100);
		final String[] words = new String[]{"nathan","mike","jackson","golda","bertels"};
		final Random rand = new Random();
		final String word = words[rand.nextInt(words.length)];
		LOG.info("FINDHY begin to spout emit");
		_collector.emit(new Values(word));
		LOG.info("FINDHY end to spout emit");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOG.info("FINDHY begin to Spout declareOutputFields");
		declarer.declare(new Fields("testwordspout"));
		LOG.info("FINDHY end to Spout declareOutputFields");
	}
	
	

}
