package storm.kafka.test;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TestWordBolt extends BaseRichBolt{
	
	public static Logger LOG = LoggerFactory.getLogger(TestWordBolt.class);
	OutputCollector _collerctor;

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		LOG.info("FINDHY begin to bolt prepare");
		this._collerctor = collector;
	}

	@Override
	public void execute(Tuple input) {
		LOG.info("FINDHY begin to bolt execute");
		LOG.info("tuple=" + input);
		LOG.info("FINDHY begin to bolt ack");
		_collerctor.ack(input);
		LOG.info("FINDHY end to bolt execute");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOG.info("FINDHY begin to bolt declareOutputFields");
		declarer.declare(new Fields("testwordbolt"));
		LOG.info("FINDHY end to bolt declareOutputFields");
	}
	
}
