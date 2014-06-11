package storm.kafka.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TestLocalStormTopology {
	public static Logger LOG = LoggerFactory.getLogger(TestLocalStormTopology.class);


	public static void main(String[] args) throws Exception{
		LOG.info("FINDHY begin to Topology TopologyBuilder");
		TopologyBuilder builder = new TopologyBuilder();
		//第一个参数为spout的id，第二个参数为spout对象，第三个参数为需要为这个spout分配的task数量，一个task对应一个线程
		builder.setSpout("word", new TestWordSpout(), 1);
		builder.setBolt("wordbolt",new TestWordBolt(),1).shuffleGrouping("word");
		
		LOG.info("FINDHY end to Topology TopologyBuilder builder");
		
		LOG.info("FINDHY begin to Topology Config");
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LOG.info("FINDHY end to Topology Config");
		
		LOG.info("FINDHY begin to Topology LocalCluster");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		LOG.info("FINDHY end to Topology LocalCluster submitTopology");
		
		Utils.sleep(5000);
		LOG.info("FINDHY begin to Topology shutdown");
		cluster.killTopology("test");
		cluster.shutdown();
	}
}
