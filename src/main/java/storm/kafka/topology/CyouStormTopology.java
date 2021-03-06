package storm.kafka.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class CyouStormTopology {
	
	public static final Logger LOG = LoggerFactory.getLogger(CyouStormTopology.class);
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts("master"), "wikipedia", "", "kafka-storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("wikispout", new KafkaSpout(kafkaConfig),1);
		builder.setBolt("wikibolt", new CyouSendToKafkaBolt()).shuffleGrouping("wikispout");
		
		Config conf = new Config();
	    conf.setDebug(true);
	    conf.setNumWorkers(3);
	    StormSubmitter.submitTopology("kafka-storm", conf, builder.createTopology());
	}
}
