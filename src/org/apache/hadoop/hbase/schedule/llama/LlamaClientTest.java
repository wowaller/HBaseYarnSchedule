package org.apache.hadoop.hbase.schedule.llama;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.llama.util.UUID;

public class LlamaClientTest {
	static final Log LOG = LogFactory.getLog(LlamaClientTest.class);

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props = new Properties();
		try {
			props.load(new FileReader(args[0]));
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			LOG.info(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			LOG.info(e.getMessage());
			System.exit(1);
		}
		
		String host = props.getProperty("llama_host");
		int port = Integer.valueOf(props.getProperty("llama_port"));
		final int callbackPort = Integer.valueOf(props
				.getProperty("callback_port"));
		String cmdFile = props.getProperty("cmd_file");
		long interval = Long
				.valueOf(props.getProperty("interval", "10000"));
		String queue = props.getProperty("queue");
		String user = props.getProperty("user");
		int round = Integer.valueOf(props.getProperty("round", "1"));
		
		BufferedReader reader = new BufferedReader(new FileReader(cmdFile));
		
		String callBackHost = InetAddress.getLocalHost().getHostAddress();
		LlamaClient client = new LlamaClient(host, port, callBackHost,
				callbackPort, true);
		client.startLlamaNotificationServer();
		client.connect();
		client.register(false);
		List<String> nodes = client.getNodes(false);
		
		
		LOG.info("Got nodes.");
		for (String node : nodes) {
			LOG.info("Node: " + node);
		}
		System.in.read();
		
		for (int i = 0; i < round; i++) {
			LOG.info("Round " + i);
			LOG.info("Allocate resource");
			for (String node : nodes) {
				LOG.info("Allocate resource to node " + node);
				String[] locations = {node};
				UUID reserveId = client.reserve(false, locations, user, queue, 1, 1024, false, true);
				LOG.info("Got UUID " + reserveId);
			}
	//		System.in.read();
	//		LOG.info("Expand resource");
	//		for (UUID id : client.getReserved().keySet()) {
	//			LOG.info("Expand to UUID " + id);
	//			UUID expandId = client.expand(false, id, "root", "", nodes.get(0), false, 1, 1024);
	//			LOG.info("Expanded UUID " + id + " by " + expandId);
	//		}
//			System.in.read();
			Thread.sleep(1000);
			LOG.info("Allocate resource2");
			for (String node : nodes) {
				LOG.info("Allocate resource to node " + node);
				String[] locations = {node};
				UUID reserveId = client.reserve(false, locations, user, queue, 1, 1024, false, true);
				LOG.info("Got UUID " + reserveId);
			}
//			System.in.read();
			Thread.sleep(1000);
			LOG.info("Allocate resource3");
			for (String node : nodes) {
				LOG.info("Allocate resource to node " + node);
				String[] locations = {node};
				UUID reserveId = client.reserve(false, locations, user, queue, 1, 1024, false, true);
				LOG.info("Got UUID " + reserveId);
			}
//			System.in.read();
			Thread.sleep(1000);
			LOG.info("Release Reserved");
			for (UUID id : client.getReserved().keySet()) {
				LOG.info("Release UUID " + id);
				client.release(false, id);
				LOG.info("Released UUID " + id);
			}
//			System.in.read();
			LOG.info("Finished round " + i);
			Thread.sleep(5000);
		}
//		String line;
//		while ((line = reader.readLine()) != null) {
//			String[] param = line.split(",");
//			if (param[0].equalsIgnoreCase("reserve")) {
//				
//				client.reserve(false, locations, "", queue, cpus, memory, relaxLocality, gang)
//			} else if (param[0].equalsIgnoreCase("expand")) {
//				
//			} else if (param[0].equalsIgnoreCase("release")) {
//				
//			}
//			Thread.sleep(interval);
//		}
//		client.unregister(false);
		client.close();

	}

}
