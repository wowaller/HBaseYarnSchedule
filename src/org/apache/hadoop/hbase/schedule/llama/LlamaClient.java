package org.apache.hadoop.hbase.schedule.llama;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.llama.server.TypeUtils;
import com.cloudera.llama.thrift.LlamaAMService;
import com.cloudera.llama.thrift.LlamaNotificationService;
import com.cloudera.llama.thrift.TAllocatedResource;
import com.cloudera.llama.thrift.TLlamaAMNotificationRequest;
import com.cloudera.llama.thrift.TLlamaAMNotificationResponse;
import com.cloudera.llama.thrift.TLlamaNMNotificationRequest;
import com.cloudera.llama.thrift.TLlamaNMNotificationResponse;
import com.cloudera.llama.thrift.TUniqueId;
import com.cloudera.llama.util.UUID;

public class LlamaClient {
	private static final Logger LOG = LoggerFactory
			.getLogger(LlamaClient.class);

	public class CallbackService implements LlamaNotificationService.Iface {
		@Override
		public TLlamaAMNotificationResponse AMNotification(
				TLlamaAMNotificationRequest request) throws TException {
			// LOG.info(request.toString());
			// System.out.println(request.toString());

			if (request.isSetAllocated_reservation_ids()) {
				// LOG.trace("Allocated: "
				// + TypeUtils.toUUIDString(request
				// .getAllocated_reservation_ids()));
				// System.out.println("Allocated: "
				// + TypeUtils.toUUIDString(request
				// .getAllocated_reservation_ids()));
				//
				// for (TUniqueId r : request.getAllocated_reservation_ids()) {
				// if (request.getAllocated_resourcesSize() < 3) {
				// LOG.trace(r + ":" + request.toString());
				// System.out.println(r + ":" + request.toString());
				// }
				// notifyStatusChange(TypeUtils.toUUID(r));
				// }
				for (NotificationListener listener : listeners) {
					listener.onAllocated(request);
				}
			}
			if (request.isSetPreempted_reservation_ids()) {
				// LOG.trace("PreEmpted: "
				// + TypeUtils.toUUIDString(request
				// .getAllocated_reservation_ids()));
				// for (TUniqueId r : request.getPreempted_reservation_ids()) {
				// notifyStatusChange(TypeUtils.toUUID(r));
				// }
				for (NotificationListener listener : listeners) {
					listener.onPreempted(request);
				}
			}
			if (request.isSetRejected_reservation_ids()) {
				// LOG.trace("Rejected: "
				// + TypeUtils.toUUIDString(request
				// .getAllocated_reservation_ids()));
				//
				// for (TUniqueId r : request.getRejected_reservation_ids()) {
				// notifyStatusChange(TypeUtils.toUUID(r));
				// }
				for (NotificationListener listener : listeners) {
					listener.onRejected(request);
				}
			}
			if (request.isSetLost_reservation_ids()) {
				// LOG.trace("Lost: "
				// + TypeUtils.toUUIDString(request
				// .getAllocated_reservation_ids()));
				//
				// for (TUniqueId r : request.getLost_reservation_ids()) {
				// notifyStatusChange(TypeUtils.toUUID(r));
				// }
				for (NotificationListener listener : listeners) {
					listener.onLost(request);
				}
			}
			if (request.isSetAdmin_released_reservation_ids()) {
				// LOG.trace("Admin released: "
				// + TypeUtils.toUUIDString(request
				// .getAllocated_reservation_ids()));
				//
				// for (TUniqueId r :
				// request.getAdmin_released_reservation_ids()) {
				// notifyStatusChange(TypeUtils.toUUID(r));
				// }
				for (NotificationListener listener : listeners) {
					listener.onReleased(request);
				}
			}
			return new TLlamaAMNotificationResponse().setStatus(TypeUtils.OK);
		}

		@Override
		public TLlamaNMNotificationResponse NMNotification(
				TLlamaNMNotificationRequest request) throws TException {
			if (LOG.isDebugEnabled()) {
				LOG.debug(request.toString());
			}
			return new TLlamaNMNotificationResponse().setStatus(TypeUtils.OK);
		}
	}

	public class CallbackListener implements NotificationListener {

		@Override
		public void onAllocated(TLlamaAMNotificationRequest request) {
			for (TAllocatedResource resource : request.getAllocated_resources()) {
				Resource reservation = reserved.get(TypeUtils.toUUID(resource
						.getReservation_id()));
				if (reservation != null) {
					reservation.allocate(resource.getLocation(),
							resource.getV_cpu_cores(), resource.getMemory_mb());
				}
			}
		}

		@Override
		public void onReleased(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getAdmin_released_reservation_ids()) {
				reserved.remove(TypeUtils.toUUID(id));
			}
		}

		@Override
		public void onRejected(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getRejected_reservation_ids()) {
				reserved.remove(TypeUtils.toUUID(id));
			}
		}

		@Override
		public void onPreempted(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getPreempted_reservation_ids()) {
				reserved.remove(TypeUtils.toUUID(id));
			}
		}

		@Override
		public void onLost(TLlamaAMNotificationRequest request) {
			for (TUniqueId id : request.getLost_reservation_ids()) {
				reserved.remove(TypeUtils.toUUID(id));
			}
		}

	}

	private String host;
	private int port;
	private int callBackPort;
	private UUID clientId;
	private UUID handleId;
	private Thread callBackThread;
	private TServer callBackServer;
	private LlamaAMService.Client client;
	private String callBaseHost;
	private Map<UUID, Resource> reserved;
	private List<String> nodes;
	private List<NotificationListener> listeners;
	private boolean isClientCreated;
	private boolean isClientRegistered;
	private boolean isCallbackCreated;
	private boolean isClientSecure;

	public LlamaClient(String host, int port, String callBackHost,
			int callBackPort, boolean isSecure) throws Exception {
		this.host = host;
		this.port = port;
		this.callBaseHost = callBackHost;
		this.callBackPort = callBackPort;
		this.clientId = UUID.randomUUID();
		this.reserved = new ConcurrentHashMap<UUID, Resource>();
		this.nodes = new ArrayList<String>();
		this.listeners = new ArrayList<NotificationListener>();
		this.callBackThread = null;
		this.isClientCreated = false;
		this.isClientRegistered = false;
		this.isCallbackCreated = false;
		this.isClientSecure = false;
		this.client = null;
		addListener(new CallbackListener());
	}

	public void connect() throws Exception {
		this.client = createClient(isClientSecure, host, port);
	}

	public void close() {
		if (isClientCreated && isClientRegistered) {
			LOG.info("Unregister client.");
			try {
				unregister(isClientSecure);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				LOG.error("Failed to ungister client.");
			}
		}
		if (isCallbackCreated && callBackServer != null) {
			LOG.info("Close callback server.");
			callBackServer.stop();
		}
		if (isCallbackCreated && callBackThread != null) {
			LOG.info("Close callback thread.");
			callBackThread.interrupt();
		}
		reserved.clear();
		this.isClientCreated = false;
		this.isClientRegistered = false;
		this.isCallbackCreated = false;
	}

	public LlamaAMService.Client createClient(boolean secure, String host,
			int port) throws Exception {
		client = LlamaClientUtil.createClient(secure, host, port);
		isClientCreated = true;
		isClientSecure = secure;
		return client;
	}

	public UUID register(boolean isSecure) throws Exception {

		handleId = LlamaClientUtil.register(isSecure, client, clientId,
				callBaseHost, callBackPort);
		isClientRegistered = true;

		return handleId;
	}

	public List<String> getNodes(boolean isSecure) throws Exception {
		nodes = LlamaClientUtil.getNodes(isSecure, client, handleId);
		return nodes;
	}

	public UUID reserve(boolean isSecure, String[] locations, String user,
			String queue, int cpus, int memory, boolean relaxLocality,
			boolean gang) throws Exception {
		UUID reserveId;
		try {
			reserveId = LlamaClientUtil.reserve(isSecure, client, handleId,
					user, queue, locations, cpus, memory, relaxLocality, gang);
		} catch (Exception e) {
			LOG.error("Reserve failed. Re-registering....");
			register(isSecure);
			reserveId = LlamaClientUtil.reserve(isSecure, client, handleId,
					user, queue, locations, cpus, memory, relaxLocality, gang);
		}
		reserved.put(reserveId, new Resource(locations, cpus, memory,
				relaxLocality));
		return reserveId;
	}

	public UUID expand(boolean isSecure, UUID reservationId, String user,
			String queue, String location, boolean relaxLocality, int cpus,
			int memory) throws Exception {
		UUID expandId;
		try {
			expandId = LlamaClientUtil.expand(isSecure, client, handleId,
					reservationId, location, cpus, memory, relaxLocality);
		} catch (Exception e) {
			LOG.error("Expand failed. Re-registering....");
			register(isSecure);
			expandId = LlamaClientUtil.expand(isSecure, client, handleId,
					reservationId, location, cpus, memory, relaxLocality);
		}
		String[] locations = { location };
		reserved.put(expandId, new Resource(locations, cpus, memory,
				relaxLocality));
		return expandId;
	}

	public void release(boolean isSecure, UUID reservationId) throws Exception {
		try {
			LlamaClientUtil.release(isSecure, client, handleId, reservationId);
		} catch (Exception e) {
			LOG.error("Release failed. Re-registering....");
			register(isSecure);
			LlamaClientUtil.release(isSecure, client, handleId, reservationId);
		}
		if (reserved.containsKey(reservationId)) {
			reserved.remove(reservationId);
		}
	}

	public void unregister(boolean isSecure) throws Exception {
		LlamaClientUtil.unregister(isSecure, client, handleId);
		isClientRegistered = false;
	}

	public void startLlamaNotificationServer() {

		Runnable run = new Runnable() {
			@Override
			public void run() {

				try {
					LlamaNotificationService.Processor processor = new LlamaNotificationService.Processor(
							new CallbackService());
					TServerTransport serverTransport = new TServerSocket(
							callBackPort);
					callBackServer = new TSimpleServer(new TServer.Args(
							serverTransport).processor(processor));
					callBackServer.serve();
				} catch (TTransportException e) {
					LOG.error("Failed to start callbacl server.", e);
				}

			}

		};

		callBackThread = new Thread(run);
		callBackThread.start();
		isCallbackCreated = true;
	}

	public void addListener(NotificationListener listener) {
		listeners.add(listener);
	}

	public void resetListener() {
		listeners.clear();
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public UUID getHandleId() {
		return handleId;
	}

	public Map<UUID, Resource> getReserved() {
		return reserved;
	}

	public List<String> getNodes() {
		return nodes;
	}

	public boolean isClientCreated() {
		return isClientCreated;
	}

	public boolean isClientRegistered() {
		return isClientRegistered;
	}

	public boolean isCallbackCreated() {
		return isCallbackCreated;
	}

	public boolean isClientSecure() {
		return isClientSecure;
	}

	public String getHost() {
		return host;
	}

	public int getTotalCpuRequire() {
		int sum = 0;
		for (Resource resource : reserved.values()) {
			sum += resource.getRequireCpu().get();
		}
		return sum;
	}

	public int getTotalMemRequire() {
		int sum = 0;
		for (Resource resource : reserved.values()) {
			sum += resource.getRequireMem().get();
		}
		return sum;
	}

	public int getTotalCpuAllocated() {
		return getTotalCpu() - getTotalCpuRequire();
	}

	public int getTotalMemAllocated() {
		return getTotalMem() - getTotalMemRequire();
	}

	public int getTotalCpu() {
		int sum = 0;
		for (Resource resource : reserved.values()) {
			sum += resource.getTotalCpu();
		}
		return sum;
	}

	public int getTotalMem() {
		int sum = 0;
		for (Resource resource : reserved.values()) {
			sum += resource.getTotalMem();
		}
		return sum;
	}

}
