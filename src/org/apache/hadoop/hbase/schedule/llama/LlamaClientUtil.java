package org.apache.hadoop.hbase.schedule.llama;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.llama.server.Security;
import com.cloudera.llama.server.TypeUtils;
import com.cloudera.llama.thrift.LlamaAMService;
import com.cloudera.llama.thrift.LlamaNotificationService;
import com.cloudera.llama.thrift.LlamaNotificationService.Iface;
import com.cloudera.llama.thrift.TLlamaAMGetNodesRequest;
import com.cloudera.llama.thrift.TLlamaAMGetNodesResponse;
import com.cloudera.llama.thrift.TLlamaAMRegisterRequest;
import com.cloudera.llama.thrift.TLlamaAMRegisterResponse;
import com.cloudera.llama.thrift.TLlamaAMReleaseRequest;
import com.cloudera.llama.thrift.TLlamaAMReleaseResponse;
import com.cloudera.llama.thrift.TLlamaAMReservationExpansionRequest;
import com.cloudera.llama.thrift.TLlamaAMReservationExpansionResponse;
import com.cloudera.llama.thrift.TLlamaAMReservationRequest;
import com.cloudera.llama.thrift.TLlamaAMReservationResponse;
import com.cloudera.llama.thrift.TLlamaAMUnregisterRequest;
import com.cloudera.llama.thrift.TLlamaAMUnregisterResponse;
import com.cloudera.llama.thrift.TLlamaServiceVersion;
import com.cloudera.llama.thrift.TLocationEnforcement;
import com.cloudera.llama.thrift.TNetworkAddress;
import com.cloudera.llama.thrift.TResource;
import com.cloudera.llama.thrift.TStatusCode;
import com.cloudera.llama.util.UUID;

public class LlamaClientUtil {
	static LlamaAMService.Client createClient(boolean secure, String host,
			int port) throws Exception {
		TTransport transport = new TSocket(host, port);
		if (secure) {
			Map<String, String> saslProperties = new HashMap<String, String>();
			saslProperties.put(Sasl.QOP, "auth-conf,auth-int,auth");
			transport = new TSaslClientTransport("GSSAPI", null, "llama", host,
					saslProperties, null, transport);
		}
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		return new LlamaAMService.Client(protocol);
	}

	static Subject getSubject(boolean secure) throws Exception {
		return (secure) ? Security.loginClientFromKinit() : new Subject();
	}
	
	public static TServer createThriftServer(int port, Class<? extends Iface> serviceKlass)
			throws TTransportException, ClassNotFoundException {
		LlamaNotificationService.Iface service = 
		        ReflectionUtils.newInstance(serviceKlass, new Configuration());
		LlamaNotificationService.Processor processor = new LlamaNotificationService.Processor(
				service);
		TServerTransport serverTransport = new TServerSocket(port);
		return new TSimpleServer(
				new TServer.Args(serverTransport).processor(processor));
	}

	static List<String> getNodes(final boolean secure,
			final LlamaAMService.Client client, final UUID handle)
			throws Exception {
		return Subject.doAs(getSubject(secure),
				new PrivilegedExceptionAction<List<String>>() {
					@Override
					public List<String> run() throws Exception {
						TLlamaAMGetNodesRequest req = new TLlamaAMGetNodesRequest();
						req.setVersion(TLlamaServiceVersion.V1);
						req.setAm_handle(TypeUtils.toTUniqueId(handle));
						TLlamaAMGetNodesResponse res = client.GetNodes(req);
						if (res.getStatus().getStatus_code() != TStatusCode.OK) {
							throw new RuntimeException(res.toString());
						}
						return res.getNodes();
					}
				});
	}

	static UUID expand(final boolean secure,
			final LlamaAMService.Client client, final UUID handle,
			final UUID reservation, final String location, final int cpus,
			final int memory, final boolean relaxLocality) throws Exception {
		return Subject.doAs(getSubject(secure),
				new PrivilegedExceptionAction<UUID>() {
					@Override
					public UUID run() throws Exception {
						return expand(client, handle, reservation, location,
								relaxLocality, cpus, memory);
					}
				});
	}

	static UUID expand(LlamaAMService.Client client, UUID handle,
			UUID reservation, String location, boolean relaxLocality, int cpus,
			int memory) throws Exception {
		TLlamaAMReservationExpansionRequest req = new TLlamaAMReservationExpansionRequest();
		req.setVersion(TLlamaServiceVersion.V1);
		req.setAm_handle(TypeUtils.toTUniqueId(handle));
		req.setExpansion_of(TypeUtils.toTUniqueId(reservation));

		TResource resource = new TResource();
		resource.setClient_resource_id(TypeUtils.toTUniqueId(UUID.randomUUID()));
		resource.setAskedLocation(location);
		resource.setV_cpu_cores((short) cpus);
		resource.setMemory_mb(memory);
		resource.setEnforcement((relaxLocality) ? TLocationEnforcement.PREFERRED
				: TLocationEnforcement.MUST);

		req.setResource(resource);
		req.setExpansion_id(TypeUtils.toTUniqueId(UUID.randomUUID()));
		TLlamaAMReservationExpansionResponse res = client.Expand(req);
		if (res.getStatus().getStatus_code() != TStatusCode.OK) {
			String status = res.getStatus().getStatus_code().toString();
			int code = (res.getStatus().isSetError_code()) ? res.getStatus()
					.getError_code() : 0;
			String msg = (res.getStatus().isSetError_msgs()) ? res.getStatus()
					.getError_msgs().get(0) : "";
			throw new RuntimeException(status + " - " + code + " - " + msg);
		}
		return TypeUtils.toUUID(res.getReservation_id());
	}

	static UUID register(final boolean secure,
			final LlamaAMService.Client client, final UUID clientId,
			final String callbackHost, final int callbackPort) throws Exception {
		return Subject.doAs(getSubject(secure),
				new PrivilegedExceptionAction<UUID>() {
					@Override
					public UUID run() throws Exception {
						return register(client, clientId, callbackHost,
								callbackPort);
					}
				});
	}

	static UUID register(LlamaAMService.Client client, UUID clientId,
			String callbackHost, int callbackPort) throws Exception {
		TLlamaAMRegisterRequest req = new TLlamaAMRegisterRequest();
		req.setVersion(TLlamaServiceVersion.V1);
		req.setClient_id(TypeUtils.toTUniqueId(clientId));
		TNetworkAddress tAddress = new TNetworkAddress();
		tAddress.setHostname(callbackHost);
		tAddress.setPort(callbackPort);
		req.setNotification_callback_service(tAddress);
		TLlamaAMRegisterResponse res = client.Register(req);
		if (res.getStatus().getStatus_code() != TStatusCode.OK) {
			throw new RuntimeException(res.toString());
		}
		return TypeUtils.toUUID(res.getAm_handle());
	}

	static void release(final boolean secure,
			final LlamaAMService.Client client, final UUID handle,
			final UUID reservation) throws Exception {
		Subject.doAs(getSubject(secure), new PrivilegedExceptionAction<Void>() {
			@Override
			public Void run() throws Exception {
				release(client, handle, reservation);
				return null;
			}
		});
	}

	static void release(LlamaAMService.Client client, UUID handle,
			UUID reservation) throws Exception {
		TLlamaAMReleaseRequest req = new TLlamaAMReleaseRequest();
		req.setVersion(TLlamaServiceVersion.V1);
		req.setAm_handle(TypeUtils.toTUniqueId(handle));
		req.setReservation_id(TypeUtils.toTUniqueId(reservation));
		TLlamaAMReleaseResponse res = client.Release(req);
		if (res.getStatus().getStatus_code() != TStatusCode.OK) {
			throw new RuntimeException(res.toString());
		}
	}

	static UUID reserve(final boolean secure,
			final LlamaAMService.Client client, final UUID handle,
			final String user, final String queue, final String[] locations,
			final int cpus, final int memory, final boolean relaxLocality,
			final boolean gang) throws Exception {
		return Subject.doAs(getSubject(secure),
				new PrivilegedExceptionAction<UUID>() {
					@Override
					public UUID run() throws Exception {
						return reserve(client, handle, user, queue, locations,
								relaxLocality, cpus, memory, gang);
					}
				});
	}

	static UUID reserve(LlamaAMService.Client client, UUID handle, String user,
			String queue, String[] locations, boolean relaxLocality, int cpus,
			int memory, boolean gang) throws Exception {
		TLlamaAMReservationRequest req = new TLlamaAMReservationRequest();
		req.setVersion(TLlamaServiceVersion.V1);
		req.setAm_handle(TypeUtils.toTUniqueId(handle));
		req.setUser(user);
		req.setQueue(queue);
		req.setReservation_id(TypeUtils.toTUniqueId(UUID.randomUUID()));
		req.setGang(gang);
		List<TResource> resources = new ArrayList<TResource>();
		for (String location : locations) {
			TResource resource = new TResource();
			resource.setClient_resource_id(TypeUtils.toTUniqueId(UUID
					.randomUUID()));
			resource.setAskedLocation(location);
			resource.setV_cpu_cores((short) cpus);
			resource.setMemory_mb(memory);
			resource.setEnforcement((relaxLocality) ? TLocationEnforcement.PREFERRED
					: TLocationEnforcement.MUST);
			resources.add(resource);
		}
		req.setResources(resources);
		TLlamaAMReservationResponse res = client.Reserve(req);
		if (res.getStatus().getStatus_code() != TStatusCode.OK) {
			String status = res.getStatus().getStatus_code().toString();
			int code = (res.getStatus().isSetError_code()) ? res.getStatus()
					.getError_code() : 0;
			String msg = (res.getStatus().isSetError_msgs()) ? res.getStatus()
					.getError_msgs().get(0) : "";
			throw new RuntimeException(status + " - " + code + " - " + msg);
		}
		return TypeUtils.toUUID(res.getReservation_id());
	}

	static void unregister(final boolean secure,
			final LlamaAMService.Client client, final UUID handle)
			throws Exception {
		Subject.doAs(getSubject(secure), new PrivilegedExceptionAction<Void>() {
			@Override
			public Void run() throws Exception {
				unregister(client, handle);
				return null;
			}
		});
	}

	static void unregister(LlamaAMService.Client client, UUID handle)
			throws Exception {
		TLlamaAMUnregisterRequest req = new TLlamaAMUnregisterRequest();
		req.setVersion(TLlamaServiceVersion.V1);
		req.setAm_handle(TypeUtils.toTUniqueId(handle));
		TLlamaAMUnregisterResponse res = client.Unregister(req);
		if (res.getStatus().getStatus_code() != TStatusCode.OK) {
			throw new RuntimeException(res.toString());
		}
	}

}
