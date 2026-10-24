package company.vk.edu.distrib.compute.kl1dd.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MyKVService implements KVService {
    private static final String GET_METHOD = "GET";
    private static final int STOP_DELAY_SECONDS = 2;
    private static final int MAX_REQUEST_BODY_SIZE = 5 * 1024 * 1024;
    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String INTERNAL_REQUEST_HEADER = "Internal-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";

    private static final int HTTP_OK = 200;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_UNAVAILABLE = 503;

    private final HttpServer httpServer;
    private final DaoHelper daoHelper;
    private final ReplicationImpl replication = new ReplicationImpl();
    private final ReplicationClient replicationClient = new ReplicationClient();

    private int[] getClusterPorts() {
        String cluster = System.getProperty(CLUSTER_PROPERTY);
        if (cluster == null || cluster.isEmpty()) {
            return new int[]{httpServer.getAddress().getPort()};
        }

        String[] ports = cluster.split(",");
        int[] results = new int[ports.length];
        for (int i = 0; i < ports.length; i++) {
            results[i] = Integer.parseInt(ports[i]);
        }
        return results;
    }

    private String getQueryParameter(HttpExchange exchange, String name) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }

        String[] splitted = query.split("&");
        String prefix = name + "=";
        for (String param : splitted) {
            if (param.startsWith(prefix)) {
                return param.substring(prefix.length());
            }
        }

        return null;
    }

    private boolean isInternalRequest(HttpExchange exchange) {
        String value = exchange.getRequestHeaders().getFirst(INTERNAL_REQUEST_HEADER);
        return INTERNAL_REQUEST_VALUE.equals(value);
    }

    public MyKVService(HttpServer httpServer) throws IOException {
        this.httpServer = httpServer;
        this.daoHelper = new DaoHelper(httpServer.getAddress().getPort());

        httpServer.createContext("/v0/status", this::handleStatus);
        httpServer.createContext("/v0/entity", this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!GET_METHOD.equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }

        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case "GET" -> {
                handleGet(exchange);
            }
            case "PUT" -> {
                handlePut(exchange);
            }
            case "DELETE" -> {
                handleDelete(exchange);
            }
            default -> {
                exchange.sendResponseHeaders(405, -1);
                exchange.close();
            }
        }
    }

    // DELETE
    private void handleDelete(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        if (isInternalRequest(exchange)) {
            handleLocalDelete(exchange, id);
            return;
        }

        int ack = getAck(exchange);
        boolean ackSpecified = (getQueryParameter(exchange, "ack") != null);
        if (ack == -1) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        int successCount = 0;
        int currentPort = httpServer.getAddress().getPort();
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int port : getClusterPorts()) {
            if (port == currentPort) {
                if (daoHelper.delete(id)) {
                    successCount++;
                }
            } else {
                futures.add(replicationClient.deleteFromReplica(port, id));
            }
        }

        boolean flag = replication.waitForAck(futures, successCount, ack);
        exchange.sendResponseHeaders(responseStatusDelete(flag, ackSpecified), -1);
        exchange.close();
    }

    private int responseStatusDelete(boolean flag, boolean ackSpecified) {
        if (flag) {
            if (ackSpecified) {
                return HTTP_OK;
            }
            return HTTP_ACCEPTED;
        }

        return HTTP_UNAVAILABLE;
    }

    private void handleLocalDelete(HttpExchange exchange, String id) throws IOException {
        if (daoHelper.delete(id)) {
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        } else {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    // PUT
    private void handlePut(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        byte[] value;
        try {
            value = exchange.getRequestBody().readNBytes(MAX_REQUEST_BODY_SIZE + 1);
            if (value.length > MAX_REQUEST_BODY_SIZE) {
                exchange.sendResponseHeaders(413, -1);
                exchange.close();
                return;
            }
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
            return;
        }

        if (isInternalRequest(exchange)) {
            handleLocalPut(exchange, id, value);
            return;
        }

        int ack = getAck(exchange);
        if (ack == -1) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        int successCount = 0;
        int currentPort = httpServer.getAddress().getPort();
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int port : getClusterPorts()) {
            if (port == currentPort) {
                if (daoHelper.upsert(id, value)) {
                    successCount++;
                }
            } else {
                futures.add(replicationClient.putToReplica(port, id, value));
            }
        }

        boolean flag = replication.waitForAck(futures, successCount, ack);
        if (flag) {
            exchange.sendResponseHeaders(201, -1);
        } else {
            exchange.sendResponseHeaders(503, -1);
        }
        exchange.close();
    }

    private void handleLocalPut(HttpExchange exchange, String id, byte[] value) throws IOException {
        if (daoHelper.upsert(id, value)) {
            exchange.sendResponseHeaders(201, -1);
            exchange.close();
        } else {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    // GET
    private void handleGet(HttpExchange exchange) throws IOException {
        String id = getId(exchange);
        if (id == null) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        if (isInternalRequest(exchange)) {
            handleLocalGet(exchange, id);
            return;
        }

        int ack = getAck(exchange);
        if (ack == -1) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        ReplicationImpl.GetReplicaResult localResult = daoHelper.getResult(id);
        int currentPort = httpServer.getAddress().getPort();
        List<CompletableFuture<ReplicationImpl.GetReplicaResult>> futures = new ArrayList<>();
        for (int port : getClusterPorts()) {
            if (port != currentPort) {
                futures.add(replicationClient.getFromReplica(port, id));
            }
        }

        ReplicationImpl.GetReplicaResult result = replication.waitForGetAck(futures, localResult, ack);
        int resultStatusCode = result.getStatusCode();
        if (resultStatusCode == HTTP_OK) {
            byte[] resultValue = result.getValue();
            exchange.sendResponseHeaders(HTTP_OK, resultValue.length);
            exchange.getResponseBody().write(resultValue);
            exchange.close();
        } else if (resultStatusCode == HTTP_NOT_FOUND) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        } else {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    private void handleLocalGet(HttpExchange exchange, String id) throws IOException {
        ReplicationImpl.GetReplicaResult result = daoHelper.getResult(id);
        int resultStatusCode = result.getStatusCode();
        if (resultStatusCode == 200) {
            byte[] resultValue = result.getValue();

            exchange.sendResponseHeaders(200, resultValue.length);
            exchange.getResponseBody().write(resultValue);
            exchange.close();
        } else if (resultStatusCode == 404) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        } else if (resultStatusCode == 503) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    private String getId(HttpExchange exchange) {
        String id = getQueryParameter(exchange, "id");
        if (id == null || id.isEmpty()) {
            return null;
        }

        return id;
    }

    private int getAck(HttpExchange exchange) {
        String ack = getQueryParameter(exchange, "ack");
        if (ack == null) {
            return 1;
        }

        int ackInt;
        try {
            ackInt = Integer.parseInt(ack);
        } catch (NumberFormatException e) {
            return -1;
        }

        if (ackInt < 1 || ackInt > getClusterPorts().length) {
            return -1;
        }
        return ackInt;
    }

    @Override
    public void start() {
        httpServer.start();
    }

    @Override
    public void stop() {
        httpServer.stop(STOP_DELAY_SECONDS);
    }
}
