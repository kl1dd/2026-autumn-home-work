package company.vk.edu.distrib.compute.kl1dd.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.kv.KVService;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MyKVService implements KVService {
    private static final String GET_METHOD = "GET";
    private static final int STOP_DELAY_SECONDS = 2;
    private static final int MAX_REQUEST_BODY_SIZE = 5 * 1024 * 1024;
    private static final String CLUSTER_PROPERTY = "CLUSTER";
    private static final String INTERNAL_REQUEST_HEADER = "Internal-Request";
    private static final String INTERNAL_REQUEST_VALUE = "true";
    private static final String LOCALHOST_URL = "http://localhost:";
    private static final String ENTITY_PATH = "/v0/entity?id=";

    private static final int HTTP_OK = 200;
    private static final int HTTP_CREATED = 201;
    private static final int HTTP_ACCEPTED = 202;
    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_UNAVAILABLE = 503;

    private final HttpServer httpServer;
    private final MyKVDao dao;
    private final HttpClient httpClient = HttpClient.newHttpClient();

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
        this.dao = new MyKVDao(httpServer.getAddress().getPort());

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

    private boolean waitForAck(List<CompletableFuture<Boolean>> futures, int initialSuccess, int ack) {
        if (initialSuccess >= ack) {
            return true;
        }
        if (futures.isEmpty()) {
            return false;
        }

        AtomicInteger successCount = new AtomicInteger(initialSuccess);
        AtomicInteger completedCount = new AtomicInteger();
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        for (CompletableFuture<Boolean> future : futures) {
            future.thenAccept(success -> {
                if (success && successCount.incrementAndGet() >= ack) {
                    result.complete(true);
                }

                if (completedCount.incrementAndGet() == futures.size()
                        && successCount.get() < ack) {
                    result.complete(false);
                }
            });
        }

        return result.join();
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
                try {
                    dao.delete(id);
                    successCount++;
                } catch (IOException e) {
                    // не подтвердила
                }
            } else {
                futures.add(deleteFromReplica(port, id));
            }
        }

        boolean flag = waitForAck(futures, successCount, ack);
        if (!flag) {
            exchange.sendResponseHeaders(HTTP_UNAVAILABLE, -1);
        } else if (ackSpecified) {
            exchange.sendResponseHeaders(HTTP_OK, -1);
        } else {
            exchange.sendResponseHeaders(HTTP_ACCEPTED, -1);
        }

        exchange.close();
    }

    private void handleLocalDelete(HttpExchange exchange, String id) throws IOException {
        try {
            dao.delete(id);

            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    private CompletableFuture<Boolean> deleteFromReplica(int port, String id) {
        URI uri = URI.create(LOCALHOST_URL + port + ENTITY_PATH + id);
        HttpRequest request = HttpRequest.newBuilder(uri).header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE)
                .DELETE().build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(response -> response.statusCode() == HTTP_ACCEPTED)
                .exceptionally(e -> false);
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
                try {
                    dao.upsert(id, value);
                    successCount++;
                } catch (IOException e) {
                    // не подтвердила
                }
            } else {
                futures.add(putToReplica(port, id, value));
            }
        }

        boolean flag = waitForAck(futures, successCount, ack);
        if (flag) {
            exchange.sendResponseHeaders(201, -1);
        } else {
            exchange.sendResponseHeaders(503, -1);
        }
        exchange.close();
    }

    private void handleLocalPut(HttpExchange exchange, String id, byte[] value) throws IOException {
        try {
            dao.upsert(id, value);

            exchange.sendResponseHeaders(201, -1);
            exchange.close();
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    private CompletableFuture<Boolean> putToReplica(int port, String id, byte[] value) {
        URI uri = URI.create(LOCALHOST_URL + port + ENTITY_PATH + id);
        HttpRequest request = HttpRequest.newBuilder(uri).header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(value)).build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenApply(response -> response.statusCode() == HTTP_CREATED)
                .exceptionally(e -> false);
    }

    // GET
    private static final class GetReplicaResult {
        private final int statusCode;
        private final byte[] value;

        private GetReplicaResult(int statusCode, byte[] value) {
            this.statusCode = statusCode;
            this.value = value;
        }

        public int getStatusCode() {
            return statusCode;
        }

        public byte[] getValue() {
            return value.clone();
        }
    }

    private GetReplicaResult getLocal(String id) {
        try {
            byte[] value = dao.get(id);
            return new GetReplicaResult(200, value);
        } catch (NoSuchElementException e) {
            return new GetReplicaResult(404, null);
        } catch (IOException e) {
            return new GetReplicaResult(503, null);
        }
    }

    private GetReplicaResult waitForGetAck(List<CompletableFuture<GetReplicaResult>> futures,
                                           GetReplicaResult localResult, int ack) {
        int initialResponses = 0;
        byte[] initialValue = null;
        if (localResult.getStatusCode() == HTTP_OK) {
            initialResponses = 1;
            initialValue = localResult.getValue();
        } else if (localResult.getStatusCode() == HTTP_NOT_FOUND) {
            initialResponses = 1;
        }

        AtomicInteger responseCount = new AtomicInteger(initialResponses);
        AtomicInteger completedCount = new AtomicInteger();
        AtomicReference<byte[]> value = new AtomicReference<>(initialValue);
        if (responseCount.get() >= ack && value.get() != null) {
            return new GetReplicaResult(HTTP_OK, value.get());
        }
        if (futures.isEmpty()) {
            if (responseCount.get() >= ack) {
                return new GetReplicaResult(HTTP_NOT_FOUND, null);
            }
            return new GetReplicaResult(HTTP_UNAVAILABLE, null);
        }

        CompletableFuture<GetReplicaResult> result = new CompletableFuture<>();
        for (CompletableFuture<GetReplicaResult> future : futures) {
            future.thenAccept(replicaResult -> {
                int status = replicaResult.getStatusCode();
                if (status == HTTP_OK) {
                    value.compareAndSet(null, replicaResult.getValue());

                    if (responseCount.incrementAndGet() >= ack) {
                        result.complete(new GetReplicaResult(HTTP_OK, value.get()));
                    }
                } else if (status == HTTP_NOT_FOUND) {
                    responseCount.incrementAndGet();
                }

                if (completedCount.incrementAndGet() == futures.size()) {
                    if (responseCount.get() >= ack) {
                        byte[] foundValue = value.get();

                        if (foundValue != null) {
                            result.complete(new GetReplicaResult(HTTP_OK, foundValue));
                        } else {
                            result.complete(new GetReplicaResult(HTTP_NOT_FOUND, null));
                        }
                    } else {
                        result.complete(new GetReplicaResult(HTTP_UNAVAILABLE, null));
                    }
                }
            });
        }

        return result.join();
    }

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

        GetReplicaResult localResult = getLocal(id);
        int currentPort = httpServer.getAddress().getPort();
        List<CompletableFuture<GetReplicaResult>> futures = new ArrayList<>();
        for (int port : getClusterPorts()) {
            if (port != currentPort) {
                futures.add(getFromReplica(port, id));
            }
        }

        GetReplicaResult result = waitForGetAck(futures, localResult, ack);
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
        try {
            byte[] value = dao.get(id);

            exchange.sendResponseHeaders(200, value.length);
            exchange.getResponseBody().write(value);
            exchange.close();
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        } catch (IOException e) {
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
        }
    }

    private CompletableFuture<GetReplicaResult> getFromReplica(int port, String id) {
        URI uri = URI.create(LOCALHOST_URL + port + ENTITY_PATH + id);
        HttpRequest request = HttpRequest.newBuilder(uri).header(INTERNAL_REQUEST_HEADER, INTERNAL_REQUEST_VALUE)
                .GET().build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> {
                    if (response.statusCode() == HTTP_OK) {
                        return new GetReplicaResult(200, response.body());
                    }

                    if (response.statusCode() == HTTP_NOT_FOUND) {
                        return new GetReplicaResult(404, null);
                    }

                    return new GetReplicaResult(503, null);
                })
                .exceptionally(e -> new GetReplicaResult(HTTP_UNAVAILABLE, null));
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
