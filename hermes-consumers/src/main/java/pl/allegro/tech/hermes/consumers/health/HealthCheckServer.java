package pl.allegro.tech.hermes.consumers.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;

import static javax.ws.rs.core.Response.Status.OK;

public class HealthCheckServer {

    private final HttpServer server;

    @Inject
    public HealthCheckServer(ConfigFactory configFactory, ConsumerMonitor monitor) throws IOException {
        server = createServer(configFactory.getIntProperty(Configs.CONSUMER_HEALTH_CHECK_PORT));
        server.createContext("/status/health", (exchange) -> respondWithString(exchange, "{\"status\": \"UP\"}"));
        server.createContext("/status/subscriptions", (exchange) -> respondWithString(exchange, monitor.check("subscriptions")));
        server.createContext("/status/subscriptionsCount", (exchange) -> respondWithString(exchange, monitor.check("subscriptionsCount")));
    }

    private HttpServer createServer(int port) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.setExecutor(null);
        return httpServer;
    }

    private static void respondWithString(HttpExchange httpExchange, String response) throws IOException {
        httpExchange.getResponseHeaders().put("Content-Type", Arrays.asList("application/json"));
        httpExchange.sendResponseHeaders(OK.getStatusCode(), response.length());
        OutputStream os = httpExchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }
}
