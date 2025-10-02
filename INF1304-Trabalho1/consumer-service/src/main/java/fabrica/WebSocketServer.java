package fabrica;

import javax.websocket.OnClose;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.glassfish.tyrus.server.Server;

/**
 * Servidor WebSocket responsável por permitir comunicação em tempo real
 * entre o backend e os clientes conectados.
 */
@ServerEndpoint(value = "/ws")
public class WebSocketServer {

    /** Sessão WebSocket do cliente atual. */
    private Session session;
    /** Lista de conexões ativas. */
    private static final Set<WebSocketServer> connections = new CopyOnWriteArraySet<>();
    /** Instância do servidor WebSocket. */
    private static Server server;

    /**
     * Inicia o servidor WebSocket na porta 8080.
     */
    public static void startServer() {
        server = new Server("localhost", 8080, "/Processor", null, WebSocketServer.class);

        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Encerra o servidor WebSocket.
     */
    public static void stopServer() {
        server.stop();
    }

    /**
     * Evento disparado quando um cliente abre uma conexão.
     *
     * @param session Sessão WebSocket associada ao cliente.
     */
    @OnOpen
    public void onOpen(Session session) {
        this.session = session;
        connections.add(this);
    }

    /**
     * Evento disparado quando uma mensagem é recebida de um cliente.
     *
     * @param message Mensagem recebida.
     */
    @OnMessage
    public void onMessage(String message) {
        broadcast(message);
    }

    /**
     * Evento disparado quando a conexão de um cliente é encerrada.
     *
     * @param session Sessão WebSocket encerrada.
     */
    @OnClose
    public void onClose(Session session) {
        connections.remove(this);
    }

    /**
     * Envia uma mensagem para todos os clientes conectados.
     *
     * @param message Mensagem a ser transmitida.
     */
    public static void broadcast(String message) {
        for (WebSocketServer client : connections) {
            try {
                synchronized (client) {
                    client.session.getBasicRemote().sendText(message);
                }
            } catch (IOException e) {
                connections.remove(client);
                try {
                    client.session.close();
                } catch (IOException ex) {
                    // Ignorado
                }
            }
        }
    }
}
