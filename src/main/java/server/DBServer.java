package server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import store.Store;

public class DBServer {
    private Server server;
    private Store store;

    public DBServer(Store store) {
        this.store = store;
    }

    public void start() throws Exception {
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(8090);
        server.setConnectors(new Connector[] { connector });

        ServletHandler servletHandler = new ServletHandler();
        server.setHandler(servletHandler);

        DBServlet dbServlet = new DBServlet(store);

        servletHandler.addServletWithMapping(new ServletHolder(dbServlet), "/");

        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }
}
