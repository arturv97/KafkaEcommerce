package org.Artur.ecommerce;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HttpEcommerceService {
    public static void main(String[] args) throws Exception {
        // criando um server com jetty
        var server = new Server(8080);

        // configurando servidor
        var context = new ServletContextHandler();
        context.setContextPath("/");
        // Servlet do novo pedido de compra
        context.addServlet(new ServletHolder(new NewOrderServlet()), "/new");

        // lidar com requisição através um contexto
        server.setHandler(context);

        // startar servidor
        server.start();
        // espera o servidor terminar para finalizar a aplicação
        server.join();
    }
}
