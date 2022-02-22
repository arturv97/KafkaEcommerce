package org.Artur.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatcher = new KafkaDispatcher()) {
            // número de partições >= número de consumidores dentro de um grupo
            // a key é responsável pelo direcionamento das mensagens para as partições - se for constante as mensagens irão para uma partição fixa
            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();

                var value = "123,456,789"; // value  (ID do pedido, usuario e o valor da compra)
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing your order!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email); // topic, chave e valor
            }
        }
    }


}
