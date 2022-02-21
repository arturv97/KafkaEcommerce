package org.Artur.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "123,456,789"; // value  (ID do pedido, usuario e o valor da compra)
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value); // topic, chave e valor

        // Como send eh assincrono, foi usado get para aguardar a finalizacao
        producer.send(record, (data, ex) -> {
            if(ex !=null){
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando" + data.topic()+":::partition " + data.partition() + " / offset " + data.offset() + " / timestamp " + data.timestamp());
        }).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
