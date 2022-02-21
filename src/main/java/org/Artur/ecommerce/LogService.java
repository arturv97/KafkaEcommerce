package org.Artur.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        // Consome/escuta vários tópicos - Qualquer tópicos que começa com ECOMMERCE | .* -> Expressão regular
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
        // Deixa o serviço rodando/escutando all time
        while (true) {
            // Verifica se existe mensagem no tópico
            var records = consumer.poll(Duration.ofMillis(100));
            // Se a mensagem for encontrada
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    System.out.println("------------------------------------------");
                    System.out.println("Log: " + record.topic());
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignoring
                        e.printStackTrace();
                    }
                    System.out.println("Email sent");
                }
                continue;
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // É necessário criar um grupo (FraudDetectorService)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getName());
        return properties;
    }
}
