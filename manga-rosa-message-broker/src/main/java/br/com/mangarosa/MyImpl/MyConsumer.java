package br.com.mangarosa.MyImpl;

import br.com.mangarosa.interfaces.Consumer;
import br.com.mangarosa.messages.Message;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;

/**
 * Classe MyConsumer que implementa diferentes tipos de consumidores para um sistema de mensageria.
 * Esta classe utiliza o Redis como backend para armazenamento e recuperação de mensagens.
 */
public class MyConsumer {

    /**
     * Classe abstrata BaseConsumer que serve como base para todos os tipos de consumidores.
     * Implementa as interfaces Consumer e AutoCloseable para padronização e gerenciamento de recursos.
     */
    private static abstract class BaseConsumer implements Consumer, AutoCloseable {
        protected Jedis jedis;

        /**
         * Construtor que inicializa a conexão com o Redis.
         * Cria uma nova instância de Jedis para conexão com o Redis local na porta padrão.
         */
        public BaseConsumer() {
            // Estabelece uma conexão com o Redis local na porta 6379
            this.jedis = new Jedis("localhost", 6379);
            System.out.println(name() + ": Conexão com Redis estabelecida.");
        }

        /**
         * Método para fechar a conexão com o Redis.
         * Implementa a interface AutoCloseable para garantir que os recursos sejam liberados adequadamente.
         */
        @Override
        public void close() {
            if (jedis != null) {
                // Fecha a conexão com o Redis se estiver aberta
                jedis.close();
                System.out.println(name() + ": Conexão com Redis fechada.");
            }
        }

        /**
         * Método para consumir mensagens de um tópico específico.
         * Utiliza o comando XRANGE do Redis para recuperar mensagens não recebidas.
         * @param topicName Nome do tópico do qual as mensagens serão consumidas.
         */
        protected void consumeMessages(String topicName) {
            System.out.println(name() + ": Iniciando consumo de mensagens do tópico " + topicName);
            
            // Recupera todas as mensagens não recebidas do tópico especificado
            List<StreamEntry> messages = jedis.xrange(topicName, StreamEntryID.UNRECEIVED_ENTRY, StreamEntryID.UNRECEIVED_ENTRY);
            System.out.println(name() + ": " + messages.size() + " mensagens encontradas no tópico " + topicName);
            
            // Itera sobre cada mensagem recuperada
            for (StreamEntry message : messages) {
                String content = message.getFields().get("message");
                System.out.println(name() + ": Processando mensagem - ID: " + message.getID() + ", Conteúdo: " + content);
            }
            
            System.out.println(name() + ": Consumo de mensagens do tópico " + topicName + " concluído.");
        }
    }

    /**
     * Classe FastDeliveryConsumer para processar mensagens de entrega rápida.
     * Estende BaseConsumer e implementa a lógica específica para entregas rápidas.
     */
    public static class FastDeliveryConsumer extends BaseConsumer {
        @Override
        public boolean consume(Message message) {
            // Lógica de consumo para entregas rápidas
            System.out.println(name() + ": Consumindo mensagem - " + message.getMessage());
            return true; // Indica que a mensagem foi consumida com sucesso
        }

        @Override
        public String name() {
            return "FastDeliveryConsumer";
        }
    }

    /**
     * Classe LongDistanceConsumer para processar mensagens de longa distância.
     * Estende BaseConsumer e implementa a lógica específica para entregas de longa distância.
     */
    public static class LongDistanceConsumer extends BaseConsumer {
        @Override
        public boolean consume(Message message) {
            // Lógica de consumo para entregas de longa distância
            System.out.println(name() + ": Consumindo mensagem - " + message.getMessage());
            return true; // Indica que a mensagem foi consumida com sucesso
        }

        @Override
        public String name() {
            return "LongDistanceConsumer";
        }
    }

    /**
     * Método estático para subscrever e consumir mensagens de um tópico específico.
     * Este método cria uma nova conexão com o Redis, recupera as mensagens não recebidas
     * e as imprime no console.
     * @param topicName Nome do tópico para subscrever.
     */
    public static void subscribe(String topicName) {
        // Usa try-with-resources para garantir que a conexão Jedis seja fechada corretamente
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            // Recupera todas as mensagens não recebidas do tópico especificado
            List<StreamEntry> messages = jedis.xrange(topicName, StreamEntryID.UNRECEIVED_ENTRY, StreamEntryID.UNRECEIVED_ENTRY);

            // Itera sobre cada mensagem recuperada e a imprime
            for (StreamEntry message : messages) {
                System.out.println("Mensagem recebida - Tópico: " + topicName + 
                                   ", ID: " + message.getID() + 
                                   ", Conteúdo: " + message.getFields().get("message"));
            }
        }
    }
}