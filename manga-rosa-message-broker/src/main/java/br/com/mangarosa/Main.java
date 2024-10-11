package br.com.mangarosa;

import br.com.mangarosa.MyImpl.*;
import br.com.mangarosa.interfaces.Consumer;
import br.com.mangarosa.interfaces.Topic;
import br.com.mangarosa.messages.Message;
import br.com.mangarosa.messages.MessageBroker;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Classe principal que demonstra o funcionamento do sistema de mensagens.
 * Este sistema simula um broker de mensagens com tópicos para entregas rápidas
 * e de longa distância,
 * garantindo que as mensagens sejam consumidas exatamente após 5 minutos de sua
 * criação.
 */

// Implementação feita por Luan Vinicius.
public class Main {
    /**
     * Tempo de expiração das mensagens em segundos (5 minutos).
     * Este valor é usado para definir quanto tempo as mensagens devem permanecer no
     * sistema antes de serem consumidas.
     */
    private static final int EXPIRATION_TIME_SECONDS = 300;

    /**
     * Método principal que executa a demonstração do sistema de mensagens.
     * Este método orquestra todo o fluxo de demonstração, desde a configuração
     * inicial até a verificação final das mensagens.
     * 
     * @param args Argumentos da linha de comando (não utilizados neste exemplo).
     */
    public static void main(String[] args) {
        // Utiliza try-with-resources para garantir que os recursos sejam fechados
        // adequadamente
        try (RedisMessageRepository repository = new RedisMessageRepository();
                TopicMessageProducer producer = new TopicMessageProducer()) {

            // Limpa todos os dados existentes no repositório Redis
            repository.clearAllData();

            // Cria uma instância do MessageBroker, que gerenciará os tópicos e mensagens
            MessageBroker messageBroker = new MessageBroker(repository);

            // Configura os tópicos e consumidores no sistema
            setupTopicsAndConsumers(messageBroker, repository);

            // Produz e envia mensagens de teste para os tópicos
            System.out.println("Produzindo e enviando mensagens...");
            produceAndSendMessages(repository);

            // Verifica as mensagens imediatamente após o envio
            checkNotConsumedMessages(repository);

            // Aguarda o tempo de expiração (5 minutos) antes de consumir as mensagens
            Thread.sleep(TimeUnit.SECONDS.toMillis(EXPIRATION_TIME_SECONDS));

            // Tenta consumir as mensagens após o período de expiração
            consumeMessages(repository);

            // Verifica e exibe as mensagens que foram consumidas após a expiração
            checkMessagesAfterExpiration(repository);

            // Verifica se ainda existem mensagens não consumidas
            checkNotConsumedMessages(repository);

        } catch (Exception e) {
            // Captura e exibe qualquer exceção que ocorra durante a execução
            System.err.println("Erro durante a execução: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Configura os tópicos e consumidores no message broker.
     * Este método cria tópicos para diferentes tipos de entrega e associa
     * consumidores a esses tópicos.
     * 
     * @param messageBroker O broker de mensagens que gerenciará os tópicos e
     *                      consumidores.
     * @param repository    O repositório Redis usado para armazenar as mensagens.
     */
    private static void setupTopicsAndConsumers(MessageBroker messageBroker, RedisMessageRepository repository) {
        // Cria tópicos para entregas rápidas e de longa distância
        Topic fastDeliveryTopic = new TopicImpl("queue/fast-delivery-items", repository);
        Topic longDistanceTopic = new TopicImpl("queue/long-distance-items", repository);

        // Adiciona os tópicos ao message broker
        messageBroker.createTopic(fastDeliveryTopic);
        messageBroker.createTopic(longDistanceTopic);

        // Cria consumidores específicos para cada tipo de entrega
        Consumer fastDeliveryConsumer = new MyConsumer.FastDeliveryConsumer();
        Consumer longDistanceConsumer = new MyConsumer.LongDistanceConsumer();

        // Associa os consumidores aos respectivos tópicos no message broker
        messageBroker.subscribe("queue/fast-delivery-items", fastDeliveryConsumer);
        messageBroker.subscribe("queue/long-distance-items", longDistanceConsumer);
    }

    /**
     * Produz e envia mensagens para os tópicos.
     * Este método simula a criação de diferentes tipos de pedidos e os envia para
     * os tópicos apropriados.
     * 
     * @param repository O repositório Redis usado para armazenar as mensagens.
     */
    private static void produceAndSendMessages(RedisMessageRepository repository) {
        // Arrays para armazenar as mensagens e seus respectivos tópicos
        String[] messages = {
                "Pedido de hamburguer duplo com batatas e refrigerante",
                "Entrega de cartao de visita e brindes para evento corporativo",
                "Item: Laptop Gamer, inclui garantia estendida",
                "Reposicao de estoque para loja de conveniencia, entrega dentro de 45 minutos"
        };

        String[] topics = {
                "queue/fast-delivery-items",
                "queue/fast-delivery-items",
                "queue/long-distance-items",
                "queue/long-distance-items"
        };

        // Itera sobre as mensagens, enviando cada uma para seu respectivo tópico
        for (int i = 0; i < messages.length; i++) {
            repository.appendWithDefaultProducer(topics[i], messages[i]);
            System.out.println("Mensagem enviada para " + topics[i] + ": " + messages[i]);
        }
    }

    /**
     * Consome as mensagens de todos os tópicos.
     * Este método tenta consumir todas as mensagens que estão prontas para serem
     * consumidas em cada tópico.
     * 
     * @param repository O repositório Redis usado para armazenar as mensagens.
     */
    private static void consumeMessages(RedisMessageRepository repository) {
        // Array com os nomes dos tópicos
        String[] topics = { "queue/fast-delivery-items", "queue/long-distance-items" };

        // Itera sobre os tópicos, tentando consumir as mensagens de cada um
        for (String topic : topics) {
            repository.consumeMessage(topic, null);
        }
    }

    /**
     * Verifica e exibe as mensagens consumidas após o período de expiração.
     * Este método recupera e exibe todas as mensagens que foram consumidas de cada
     * tópico.
     * 
     * @param repository O repositório Redis usado para armazenar as mensagens.
     */
    private static void checkMessagesAfterExpiration(RedisMessageRepository repository) {
        // Array com os nomes dos tópicos
        String[] topics = { "queue/fast-delivery-items", "queue/long-distance-items" };

        // Itera sobre os tópicos, verificando as mensagens consumidas de cada um
        for (String topic : topics) {
            List<Message> consumedMessages = repository.getAllConsumedMessagesByTopic(topic);
            System.out.println("Mensagens consumidas do tópico " + topic + ":");
            if (!consumedMessages.isEmpty()) {
                // Exibe cada mensagem consumida
                for (Message msg : consumedMessages) {
                    System.out.println(" - " + msg.getMessage());
                }
            }
        }
    }

    /**
     * Verifica e exibe as mensagens não consumidas.
     * Este método recupera e exibe todas as mensagens que ainda não foram
     * consumidas de cada tópico.
     * 
     * @param repository O repositório Redis usado para armazenar as mensagens.
     */
    private static void checkNotConsumedMessages(RedisMessageRepository repository) {
        // Array com os nomes dos tópicos
        String[] topics = { "queue/fast-delivery-items", "queue/long-distance-items" };

        // Itera sobre os tópicos, verificando as mensagens não consumidas de cada um
        for (String topic : topics) {
            List<Message> notConsumedMessages = repository.getAllNotConsumedMessagesByTopic(topic);
            if (!notConsumedMessages.isEmpty()) {
                // Exibe cada mensagem não consumida
                for (Message msg : notConsumedMessages) {
                    System.out.println(" - " + msg.getMessage());
                }
            }
        }
    }
}