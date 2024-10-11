package br.com.mangarosa.MyImpl;

import br.com.mangarosa.interfaces.Producer;
import br.com.mangarosa.interfaces.Topic;
import br.com.mangarosa.messages.Message;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.StreamEntryID;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementação de um produtor de mensagens que envia mensagens para tópicos específicos.
 * Utiliza Redis como backend para armazenamento de mensagens.
 */
public class TopicMessageProducer implements Producer, AutoCloseable {

    private final JedisPool jedisPool; // Pool de conexões Redis
    private final Set<Topic> topics; // Conjunto de tópicos gerenciados por este produtor

    /**
     * Construtor que inicializa o pool de conexões Redis e o conjunto de tópicos.
     */
    public TopicMessageProducer() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.jedisPool = new JedisPool(poolConfig, "localhost", 6379);
        this.topics = ConcurrentHashMap.newKeySet(); // Cria um set thread-safe
    }

    /**
     * Adiciona um novo tópico ao conjunto de tópicos gerenciados.
     * @param topic Tópico a ser adicionado
     */
    @Override
    public void addTopic(Topic topic) {
        topics.add(topic);
        System.out.println("Tópico adicionado: " + topic.name());
    }

    /**
     * Remove um tópico do conjunto de tópicos gerenciados.
     * @param topic Tópico a ser removido
     */
    @Override
    public void removeTopic(Topic topic) {
        topics.remove(topic);
        System.out.println("Tópico removido: " + topic.name());
    }

    /**
     * Envia uma mensagem para todos os tópicos gerenciados.
     * @param message Conteúdo da mensagem a ser enviada
     */
    @Override
    public void sendMessage(String message) {
        for (Topic topic : topics) {
            sendMessageToTopic(message, topic.name());
        }
    }

    /**
     * Envia uma mensagem para um tópico específico no Redis.
     * @param message Conteúdo da mensagem
     * @param topic Nome do tópico
     */
    public void sendMessageToTopic(String message, String topic) {
        // Usa try-with-resources para garantir que a conexão Jedis seja fechada automaticamente
        try (Jedis jedis = jedisPool.getResource()) {
            // Cria um mapa para armazenar os dados da mensagem
            // No Redis Streams, as mensagens são armazenadas como pares chave-valor
            Map<String, String> messageData = new HashMap<>();
            // Adiciona a mensagem ao mapa com a chave "message"
            messageData.put("message", message);

            // Usa o comando XADD do Redis para adicionar a mensagem ao stream (tópico)
            // StreamEntryID.NEW_ENTRY gera um novo ID para a entrada
            // O Redis irá gerar um ID único para esta mensagem
            StreamEntryID id = jedis.xadd(topic, StreamEntryID.NEW_ENTRY, messageData);

            // Imprime uma mensagem de log confirmando o envio da mensagem
            System.out.println("Mensagem enviada para o tópico " + topic + ": " + message + " com ID: " + id);
        } catch (Exception e) {
            // Captura qualquer exceção que possa ocorrer durante o processo
            // Por exemplo, problemas de conexão com o Redis ou erros de execução do comando
            // Imprime uma mensagem de erro no fluxo de erro padrão
            System.err.println("Erro ao enviar mensagem para o tópico " + topic + ": " + e.getMessage());
        }
    }

    /**
     * Fecha o pool de conexões Redis.
     */
    @Override
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            System.out.println("Conexão com o Redis fechada.");
        }
    }

    /**
     * Retorna o nome do produtor.
     * @return Nome do produtor
     */
    @Override
    public String name() {
        return "TopicMessageProducer";
    }

    /**
     * Cria e envia uma nova mensagem para um tópico específico.
     * @param content O conteúdo da mensagem a ser enviada.
     * @param topic O nome do tópico para o qual a mensagem será enviada.
     * @return O objeto Message criado e enviado.
     */
    public Message produce(String content, String topic) {
        // Cria um novo objeto Message
        // 'this' refere-se à instância atual do TopicMessageProducer
        // que está sendo passada como o produtor desta mensagem
        Message message = new Message(this, content);

        // Envia a mensagem para o tópico especificado
        // Usa o método getMessage() para obter o conteúdo da mensagem
        sendMessageToTopic(message.getMessage(), topic);

        // Retorna o objeto Message criado
        // Isso permite que o chamador tenha acesso à mensagem após o envio
        return message;
    }
}