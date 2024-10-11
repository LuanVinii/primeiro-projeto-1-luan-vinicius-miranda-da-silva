package br.com.mangarosa.MyImpl;

import br.com.mangarosa.interfaces.MessageRepository;
import br.com.mangarosa.interfaces.Producer;
import br.com.mangarosa.interfaces.Topic;
import br.com.mangarosa.messages.Message;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.*;

/**
 * Implementação do MessageRepository usando Redis como armazenamento.
 * Esta classe gerencia o armazenamento e recuperação de mensagens em tópicos usando Redis Streams.
 * Garante que as mensagens expirem exatamente após 5 minutos.
 */
public class RedisMessageRepository implements MessageRepository {

    /**
     * Pool de conexões Jedis para interagir com o Redis.
     */
    private final JedisPool jedisPool;

    /**
     * Produtor padrão usado para criar mensagens quando nenhum produtor específico é fornecido.
     */
    private final Producer defaultProducer;

    /**
     * Tempo de expiração das mensagens em segundos (5 minutos).
     */
    private static final int EXPIRATION_TIME_SECONDS = 300;

    /**
     * Construtor da classe RedisMessageRepository.
     * Inicializa o pool de conexões Jedis e o produtor padrão.
     */
    public RedisMessageRepository() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        this.jedisPool = new JedisPool(poolConfig, "localhost", 6379);
        this.defaultProducer = new DefaultProducer();
    }

    /**
     * Adiciona uma nova mensagem a um tópico específico no Redis.
     * @param topic Nome do tópico.
     * @param message Mensagem a ser adicionada.
     */
    @Override
    public void append(String topic, Message message) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Prepara os dados da mensagem
            Map<String, String> messageData = new HashMap<>();
            messageData.put("message", message.getMessage());
            
            // Adiciona a mensagem ao stream do Redis
            StreamEntryID id = jedis.xadd(topic, StreamEntryID.NEW_ENTRY, messageData);
            
            // Define a expiração da mensagem
            String messageKey = topic + ":" + id.toString();
            jedis.expire(messageKey, EXPIRATION_TIME_SECONDS);
            
            System.out.println("Mensagem adicionada ao tópico " + topic + " com ID: " + id + ". Expira em 5 minutos.");
        }
    }

    /**
     * Consome mensagens de um tópico específico, movendo as mensagens expiradas para um tópico de consumidas.
     * @param topic Nome do tópico.
     * @param messageId ID da mensagem (não utilizado nesta implementação).
     */
    @Override
    public void consumeMessage(String topic, UUID messageId) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Obtém todas as entradas do stream
            List<StreamEntry> entries = jedis.xrange(topic, "-", "+");
            
            // Verifica cada entrada para consumo
            for (StreamEntry entry : entries) {
                String messageKey = topic + ":" + entry.getID().toString();
                if (!jedis.exists(messageKey)) {
                    // Se a chave não existe, a mensagem expirou e deve ser movida
                    moveToConsumedTopic(jedis, topic, entry);
                }
            }
        }
    }

    /**
     * Move uma mensagem expirada para o tópico de mensagens consumidas.
     * @param jedis Conexão Jedis.
     * @param sourceTopic Tópico de origem.
     * @param entry Entrada do stream a ser movida.
     */
    private void moveToConsumedTopic(Jedis jedis, String sourceTopic, StreamEntry entry) {
        String consumedTopic = sourceTopic + ":consumed";
        jedis.xadd(consumedTopic, entry.getID(), entry.getFields());
        jedis.xdel(sourceTopic, entry.getID());
        System.out.println("Mensagem expirada movida para " + consumedTopic + ": " + entry.getID());
    }

    /**
     * Retorna todas as mensagens não consumidas de um tópico específico.
     * @param topic Nome do tópico.
     * @return Lista de mensagens não consumidas.
     */
    @Override
    public List<Message> getAllNotConsumedMessagesByTopic(String topic) {
        return getMessagesByTopic(topic, false);
    }

    /**
     * Retorna todas as mensagens consumidas de um tópico específico.
     * @param topic Nome do tópico.
     * @return Lista de mensagens consumidas.
     */
    @Override
    public List<Message> getAllConsumedMessagesByTopic(String topic) {
        return getMessagesByTopic(topic, true);
    }

    /**
     * Recupera mensagens de um tópico específico, com base no estado de consumo.
     * Este método é usado internamente para obter tanto mensagens consumidas quanto não consumidas.
     *
     * @param topic O nome do tópico do qual as mensagens serão recuperadas.
     * @param consumed Um booleano indicando se devem ser recuperadas mensagens consumidas (true) ou não consumidas (false).
     * @return Uma lista de objetos Message contendo as mensagens recuperadas.
     */
    private List<Message> getMessagesByTopic(String topic, boolean consumed) {
        // Usa try-with-resources para garantir que a conexão Jedis seja fechada adequadamente após o uso
        try (Jedis jedis = jedisPool.getResource()) {
            // Determina o nome real do tópico com base no estado de consumo
            // Para mensagens consumidas, adiciona o sufixo ":consumed" ao nome do tópico
            String actualTopic = consumed ? topic + ":consumed" : topic;
            
            // Recupera todas as entradas do stream do Redis para o tópico especificado
            // "-" e "+" são usados como IDs mínimo e máximo para obter todas as entradas
            List<StreamEntry> entries = jedis.xrange(actualTopic, "-", "+");
            
            // Inicializa uma lista para armazenar as mensagens recuperadas
            List<Message> messages = new ArrayList<>();
            
            // Itera sobre cada entrada do stream
            for (StreamEntry entry : entries) {
                // Constrói a chave da mensagem combinando o tópico e o ID da entrada
                String messageKey = actualTopic + ":" + entry.getID().toString();
                
                // Verifica se a mensagem deve ser incluída na lista de retorno
                // Para mensagens consumidas, sempre inclui
                // Para mensagens não consumidas, verifica se a chave ainda existe (não expirou)
                if (consumed || jedis.exists(messageKey)) {
                    // Extrai o conteúdo da mensagem do campo "message" da entrada
                    String content = entry.getFields().get("message");
                    
                    // Cria um novo objeto Message com o conteúdo extraído e o adiciona à lista
                    messages.add(new Message(defaultProducer, content));
                }
            }
            
            // Retorna a lista de mensagens recuperadas
            return messages;
        }
    }

    /**
     * Fecha o pool de conexões Jedis.
     */
    @Override
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    /**
     * Limpa todos os dados do Redis.
     */
    public void clearAllData() {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.flushAll();
        }
    }

    /**
     * Adiciona uma nova mensagem a um tópico usando o produtor padrão.
     * @param topic Nome do tópico.
     * @param messageContent Conteúdo da mensagem.
     */
    public void appendWithDefaultProducer(String topic, String messageContent) {
        Message message = new Message(defaultProducer, messageContent);
        append(topic, message);
    }

    /**
     * Classe interna que implementa um produtor padrão.
     */
    private static class DefaultProducer implements Producer {
        @Override
        public void addTopic(Topic topic) {}
        @Override
        public void removeTopic(Topic topic) {}
        @Override
        public void sendMessage(String message) {}
        @Override
        public String name() {
            return "DefaultProducer";
        }
    }
}