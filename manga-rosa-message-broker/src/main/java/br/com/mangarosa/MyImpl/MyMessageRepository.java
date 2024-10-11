package br.com.mangarosa.MyImpl;

import br.com.mangarosa.interfaces.MessageRepository;
import br.com.mangarosa.messages.Message;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Implementação do repositório de mensagens que armazena mensagens em memória.
 * Esta classe gerencia mensagens para diferentes tópicos, permitindo adição,
 * consumo e recuperação de mensagens, além de lidar com a expiração das mesmas.
 */
public class MyMessageRepository implements MessageRepository, AutoCloseable {

    /**
     * Mapa que armazena as mensagens. A chave é o nome do tópico e o valor
     * é uma lista de MessageWrapper contendo as mensagens desse tópico.
     */
    private final Map<String, List<MessageWrapper>> messages;

    /**
     * Tempo de expiração das mensagens em milissegundos (5 minutos).
     * Após este período, as mensagens são consideradas expiradas.
     */
    private static final long EXPIRATION_TIME = 300000;

    /**
     * Construtor da classe.
     * Inicializa o mapa de mensagens vazio.
     */
    public MyMessageRepository() {
        this.messages = new HashMap<>();
    }

    /**
     * Adiciona uma nova mensagem a um tópico específico.
     * @param topic Nome do tópico.
     * @param message Mensagem a ser adicionada.
     */
    @Override
    public void append(String topic, Message message) {
        synchronized (messages) {
            // Verifica se o tópico já existe, se não, cria uma nova lista
            if (!messages.containsKey(topic)) {
                messages.put(topic, new ArrayList<>());
            }
            // Adiciona a nova mensagem encapsulada em um MessageWrapper
            messages.get(topic).add(new MessageWrapper(message));
        }
    }

    /**
     * Marca uma mensagem como consumida em um tópico específico.
     * @param topic Nome do tópico.
     * @param messageId ID da mensagem a ser marcada como consumida.
     */
    @Override
    public void consumeMessage(String topic, UUID messageId) {
        synchronized (messages) {
            if (messages.containsKey(topic)) {
                // Itera sobre as mensagens do tópico
                for (MessageWrapper wrapper : messages.get(topic)) {
                    if (wrapper.getMessage().getId().equals(messageId)) {
                        wrapper.setConsumed(true);
                        break; // Sai do loop após encontrar e marcar a mensagem
                    }
                }
            }
        }
        System.out.println("Mensagem consumida - Tópico: " + topic + ", ID: " + messageId);
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
     * Remove todas as mensagens expiradas do repositório.
     */
    public void removeExpiredMessages() {
        long currentTime = System.currentTimeMillis();
        synchronized (messages) {
            // Itera sobre todos os tópicos e suas mensagens
            for (Map.Entry<String, List<MessageWrapper>> entry : messages.entrySet()) {
                // Remove mensagens expiradas usando removeIf
                entry.getValue().removeIf(wrapper -> wrapper.isExpired(currentTime));
            }
        }
    }

    /**
     * Fecha o repositório, limpando todas as mensagens.
     */
    @Override
    public void close() {
        synchronized (messages) {
            messages.clear(); // Remove todas as mensagens de todos os tópicos
        }
        System.out.println("Repositório fechado e mensagens limpas.");
    }

    /**
     * Classe interna para encapsular uma mensagem com metadados adicionais.
     */
    private static class MessageWrapper {
        private final Message message;
        private final long creationTime;
        private boolean consumed;

        MessageWrapper(Message message) {
            this.message = message;
            this.creationTime = System.currentTimeMillis();
            this.consumed = false;
        }

        Message getMessage() {
            return message;
        }

        boolean isConsumed() {
            return consumed;
        }

        void setConsumed(boolean consumed) {
            this.consumed = consumed;
        }

        boolean isExpired(long currentTime) {
            return (currentTime - creationTime) > EXPIRATION_TIME;
        }
    }

    /**
     * Método privado para obter mensagens por tópico e estado de consumo.
     * @param topic Nome do tópico.
     * @param consumed Estado de consumo das mensagens a serem retornadas.
     * @return Lista de mensagens filtradas.
     */
    private List<Message> getMessagesByTopic(String topic, boolean consumed) {
        long currentTime = System.currentTimeMillis();
        synchronized (messages) {
            if (!messages.containsKey(topic)) {
                return new ArrayList<>(); // Retorna lista vazia se o tópico não existir
            }
            // Usa Stream API para filtrar e mapear as mensagens
            return messages.get(topic).stream()
                    .filter(wrapper -> wrapper.isConsumed() == consumed && !wrapper.isExpired(currentTime))
                    .map(MessageWrapper::getMessage)
                    .collect(Collectors.toList());
        }
    }
}