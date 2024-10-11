package br.com.mangarosa.MyImpl;

import br.com.mangarosa.interfaces.Consumer;
import br.com.mangarosa.interfaces.MessageRepository;
import br.com.mangarosa.interfaces.Topic;
import br.com.mangarosa.messages.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementação da interface Topic, responsável por gerenciar um tópico específico,
 * incluindo a adição de mensagens e o gerenciamento de consumidores.
 */
public class TopicImpl implements Topic {

    private String name; // Nome do tópico
    private List<Consumer> consumers; // Lista de consumidores inscritos no tópico
    private MessageRepository repository; // Repositório de mensagens associado ao tópico

    /**
     * Construtor que inicializa o tópico com um nome e um repositório de mensagens.
     * @param name Nome do tópico
     * @param repository Repositório de mensagens a ser utilizado
     */
    public TopicImpl(String name, MessageRepository repository) {
        this.name = name;
        this.consumers = new ArrayList<>(); // Inicializa a lista de consumidores
        this.repository = repository;
    }

    /**
     * Retorna o nome do tópico.
     * @return Nome do tópico
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Adiciona uma nova mensagem ao tópico e notifica os consumidores.
     * @param message Mensagem a ser adicionada
     */
    @Override
    public void addMessage(Message message) {
        repository.append(name, message); // Adiciona a mensagem ao repositório
        notifyConsumers(message); // Notifica os consumidores sobre a nova mensagem
    }

    /**
     * Inscreve um novo consumidor no tópico.
     * @param consumer Consumidor a ser inscrito
     */
    @Override
    public void subscribe(Consumer consumer) {
        if (!consumers.contains(consumer)) {
            consumers.add(consumer); // Adiciona o consumidor se ele ainda não estiver inscrito
        }
    }

    /**
     * Remove a inscrição de um consumidor do tópico.
     * @param consumer Consumidor a ser removido
     */
    @Override
    public void unsubscribe(Consumer consumer) {
        consumers.remove(consumer); // Remove o consumidor da lista
    }

    /**
     * Retorna uma lista de todos os consumidores inscritos no tópico.
     * @return Lista de consumidores
     */
    @Override
    public List<Consumer> consumers() {
        return new ArrayList<>(consumers); // Retorna uma cópia da lista para evitar modificações externas
    }

    /**
     * Retorna o repositório de mensagens associado ao tópico.
     * @return Repositório de mensagens
     */
    @Override
    public MessageRepository getRepository() {
        return repository;
    }

    /**
     * Notifica todos os consumidores inscritos sobre uma nova mensagem.
     * @param message Mensagem a ser notificada aos consumidores
     */
    public void notifyConsumers(Message message) {
        for (Consumer consumer : consumers) {
            try {
                boolean consumed = consumer.consume(message);
                if (consumed) {
                    System.out.println("Mensagem consumida por " + consumer.name());
                } else {
                    System.out.println("Falha ao consumir mensagem por " + consumer.name());
                }
            } catch (Exception e) {
                System.out.println("Erro ao notificar consumidor " + consumer.name() + ": " + e.getMessage());
            }
        }
    }
}
