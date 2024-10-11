// Classe Message
package br.com.mangarosa.messages;

import br.com.mangarosa.interfaces.Consumer;
import br.com.mangarosa.interfaces.Producer;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;

public class Message implements Serializable {
    private String id;
    private Producer producer;
    private final LocalDateTime createdAt;
    private final List<MessageConsumption> consumptionList;
    private boolean isConsumed;
    private String message;

    public Message(Producer producer, String message) {
        validateProducer(producer);
        validateMessage(message);
        this.id = UUID.randomUUID().toString();
        this.producer = producer;
        this.message = message;
        this.createdAt = LocalDateTime.now();
        this.consumptionList = new ArrayList<>();
        this.isConsumed = false; // Inicializa como não consumida
    }

    public Message(Producer producer, Map<String, String> data) {
        validateProducer(producer);
        this.producer = producer;
        this.id = data.getOrDefault("id", UUID.randomUUID().toString());
        this.message = data.getOrDefault("message", "");
        this.createdAt = LocalDateTime.now();
        this.consumptionList = new ArrayList<>();
        this.isConsumed = false; // Inicializa como não consumida
    }

    // Getters
    public String getId() {
        return id;
    }

    public Producer getProducer() {
        return producer;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public List<MessageConsumption> getConsumptionList() {
        return consumptionList;
    }

    public boolean isConsumed() {
        return isConsumed;
    }

    public String getMessage() {
        return message;
    }

    // Setters
    public void setMessage(String message) {
        validateMessage(message);
        this.message = message;
    }

    public void setConsumed(boolean consumed) {
        isConsumed = consumed;
    }

    private void validateProducer(Producer producer) {
        if (producer == null) throw new IllegalArgumentException("The message's producer can't be null");
    }

    private void validateMessage(String message) {
        if (message == null || message.isBlank()) throw new IllegalArgumentException("The message content can't be null or empty or blank");
    }

    public void addConsumption(Consumer consumer) {
        if (consumer == null) throw new IllegalArgumentException("Consumer can't be null in a consumption");
        this.consumptionList.add(new MessageConsumption(consumer));
    }

    public Map<String, String> toMap() throws IllegalAccessException {
        Map<String, String> map = new HashMap<>();
        for (Field field : this.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(this);
            if (value != null) {
                map.put(field.getName(), value.toString());
            }
        }
        return map;
    }

    @Override
    public String toString() {
        return "Message{id='" + id + "', producer=" + producer + ", createdAt=" + createdAt + ", consumptionList=" + consumptionList + ", isConsumed=" + isConsumed + ", message='" + message + "'}";
    }
}
