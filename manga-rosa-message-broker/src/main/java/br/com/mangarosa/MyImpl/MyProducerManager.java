package br.com.mangarosa.MyImpl;

import br.com.mangarosa.messages.Message;

/**
 * Gerencia a produção de mensagens para diferentes tópicos.
 * Esta classe utiliza um único produtor de mensagens compartilhado
 * para todos os tipos de produtores, economizando recursos do sistema.
 */
public class MyProducerManager {

     /**
     * Instância única do produtor de mensagens.
     * 
     * Explicação do padrão utilizado:
     * Este código usa uma abordagem chamada "Singleton", que é uma técnica
     * de programação onde garantimos que existe apenas uma instância de uma classe
     * em toda a aplicação. Neste caso, criamos apenas um TopicMessageProducer
     * que é compartilhado por todos os produtores específicos.
     */
    private static final TopicMessageProducer producer = new TopicMessageProducer();

    /**
     * Produtor de mensagens para entregas de comida.
     */
    public static class FoodDeliveryProducer {
        /**
         * Produz uma mensagem para o tópico de entregas rápidas.
         * @param content O conteúdo da mensagem a ser produzida.
         * @return A mensagem produzida.
         */
        public Message produce(String content) {
            // Utiliza o método estático do produtor, evitando a necessidade de instanciar um novo produtor.
            return producer.produce(content, "queue/fast-delivery-items");
        }
    }

    /**
     * Produtor de mensagens para entregas por pessoa física.
     */
    public static class PhysicPersonDeliveryProducer {
        /**
         * Produz uma mensagem para o tópico de entregas rápidas.
         * @param content O conteúdo da mensagem a ser produzida.
         * @return A mensagem produzida.
         */
        public Message produce(String content) {
            return producer.produce(content, "queue/fast-delivery-items");
        }
    }

    /**
     * Produtor de mensagens para o mercado de produtos.
     */
    public static class PyMarketPlaceProducer {
        /**
         * Produz uma mensagem para o tópico de longas distâncias.
         * @param content O conteúdo da mensagem a ser produzida.
         * @return A mensagem produzida.
         */
        public Message produce(String content) {
            return producer.produce(content, "queue/long-distance-items");
        }
    }

    /**
     * Produtor de mensagens para entregas rápidas.
     */
    public static class FastDeliveryProducer {
        /**
         * Produz uma mensagem para o tópico de longas distâncias.
         * @param content O conteúdo da mensagem a ser produzida.
         * @return A mensagem produzida.
         */
        public Message produce(String content) {
            return producer.produce(content, "queue/long-distance-items");
        }
    }
}