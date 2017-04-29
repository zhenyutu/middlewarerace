package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.IOException;

/**
 * 主角：Producer接口的默认实现。
 */
public class DefaultProducer  implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
    }


    /**
     * 对MessageFactory同样方法的二次封装。
     * @param topic the target topic to send
     * @param body the body data for a message
     * @return
     */
    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    /**
     * 对MessageFactory同样方法的二次封装。
     * @param queue the target queue to send
     * @param body the body data for a message
     * @return
     */
    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    /**
     * 开始Producer的方法。
     * 估计这里需要启动线程一类的。
     */
    @Override public void start() {

    }

    /**
     * 关闭Producer的方法。
     * 估计需要关闭线程。
     */
    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    /**
     * 发送一条消息。
     * 是对MessageStore.putMessage的进一步封装，是最高层的方法，供开发者使用。
     * 它的意思已经转换成了：将一条消息发送的消息队列里面，具体的细节已经不需要开发者观察。是文件系统持久化，还是db持久化。
     * @param message a message will be sent
     * @throws IOException
     */
    @Override public void send(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        messageStore.setFilePath(properties.getString("STORE_PATH"));
        messageStore.putMessage(topic != null ? topic : queue, message);
    }

    /**
     * 发送一条消息，带属性。
     * @param message a message will be sent
     * @param properties the specified properties
     */
    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    /**
     * 异步发送一条消息。
     * 返回一个Promise对象。
     * @param message a message will be sent
     * @return
     */
    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    /**
     * 异步发哦少年宫一条消息，带属性。
     * @param message a message will be sent
     * @param properties the specified properties
     * @return
     */
    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    /**
     * 发送一条消息，不在乎成功与否。
     * @param message a message will be sent
     */
    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    /**
     * 发送一条消息，带属性，不在乎成功与否。
     * @param message a message will be sent
     * @param properties the specified properties
     */
    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }
}
