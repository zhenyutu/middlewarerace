package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;


/**
 * 默认的消息工厂实现。
 * 提供了两个方法：createBytesMessageToTopic，createBytesMessageToQueue.
 * 这两个方法都返回BytesMessage。
 * 功能：
 * 输入Topic类型或者Queue类型，byte[]类型的原始数据。
 * 将上面两部分包装成一个ByteMessage并返回。
 */
public class DefaultMessageFactory implements MessageFactory {

    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.TOPIC, topic);
        return defaultBytesMessage;
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        defaultBytesMessage.putHeaders(MessageHeader.QUEUE, queue);
        return defaultBytesMessage;
    }
}
