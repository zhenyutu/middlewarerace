package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 主角：BytesMessage接口的默认实现。
 * BytesMessage包含了消息本身的内容，以及消息的附加属性信息。
 * 其中包括三个成员：
 *     1. headers：KeyValue类型，key指的是topic或者queue，value指的是topic或者queue的名字。
 *     2. properties：
 *     3. body：原始数据。
 *
 */
public class DefaultBytesMessage implements BytesMessage, Serializable {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }
    @Override public byte[] getBody() {
        return body;
    }

    @Override public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override public KeyValue headers() {
        return headers;
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public int hashCode() {
        return new String(body).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return Arrays.equals(this.body,((DefaultBytesMessage)obj).getBody());
    }

    @Override
    public String toString() {
        return headers.toString();
    }
}
