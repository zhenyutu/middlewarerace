package io.openmessaging.demo.thread;

import io.openmessaging.KeyValue;
import io.openmessaging.Producer;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.demo.DefaultProducer;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class Main {
    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/tuzhenyu/tmp/test/");
        Producer producer = new DefaultProducer(properties);
        MessageQueue messageQueue = new MessageQueue(producer);
        for (int i=0;i<12;i++){
            ProduceThread produce = new ProduceThread(messageQueue,""+i);
            produce.start();
        }
    }
}
