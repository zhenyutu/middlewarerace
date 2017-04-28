package io.openmessaging.demo.thread;

import io.openmessaging.KeyValue;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.demo.DefaultProducer;
import io.openmessaging.demo.DefaultPullConsumer;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class Main {
    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "/home/tuzhenyu/tmp/test/");
        Producer producer = new DefaultProducer(properties);
        PullConsumer consumer1 = new DefaultPullConsumer(properties);
        MessageQueue messageQueue = new MessageQueue(producer,consumer1);

//        for (int i=0;i<12;i++){
//            ProduceThread produce = new ProduceThread(messageQueue,""+i);
//            produce.start();
//        }

        for (int i=0;i<2;i++){
            PullConsumer consumer = new DefaultPullConsumer(properties);
            consumer.attachQueue("queue"+i, null);
            ConsumeThread consume = new ConsumeThread(consumer,""+i);
            consume.start();
        }
    }
}
