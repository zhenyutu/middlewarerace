package io.openmessaging.demo.thread;

import io.openmessaging.Producer;
import io.openmessaging.demo.DefaultProducer;

import java.io.IOException;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class MessageQueue {
    private Producer producer;
    public MessageQueue(Producer producer){
        this.producer = producer;
    }
    public void produceMessage()throws IOException{
        for (int i=0;i<1024;i++){
//            producer.send(producer.createBytesMessageToTopic("topic"+(Integer.parseInt(Thread.currentThread().getName())%4),
//                    ("thread"+Thread.currentThread().getName()+"-"+i).getBytes()));
//            producer.send(producer.createBytesMessageToTopic("queue"+(Integer.parseInt(Thread.currentThread().getName())%4),
//                    ("thread"+Thread.currentThread().getName()+"-"+i).getBytes()));

            producer.send(producer.createBytesMessageToTopic("topic"+(Integer.parseInt(Thread.currentThread().getName())),
                    ("thread"+Thread.currentThread().getName()+"-"+i).getBytes()));
            producer.send(producer.createBytesMessageToTopic("queue"+(Integer.parseInt(Thread.currentThread().getName())),
                    ("thread"+Thread.currentThread().getName()+"-"+i).getBytes()));
        }
    }
}
