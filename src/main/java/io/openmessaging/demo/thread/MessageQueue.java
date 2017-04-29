package io.openmessaging.demo.thread;

import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultBytesMessage;

import java.io.IOException;
import java.util.Collections;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class MessageQueue {
    private Producer producer;
    private PullConsumer pullConsumer;
    public MessageQueue(Producer producer,PullConsumer pullConsumer){
        this.producer = producer;
        this.pullConsumer = pullConsumer;
    }
    public void produceMessage()throws IOException{
        for (int i=0;i<1024;i++){
            producer.send(producer.createBytesMessageToTopic("topic"+(Integer.parseInt(Thread.currentThread().getName())%4),
                    ("thread"+Thread.currentThread().getName()+"-"+i).getBytes()));
            producer.send(producer.createBytesMessageToTopic("queue"+(Integer.parseInt(Thread.currentThread().getName())%4),
                    ("thread"+Thread.currentThread().getName()+"-"+i).getBytes()));

        }
    }

    public void consumeMessage(){
        for (int i=0;i<1000;i++){
            DefaultBytesMessage message = (DefaultBytesMessage) pullConsumer.poll();
            System.out.println(Thread.currentThread().getName()+"-"+new String(message.getBody()));
        }
    }
}
