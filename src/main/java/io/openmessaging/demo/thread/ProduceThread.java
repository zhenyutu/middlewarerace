package io.openmessaging.demo.thread;

import io.openmessaging.demo.DefaultProducer;

import java.io.IOException;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class ProduceThread extends Thread{
    private MessageQueue messageQueue;
    public ProduceThread(MessageQueue messageQueue,String name){
        super(name);
        this.messageQueue = messageQueue;
    }
    @Override
    public void run(){
        try {
            System.out.println("thread"+Thread.currentThread().getName()+" is producing messages...");
            messageQueue.produceMessage();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
