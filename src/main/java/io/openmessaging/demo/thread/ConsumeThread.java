package io.openmessaging.demo.thread;

import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultPullConsumer;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class ConsumeThread extends Thread{
    private PullConsumer pullConsumer;
    public ConsumeThread(PullConsumer pullConsumer, String name){
        super(name);
        this.pullConsumer = pullConsumer;
    }
    @Override
    public void run(){
//        System.out.println("thread"+Thread.currentThread().getName()+" is consuming messages...");
//        messageQueue.consumeMessage();
        for (int i=0;i<1000;i++){
            DefaultBytesMessage message = (DefaultBytesMessage) pullConsumer.pullNoWait();
            System.out.println(Thread.currentThread().getName()+"-"+new String(message.getBody()));
        }
    }
}
