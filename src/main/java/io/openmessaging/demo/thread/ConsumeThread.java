package io.openmessaging.demo.thread;

import io.openmessaging.demo.DefaultPullConsumer;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class ConsumeThread extends Thread{
    private DefaultPullConsumer defaultPullConsumer;
    public ConsumeThread(DefaultPullConsumer defaultPullConsumer){
        this.defaultPullConsumer = defaultPullConsumer;
    }
    @Override
    public void run(){
        defaultPullConsumer.pullNoWait();
    }
}
