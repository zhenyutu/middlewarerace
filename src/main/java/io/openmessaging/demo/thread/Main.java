package io.openmessaging.demo.thread;

import io.openmessaging.KeyValue;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.demo.DefaultProducer;
import io.openmessaging.demo.DefaultPullConsumer;

import java.util.Arrays;

/**
 * Created by tuzhenyu on 17-4-15.
 * @author tuzhenyu
 */
public class Main {
    public static void main(String[] args) {
        String string = "Queue QUEUE1 Queue QUEUE1 ,";
        String[] str = string.split(",");
        String[] header = str[0].split(" ");
        for (int i=0;i<header.length;i=i+2){
            System.out.println((header[i].split(" ")[0]+"-"+header[i+1].split(" ")[0]));
        }
        if (str.length>1){
            String[] properties = str[1].split(" ");
            for (int j=0;j<properties.length/2;j++){
                System.out.println(properties[j].split(" ")[0]+"-"+properties[j+1].split(" ")[0]);
            }
        }
    }
}
