package io.openmessaging.demo.thread;

import io.openmessaging.Message;
import io.openmessaging.demo.DefaultBytesMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

/**
 * Created by tuzhenyu on 17-4-28.
 * @author tuzhenyu
 */
public class ReadMessage {
    public static void main(String[] args) {
        ArrayList<Message> list = new ArrayList<>();
        File file = new File("/home/tuzhenyu/tmp/test/QUEUE_10.txt");
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            for (int i=0;i<1000;i++){
                list.add((Message) objectInputStream.readObject());
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        for (int i=0;i<list.size();i++){
            DefaultBytesMessage message = (DefaultBytesMessage) list.get(i);
            System.out.println(i+"-"+new String(message.getBody()));
        }
    }
}
