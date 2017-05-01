package io.openmessaging.demo.thread;

import io.openmessaging.Message;
import io.openmessaging.demo.DefaultBytesMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by tuzhenyu on 17-4-28.
 * @author tuzhenyu
 */
public class ReadMessage {
    public static void main(String[] args) {
        ArrayList<Message> list = new ArrayList<>();
        File file = new File("/home/tuzhenyu/tmp/race2/QUEUE11.txt");
//        try {
//            FileInputStream fileInputStream = new FileInputStream(file);
//            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
//            for (int i=0;i<1000;i++){
//                list.add((Message) objectInputStream.readObject());
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }

        try {
            FileInputStream in = new FileInputStream(file);
            FileChannel fc = in.getChannel();
            MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            while (true){
                byte[] headerProperties = new byte[buffer.getInt()];
                buffer.get(headerProperties);
                if (headerProperties.length==0)
                    break;
                System.out.println(new String(headerProperties));
                byte[] body = new byte[buffer.getInt()];
                buffer.get(body);
                System.out.println(new String(body));

            }
        }catch (IOException e){
            e.printStackTrace();
        }


        for (int i=0;i<list.size();i++){
            DefaultBytesMessage message = (DefaultBytesMessage) list.get(i);
            System.out.println(i+"-"+new String(message.getBody()));
        }
    }
}
