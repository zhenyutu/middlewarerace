package io.openmessaging.demo;

import java.io.FileInputStream;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by tuzhenyu on 17-5-20.
 * @author tuzhenyu
 */
public class Main {
    public static void main(String[] args) {
        try {
            FileInputStream in = new FileInputStream("/home/tuzhenyu/tmp/race2/QUEUE10.txt");
            FileChannel fc = in.getChannel();
            MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());

            while (buffer.position()<buffer.capacity()){
                byte[] headerProperties = new byte[buffer.getInt()];
                buffer.get(headerProperties);
                System.out.println(new String(headerProperties));
                byte[] body = new byte[buffer.getInt()];
                buffer.get(body);
                System.out.println(new String(body));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
