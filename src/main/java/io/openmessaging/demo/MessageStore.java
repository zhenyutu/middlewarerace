package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Message操作的类.
 * 单例模式，操作过程中就这一个MessageStore。
 * 实现了两个功能：
 *     1. 将一条消息发送出去（放到内存或者文件）
 *     2. 将一条消息获取出来（从内存或者文件里拿到数据）
 */
public class MessageStore {

    private static final int SIZE = 1000;

    private static final MessageStore INSTANCE = new MessageStore();


    // TODO：是否加入线程安全保障
    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, MappedByteBuffer> messagePutBuckets = new HashMap<>();

    private Map<String, ArrayList<Message>> messagePullBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private String filePath;

    private static final int STORE_SIZE = 50000;
//    ByteBuffer byteBufferMessage = ByteBuffer.allocate(STORE_SIZE);

    private Map<String, Integer> bucketCountsMap = new HashMap<>();

    public void setFilePath(String filePath){
        this.filePath = filePath;
    }

    private void saveMessageToBuffer(DefaultBytesMessage message,ByteBuffer byteBufferMessage){
        StringBuffer str = new StringBuffer();
        for (String header : message.headers().keySet()){
            str.append(header+" ");
            str.append(message.headers().getString(header)+" ");
        }
        str.append(",");
        if (message.properties()!=null){
            for (String properties : message.properties().keySet()){
                str.append(properties+" ");
                str.append(message.properties().getString(properties)+" ");
            }
        }

        byte[] headerProperties = str.toString().getBytes();
        byteBufferMessage.putInt(headerProperties.length);
        byteBufferMessage.put(headerProperties);
        byteBufferMessage.putInt(message.getBody().length);
        byteBufferMessage.put(message.getBody());
    }

    private MappedByteBuffer getMappedFile(String bucket){
        MappedByteBuffer mappedByteBuffer = null;
        int count = bucketCountsMap.getOrDefault(bucket,0)/SIZE;
        String filename = filePath + bucket + count + ".txt";
        try {
             mappedByteBuffer = new RandomAccessFile(filename, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, STORE_SIZE);
        }catch (IOException e){
            e.printStackTrace();
        }

        return mappedByteBuffer;
    }

    public synchronized void putMessage(String bucket, Message message) {
        if (!messagePutBuckets.containsKey(bucket)) {
            messagePutBuckets.put(bucket, getMappedFile(bucket));
        }
        int count = bucketCountsMap.getOrDefault(bucket, 0);
        if (count%SIZE==0){
            messagePutBuckets.put(bucket,getMappedFile(bucket));
        }
        MappedByteBuffer bucketBuffer = messagePutBuckets.get(bucket);
        saveMessageToBuffer((DefaultBytesMessage)message,bucketBuffer);
        bucketCountsMap.put(bucket, ++count);
    }

    private synchronized ArrayList<Message> pullMessageFromFile(String bucket,long offset){
        ArrayList<Message> bucketList = new ArrayList<>();
        int flag = (int)offset/SIZE;
        File file = new File(filePath+"/"+bucket+flag+".txt");
        if (!file.exists())
            return null;
        try {
            FileInputStream in = new FileInputStream(file);
            FileChannel fc = in.getChannel();
            MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            while (true){
                byte[] headerProperties = new byte[buffer.getInt()];
                buffer.get(headerProperties);
                if (headerProperties.length==0)
                    break;
                byte[] body = new byte[buffer.getInt()];
                buffer.get(body);
                DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
                String[] str = new String(headerProperties).split(",");
                String[] header = str[0].split(" ");
                for (int j=0;j<header.length;j=j+2){
                    defaultBytesMessage.putHeaders(header[j].split(" ")[0],header[j+1].split(" ")[0]);
                }
                if (str.length>1){
                    String[] properties = str[1].split(" ");
                    for (int j=0;j<header.length;j=j+2){
                        defaultBytesMessage.putProperties(properties[j].split(" ")[0],properties[j+1].split(" ")[0]);
                    }
                }
                bucketList.add(defaultBytesMessage);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return bucketList;
    }

    public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList;
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset%SIZE==0) {
            bucketList = pullMessageFromFile(bucket,offset);
            messagePullBuckets.put(bucket,bucketList);
        }else {
            bucketList = messagePullBuckets.get(bucket);
        }
        Message message = null;
        if (bucketList!=null&&(offset%SIZE<bucketList.size())){
            message= bucketList.get(offset%SIZE);
            offsetMap.put(bucket, ++offset);
        }
        return message;
    }
}
