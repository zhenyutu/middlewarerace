package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private String filePath;

    private static final int STORE_SIZE = 20000;
    ByteBuffer byteBufferIndex = ByteBuffer.allocate(STORE_SIZE);
    ByteBuffer byteBufferBody = ByteBuffer.allocate(STORE_SIZE);

    public void setFilePath(String filePath){
        this.filePath = filePath;
    }

    private void saveMessageToBuffer(long offset,int size,byte[] body){
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferBody.put(body);
    }

    private void saveMessageToFile(String bucket,ArrayList<Message> bucketList){
        System.out.println("save message");
        for (int i=0;i<bucketList.size();i++){
            DefaultBytesMessage message = (DefaultBytesMessage) bucketList.get(i);
            byte[] body = message.getBody();
            saveMessageToBuffer(byteBufferBody.position(),body.length,body);
        }
        try {
            File f1 = new File(filePath+"/"+bucket+".index");
            FileChannel out1 = new FileOutputStream(f1,true).getChannel();
            byteBufferIndex.flip();
            out1.write(byteBufferIndex);
            byteBufferIndex.clear();
            out1.close();

            File f2 = new File(filePath+"/"+bucket+".message");
            FileChannel out2 = new FileOutputStream(f2,true).getChannel();
            byteBufferBody.flip();
            out2.write(byteBufferBody);
            byteBufferBody.clear();
            out2.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public synchronized void putMessage(String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        bucketList.add(message);
        if (bucketList.size()>=500){
            ArrayList<Message> oldMessageBucket = bucketList;
            messageBuckets.put(bucket,new ArrayList<>(1024));
            saveMessageToFile(bucket,oldMessageBucket);
        }
    }

    public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
    }
}
