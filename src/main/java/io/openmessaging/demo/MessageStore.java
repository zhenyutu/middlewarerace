package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
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

    private Map<String, Integer> bucketCountsMap = new HashMap<>();

    public void setFilePath(String filePath){
        this.filePath = filePath;
    }

    private void saveMessageToFile(String bucket,ArrayList<Message> bucketList) throws IOException {
        String filename = filePath + bucket + bucketCountsMap.get(bucket)/500 + ".message";
        FileOutputStream fileOutputStream = new FileOutputStream(filename);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        System.out.println("Saving to " + filename + "...");
        for (int i=0;i<bucketList.size();i++){
            DefaultBytesMessage message = (DefaultBytesMessage) bucketList.get(i);
            objectOutputStream.writeObject(message);
        }
    }

    public synchronized void putMessage(String bucket, Message message) throws IOException {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        bucketList.add(message);
        int count = bucketCountsMap.getOrDefault(bucket, 0);
        bucketCountsMap.put(bucket, ++count);
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
