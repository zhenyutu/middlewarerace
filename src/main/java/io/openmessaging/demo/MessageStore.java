package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
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

    private Map<String, ArrayList<Message>> messagePutBuckets = new HashMap<>();

    private Map<String, ArrayList<Message>> messagePullBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private String filePath;

    private Map<String, Integer> bucketCountsMap = new HashMap<>();

    public void setFilePath(String filePath){
        this.filePath = filePath;
    }

    private synchronized void saveMessageToFile(String bucket,ArrayList<Message> bucketList){
        int count = bucketCountsMap.get(bucket)/SIZE-1;
        String filename = filePath + bucket + count + ".txt";
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filename);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            System.out.println("Saving to " + filename + "..."+System.currentTimeMillis());
            for (int i=0;i<bucketList.size();i++){
                DefaultBytesMessage message = (DefaultBytesMessage) bucketList.get(i);
                objectOutputStream.writeObject(message);
            }
            System.out.println("Saving to " + filename + "..."+System.currentTimeMillis());
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    public synchronized void putMessage(String bucket, Message message) {
        if (!messagePutBuckets.containsKey(bucket)) {
            messagePutBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messagePutBuckets.get(bucket);
        bucketList.add(message);
        int count = bucketCountsMap.getOrDefault(bucket, 0);
        bucketCountsMap.put(bucket, ++count);
        if (bucketList.size()>=SIZE){
            ArrayList<Message> oldMessageBucket = bucketList;
            messagePutBuckets.put(bucket,new ArrayList<>(1024));
            saveMessageToFile(bucket,oldMessageBucket);
        }
    }

    private synchronized ArrayList<Message> pullMessageFromFile(String bucket,long offset){
        ArrayList<Message> bucketList = new ArrayList<>();
        int flag = (int)offset/SIZE;
        File file = new File(filePath+"/"+bucket+flag+".txt");
        try {
            FileInputStream fileInputStream = new FileInputStream(file);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            for (int i=0;i<SIZE;i++){
                bucketList.add((Message) objectInputStream.readObject());
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
//        int count = bucketCountsMap.getOrDefault(bucket,0);
//        if (offset/SIZE == count/SIZE){
//            bucketList = messagePutBuckets.get(bucket);
//        }else {
//            bucketList = messagePullBuckets.get(bucket);
//        }
//        if (offset >= count) {
//            return null;
//        }
//        if (offset%SIZE==0){
//            if (offset/SIZE == count/SIZE){
//                bucketList = messagePutBuckets.get(bucket);
//            }else {
//                bucketList = pullMessageFromFile(bucket,offset);
//                messagePullBuckets.put(bucket,bucketList);
//            }
//        }
        if (offset%SIZE==0) {
            bucketList = pullMessageFromFile(bucket,offset);
            messagePullBuckets.put(bucket,bucketList);
        }else {
            bucketList = messagePullBuckets.get(bucket);
        }
        Message message = bucketList.get(offset%SIZE);
        offsetMap.put(bucket, ++offset);
        return message;
    }
}
