package io.openmessaging.demo;

import io.openmessaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

/**
 * Message操作的类.
 * 单例模式，操作过程中就这一个MessageStore。
 * 实现了两个功能：
 * 1. 将一条消息发送出去（放到内存或者文件）
 * 2. 将一条消息获取出来（从内存或者文件里拿到数据）
 */
public class MessageStore {
    private static final Logger logger = LoggerFactory.getLogger(DefaultPullConsumer.class);

    private static final int SIZE = 1000000;
    private static final int MSG_SIZE = 100;

    private static final MessageStore INSTANCE = new MessageStore();

    // TODO：是否加入线程安全保障
    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, MappedByteBuffer> messagePutBuckets = new HashMap<>();

    private Map<String, MappedByteBuffer> messagePullBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private String filePath;

    private Map<String, Integer> bucketCountsMap = new HashMap<>();

    private Map<String, Object> bucketObject = new HashMap<>();

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    private void saveMessageToBuffer(String bucket, DefaultBytesMessage message, ByteBuffer byteBufferMessage) throws IOException {

        StringBuffer str = new StringBuffer();
        for (String header : message.headers().keySet()) {
            str.append(header + " ");
            str.append(message.headers().getString(header) + " ");
        }
        str.append(",");
        if (message.properties() != null) {
            for (String properties : message.properties().keySet()) {
                str.append(properties + " ");
                str.append(message.properties().getString(properties) + " ");
            }
        }

        byte[] headerProperties = str.toString().getBytes();
        int leftLength = byteBufferMessage.capacity() - byteBufferMessage.position();
        int msgLen = 4 + headerProperties.length + 4 + message.getBody().length;
        if (msgLen > leftLength) {
            byteBufferMessage = expandMappedFile(bucket, byteBufferMessage.position(), msgLen);
            messagePutBuckets.put(bucket, (MappedByteBuffer) byteBufferMessage);
        }

        byteBufferMessage.putInt(headerProperties.length);
        byteBufferMessage.put(headerProperties);
        byteBufferMessage.putInt(message.getBody().length);
        byteBufferMessage.put(message.getBody());
    }

    private MappedByteBuffer expandMappedFile(String bucket, int position, int msgLen) throws IOException {
        int count = bucketCountsMap.getOrDefault(bucket, 0) / SIZE;
        String filename = filePath + bucket + count + ".txt";
        long sizeNeed = SIZE * MSG_SIZE;
        if (sizeNeed < msgLen) {
            sizeNeed = msgLen;
        }
        return new RandomAccessFile(filename, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, position, sizeNeed);
    }

    private MappedByteBuffer getPutMappedFile(String bucket) {
        MappedByteBuffer mappedByteBuffer = null;
        int count = bucketCountsMap.getOrDefault(bucket, 0) / SIZE;
        File file = new File(filePath + "/" + bucket + count + ".txt");
        try {
            mappedByteBuffer = new RandomAccessFile(file, "rw")
                    .getChannel().map(FileChannel.MapMode.READ_WRITE, 0, SIZE * MSG_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return mappedByteBuffer;
    }

    public synchronized void putMessage(String bucket, Message message) throws IOException {
//        if (!bucketObject.containsKey(bucket)) {
//            bucketObject.put(bucket, new Object());
//        }
//        synchronized(bucketObject.get(bucket)){
        if (!messagePutBuckets.containsKey(bucket)) {
            messagePutBuckets.put(bucket, getPutMappedFile(bucket));
        }
        int count = bucketCountsMap.getOrDefault(bucket, 0);
        if (count % SIZE == 0) {
            messagePutBuckets.put(bucket, getPutMappedFile(bucket));
        }
        MappedByteBuffer bucketBuffer = messagePutBuckets.get(bucket);
        saveMessageToBuffer(bucket, (DefaultBytesMessage) message, bucketBuffer);
        bucketCountsMap.put(bucket, ++count);
//        }
    }

    private synchronized MappedByteBuffer getPullMappedFile(String bucket, long offset) {
        MappedByteBuffer mappedByteBuffer = null;
        int flag = (int) offset / SIZE;
        File file = new File(filePath + "/" + bucket + flag + ".txt");
        try {
            RandomAccessFile raf = new RandomAccessFile(file,"r");
            mappedByteBuffer = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mappedByteBuffer;
    }

    private Message pullMessageFromBuffer(ByteBuffer buffer) {
        byte[] headerProperties = new byte[buffer.getInt()];
        buffer.get(headerProperties);
        if (headerProperties.length == 0)
            return null;
        byte[] body = new byte[buffer.getInt()];
        buffer.get(body);
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(body);
        String[] str = new String(headerProperties).split(",");
        String[] header = str[0].split(" ");
        for (int j = 0; j < header.length; j = j + 2) {
            defaultBytesMessage.putHeaders(header[j].split(" ")[0], header[j + 1].split(" ")[0]);
        }
        if (str.length > 1) {
            String[] properties = str[1].split(" ");
            for (int j = 0; j < properties.length; j = j + 2) {
                defaultBytesMessage.putProperties(properties[j].split(" ")[0], properties[j + 1].split(" ")[0]);
            }
        }

        return defaultBytesMessage;
    }

    public synchronized Message pullMessage(String queue, String bucket) {
        MappedByteBuffer bucketbufer = null;
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset % SIZE == 0) {
            bucketbufer = messagePullBuckets.get(bucket);
            if (bucketbufer != null)
                clean(bucketbufer);
            bucketbufer = getPullMappedFile(bucket, offset);
            messagePullBuckets.put(bucket, bucketbufer);
        } else {
            bucketbufer = messagePullBuckets.get(bucket);
        }
        Message message = null;
        if (bucketbufer != null) {
            message = pullMessageFromBuffer(bucketbufer);
            offsetMap.put(bucket, ++offset);
        }else {
            logger.info("BucketBuffer is null-"+bucket);
        }
        if (message!=null)
            logger.info(bucket+"-"+offset+"-"+new String(((DefaultBytesMessage)message).getBody()));
        return message;
    }

    public static void clean(final Object buffer) {
        AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                try {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
                    cleaner.clean();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });

    }
}
