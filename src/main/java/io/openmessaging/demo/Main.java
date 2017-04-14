package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by hujianxin on 17-4-14.
 */
class Demo implements Serializable {
    private String username;
    private String password;

    public Demo(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "username: " + username + ", password: " + password;
    }
}

public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        FileOutputStream outputStream = new FileOutputStream("/home/hujianxin/tmp/test/demo");
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(new Demo("Hello", "World"));
        objectOutputStream.writeObject(new Demo("Hujianxin", "Nihao"));
        objectOutputStream.writeObject(new Demo("Jeny", "jeny"));
        objectOutputStream.writeObject(new Demo("John", "john"));
        objectOutputStream.close();
        objectOutputStream = null;

        FileInputStream fileInputStream = new FileInputStream("/home/hujianxin/tmp/test/QUEUE11.message");
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
        Message demo1 = (Message) objectInputStream.readObject();
        Message demo2 = (Message) objectInputStream.readObject();
        Message demo3 = (Message) objectInputStream.readObject();
        Message demo4 = (Message) objectInputStream.readObject();
        System.out.println(demo1);
        System.out.println(demo2);
        System.out.println(demo3);
        System.out.println(demo4);
        objectInputStream.close();
        objectInputStream = null;
    }
}
