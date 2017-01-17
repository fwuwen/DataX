package com.alibaba.datax.plugin.writer.otswriter.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class Person implements Serializable {
    private static final long serialVersionUID = 8543714319157542605L;
    private String name;
    private long age;
    private boolean male;
    private double height;
    
    public boolean isMale() {
        return male;
    }
    public void setMale(boolean male) {
        this.male = male;
    }
    public double getHeight() {
        return height;
    }
    public void setHeight(double height) {
        this.height = height;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public long getAge() {
        return age;
    }
    public void setAge(long age) {
        this.age = age;
    }
    
    public static byte[] toByte(Person p) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oout;
        try {
            oout = new ObjectOutputStream(out);
            oout.writeObject(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return out.toByteArray();
    }
    
    public static Person toPerson(byte [] b) {
        ByteArrayInputStream bais = new ByteArrayInputStream(b);
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(bais);
            return (Person) ois.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}