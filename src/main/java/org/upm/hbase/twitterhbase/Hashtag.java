package org.upm.hbase.twitterhbase;


public class Hashtag {

    private String key;
    private int value;

    public Hashtag(String hash, int count) {
        this.key = hash;
        this.value = count;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String hash) {
        this.key = hash;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int count) {
        this.value = count;
    }

    @Override
    public String toString() {
        return key + "," + value;
    }

}