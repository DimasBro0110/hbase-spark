package ru.dimas.brosalin.pojo;

import java.io.Serializable;

/**
 * Created by DmitriyBrosalin on 11/03/2017.
 */
public class Message implements Serializable{

    private String urlString;
    private long urlHash;

    public Message(String url, long hash){
        this.urlString = url;
        this.urlHash = hash;
    }

    public String getUrlString() {
        return urlString;
    }

    public void setUrlString(String urlString) {
        this.urlString = urlString;
    }

    public long getUrlHash() {
        return urlHash;
    }

    public void setUrlHash(long urlHash) {
        this.urlHash = urlHash;
    }
}
