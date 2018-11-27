package com.trey.bigdata.hos.common;

import lombok.Data;
import okhttp3.Response;

import java.io.IOException;
import java.io.InputStream;

@Data
public class HosObject {
    private ObjectMetaData metaData;

    private InputStream content;

    private Response response;

    public HosObject() {

    }

    public HosObject(Response response) {
        this.response = response;
    }

    public void close() {
        try {
            if(content != null) {
                this.content.close();
            }
            if(response != null) {
                this.response.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
