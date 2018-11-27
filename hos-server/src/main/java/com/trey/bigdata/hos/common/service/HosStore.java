package com.trey.bigdata.hos.common.service;

import com.trey.bigdata.hos.common.HosObjectSummary;
import com.trey.bigdata.hos.common.ObjectListResult;
import com.trey.bigdata.hos.common.util.HosUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

@Slf4j
public class HosStore {

    private Connection conn = null;

    private HdfsService fileStore;

    private String zkUrls;

    private CuratorFramework zkClient;

    /**
     * 构造函数
     *
     * @param conn
     * @param fileStore
     * @param zkUrls
     */
    public HosStore(Connection conn, HdfsService fileStore, String zkUrls) {
        this.conn = conn;
        this.fileStore = fileStore;
        this.zkUrls = zkUrls;
        zkClient = CuratorFrameworkFactory.newClient(zkUrls, new ExponentialBackoffRetry(20, 5));
        this.zkClient.start();
    }

    public void createBucketStore(String bucket) {
        // 1、创建目录表
        HBaseService.createTable(conn, HosUtils.getDirTableName(bucket), HosUtils.getDirColumnFamily(), null);
        // 2、创建文件表
        HBaseService.createTable(conn, HosUtils.getObjTableName(bucket), HosUtils.getObjColumnFamily(), HosUtils.OBJ_REGIONS);
        // 3、将其添加到seq表
        Put put = new Put(Bytes.toBytes(bucket));
        put.addColumn(HosUtils.BUCKET_DIR_SEQ_CF_BYTES, HosUtils.BUCKET_DIR_SEQ_QUALIFIER, Bytes.toBytes(0L));
        HBaseService.putRow(conn, HosUtils.BUCKET_DIR_SEQ_TABLE, put);
        // 4、创建hdfs目录
        fileStore.mkDir(HosUtils.FILE_STORE_ROOT + File.separator + bucket);
    }

    public void deleteBucketStore(String bucket) {
        // 1、删除目录表和文件表
        HBaseService.deleteTable(conn, HosUtils.getDirTableName(bucket));
        HBaseService.deleteTable(conn, HosUtils.getObjTableName(bucket));
        // 2、删除seq表中的记录
        HBaseService.deleteRow(conn, HosUtils.BUCKET_DIR_SEQ_TABLE, bucket);
        ///3、删除hdfs上的目录
        fileStore.deleteDir(HosUtils.FILE_STORE_ROOT + File.separator + bucket);
    }

    public void createSeqTable() {
        HBaseService.createTable(conn, HosUtils.BUCKET_DIR_SEQ_TABLE,
                new String[]{HosUtils.BUCKET_DIR_SEQ_CF}, null);
    }

    // ------------------------------

    public void put(String bucket, String key, ByteBuffer content, long length, String mediaType, Map<String, String> properties) {
        // 1、判断是否创建目录
        if (key.endsWith("/")) {
            putDir(bucket, key);
            return;
        }

        String dir = key.substring(0, key.substring("/") + 1);
        String hash = null;

        while (hash == null) {
            if (!dirExist(bucket, dir)) {
                hash = putDir(bucket, dir);
            } else {
                hash = getDirSeqId(bucket, dir);
            }
        }
    }

    // private String putDir

    public HosObjectSummary getSummary(String bucket, String key) {

    }

    public List<HosObjectSummary> list(String bucket, String startKey, String endKey) {

    }

    public ObjectListResult listDir(String bucket, String dir, String start, int maxCount) {

    }

    public ObjectListResult listByPrefix(String bucket, String dir, String start, String prefix, int maxCount) {

    }
}
