package com.trey.bigdata.hos.common.service;

import com.trey.bigdata.hos.common.HosObject;
import com.trey.bigdata.hos.common.HosObjectSummary;
import com.trey.bigdata.hos.common.ObjectListResult;
import com.trey.bigdata.hos.common.ObjectMetaData;
import com.trey.bigdata.hos.common.util.HosUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
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
        InterProcessMutex lock = null;

        // 1、判断是否创建目录
        if (key.endsWith("/")) {
            putDir(bucket, key);
            return;
        }

        // 获取seqid
        String dir = key.substring(0, key.substring("/") + 1);
        String hash = null;

        while (hash == null) {
            if (!dirExist(bucket, dir)) {
                hash = putDir(bucket, dir);
            } else {
                hash = getDirSeqId(bucket, dir);
            }
        }

        // 上传文件到文件表


        // 获取锁
        String lockKey = key.replace("/", "_");
        lock = new InterProcessMutex(zkClient, "/hos/" + bucket + "/" + lockKey);
        try {
            lock.acquire();
            String fileKey = hash + "_" + key.substring(key.lastIndexOf("/") + 1);
            Put contentPut = new Put(fileKey.getBytes());
            if (!Strings.isNotEmpty(mediaType)) {
                contentPut.addColumn(HosUtils.OBJ_META_CF_BYTES, HosUtils.OBJ_MEIDATYPE_QUALIFIER, mediaType.getBytes());
            }

            // todo add props length

            // 判断文件的大小，如果小于20M，存储到hbase，否则存储到hdfs
            if (length <= HosUtils.FILE_STORE_THRESHOLD) {
                ByteBuffer byteBuffer = ByteBuffer.wrap(HosUtils.OBJ_CONT_QUALIFIER);
                contentPut.addColumn(HosUtils.OBJ_CONT_CF_BYTES, byteBuffer, System.currentTimeMillis(), content);
                byteBuffer.clear();
            } else {
                String fileDir = HosUtils.FILE_STORE_ROOT + "/" + bucket + "/" + hash;
                String name = key.substring(key.lastIndexOf("/") + 1);
                InputStream inputStream = new ByteBufferInputStream(content);
                fileStore.saveFile(fileDir, name, inputStream, length, (short) 1);
            }
            HBaseService.putRow(conn, HosUtils.getObjTableName(bucket), contentPut);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 释放锁
        if (lock != null) {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    private String putDir(String bucket, String key) {
        if (dirExist(bucket, key)) {
            return null;
        }

        // 为了防止用户A在操作某个目录的时候，用户B将某个目录删除，所以使用ZK的分布式锁
        // 从ZK获取锁
        InterProcessMutex lock = null;
        try {
            String lockKey = key.replace("/", "_");
            lock = new InterProcessMutex(zkClient, "/hos" + bucket + "/" + lockKey);
            lock.acquire();
            String dir1 = key.substring(0, key.lastIndexOf("/"));
            String name = dir1.substring(dir1.lastIndexOf("/"));
            if (name.length() > 0) {
                String parent = dir1.substring(0, dir1.lastIndexOf("/") + 1);
                if (!dirExist(bucket, parent)) {
                    this.putDir(bucket, parent);
                }
                // 在父目录添加sub列族
                Put put = new Put(Bytes.toBytes(parent));
                put.addColumn(HosUtils.DIR_SUBDIR_CF_BYTES, Bytes.toBytes(name), Bytes.toBytes(1));
                HBaseService.putRow(conn, HosUtils.getDirTableName(bucket), put);
            }
            // 再去添加到目录表
            String seqId = getDirSeqId(bucket, key);
            String hash = seqId == null ? makeDirSeqId(bucket) : seqId;
            Put dirPut = new Put(Bytes.toBytes(key));
            dirPut.addColumn(HosUtils.DIR_META_CF_BYTES, HosUtils.DIR_SEQID_QUALIFIER, Bytes.toBytes(hash));
            HBaseService.putRow(conn, HosUtils.getDirTableName(bucket), dirPut);
            return hash;
        } catch (Exception e) {

        } finally {
            if (lock != null) {
                lock.release();
            }
        }
        return null;
    }

    private String makeDirSeqId(String bucket) {
        long v = HBaseService.incrementColumnValue(conn, HosUtils.BUCKET_DIR_SEQ_TABLE, bucket,
                String.valueOf(HosUtils.BUCKET_DIR_SEQ_CF_BYTES), String.valueOf(HosUtils.BUCKET_DIR_SEQ_QUALIFIER), 1);
        return String.format("%da%d", v % 64, v);
    }

    /**
     * 判断目录是否存在
     *
     * @param bucket
     * @param key
     * @return
     */
    private boolean dirExist(String bucket, String key) {
        return HBaseService.existsRow(conn, HosUtils.getDirTableName(bucket), key);
    }

    private String getDirSeqId(String bucket, String key) {
        Result result = HBaseService.getRow(conn, HosUtils.getDirTableName(bucket), key);
        if (result.isEmpty()) {
            return null;
        }
        return Bytes.toString(result.getValue(HosUtils.DIR_META_CF_BYTES, HosUtils.DIR_SEQID_QUALIFIER));
    }

    public HosObjectSummary getSummary(String bucket, String key) {
        // 判断是否为文件夹
        if (key.endsWith("/")) {
            Result result = HBaseService.getRow(conn, HosUtils.getDirTableName(bucket), key);
            if (!result.isEmpty()) {
                // 读取文件夹的基础属性，转换为HosObjectSummary
                return this.dirObjectToSummary(result, bucket, key);
            }
            return null;
        }
        // 获取文件的基本属性
        String dir = key.substring(0, key.lastIndexOf("/") + 1);
        String seq = getDirSeqId(bucket, dir);
        if (seq == null) {
            return null;
        }
        String objKey = seq + "_" + key.substring(key.lastIndexOf("/") + 1);
        Result result = HBaseService.getRow(conn, HosUtils.getObjTableName(bucket), objKey);
        if (result.isEmpty()) {
            return null;
        }
        return this.resultToObjectSummary(result, bucket, dir);
    }

    /**
     * 将dirObject转换为summary
     *
     * @param result
     * @param bucket
     * @param dir
     * @return
     */
    private HosObjectSummary dirObjectToSummary(Result result, String bucket, String dir) {
        HosObjectSummary summary = new HosObjectSummary();
        summary.setId(Bytes.toString(result.getRow()));
        summary.setAttrs(new HashMap<>(0));
        summary.setBucket(bucket);
        summary.setLastModifyTime(result.rawCells()[0].getTimestamp());
        summary.setLength(0L);
        summary.setMediaType("");
        if (dir.length() > 1) {
            summary.setName(dir.substring(dir.lastIndexOf("/") + 1));
        } else {
            summary.setName("");
        }
        return summary;
    }

    private HosObjectSummary resultToObjectSummary(Result result, String bucket, String dir) {
        HosObjectSummary summary = new HosObjectSummary();
        long timeStamp = result.rawCells()[0].getTimestamp();
        summary.setLastModifyTime(timeStamp);
        String id = new String(result.getRow());
        summary.setId(id);
        String name = id.split("_", 2)[1];
        summary.setName(name);
        summary.setKey(dir + name);
        summary.setBucket(bucket);
        summary.setMediaType(Bytes.toString(result.getValue(HosUtils.OBJ_META_CF_BYTES, HosUtils.OBJ_MEIDATYPE_QUALIFIER)));
        // todo length attr
        return summary;
    }

    public HosObject getObject(String bucket, String key) {
        if (key.endsWith("/")) {
            Result result = HBaseService.getRow(conn, HosUtils.getDirTableName(bucket), key);
            if (result.isEmpty()) {
                return null;
            }
            ObjectMetaData objectMetaData = new ObjectMetaData();
            objectMetaData.setBucket(bucket);
            objectMetaData.setKey(key);
            objectMetaData.setLength(0);
            objectMetaData.setLastModifyTime(result.rawCells()[0].getTimestamp());
            HosObject object = new HosObject();
            object.setMetaData(objectMetaData);
            return object;
        }

        // 读取文件表
        String dir = key.substring(0, key.lastIndexOf("/") + 1);
        String seq = getDirSeqId(bucket, dir);
        if (seq == null) {
            return null;
        }
        String objKey = seq + "_" + key.substring(key.lastIndexOf("/") + 1);
        Result result = HBaseService.getRow(conn, HosUtils.getObjTableName(bucket), objKey);
        if (result.isEmpty()) {
            return null;
        }
        HosObject object = new HosObject();
        long len = Bytes.toLong(result.getValue(HosUtils.OBJ_META_CF_BYTES, HosUtils.OBJ_LEN_QUALIFIER));
        ObjectMetaData objectMetaData = new ObjectMetaData();
        objectMetaData.setBucket(bucket);
        objectMetaData.setKey(key);
        objectMetaData.setLength(len);
        objectMetaData.setLastModifyTime(result.rawCells()[0].getTimestamp());
        objectMetaData.setMediaType(Bytes.toString(result.getValue(HosUtils.OBJ_META_CF_BYTES, HosUtils.OBJ_MEIDATYPE_QUALIFIER)));
        byte[] b = result.getValue(HosUtils.OBJ_META_CF_BYTES, HosUtils.OBJ_PROPS_QUALIFIER);
        if (b != null) {
            // todo
            // JsonUtil.fromJson(Map.class, Bytes.toString(b))
            // objectMetaData.setAttrs(new HashMap<>());
        }
        object.setMetaData(objectMetaData);
        // 读取文件内容，是从hbase中读取，还是从hdfs中读取
        if (result.containsNonEmptyColumn(HosUtils.OBJ_CONT_CF_BYTES, HosUtils.OBJ_CONT_QUALIFIER)) {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(result.getValue(HosUtils.OBJ_CONT_CF_BYTES, HosUtils.OBJ_CONT_QUALIFIER));
            object.setContent(inputStream);
        } else {
            String fileDir = HosUtils.FILE_STORE_ROOT + "/" + bucket + "/" + seq;
            InputStream inputStream = this.fileStore.openFile(
                    fileDir, key.substring(key.lastIndexOf("/") + 1)
            );
            object.setContent(inputStream);
        }
        return object;
    }

    public List<HosObjectSummary> list(String bucket, String startKey, String endKey) {

    }

    public ObjectListResult listDir(String bucket, String dir, String start, int maxCount) {

    }

    public ObjectListResult listByPrefix(String bucket, String dir, String start, String prefix, int maxCount) {

    }
}