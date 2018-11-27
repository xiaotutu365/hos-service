package com.trey.bigdata.hos.common.util;

import org.apache.hadoop.hbase.util.Bytes;

public class HosUtils {
    public static final String DIR_TABLE_PREFIX = "hos_dir_";
    public static final String OBJ_TABLE_PREFIX = "hos_obj_";

    public static final String DIR_META_CF = "cf";
    public static final byte[] DIR_META_CF_BYTES = DIR_META_CF.getBytes();
    public static final String DIR_SUBDIR_CF = "sub";
    public static final byte[] DIR_SUBDIR_CF_BYTES = DIR_SUBDIR_CF.getBytes();

    public static final String OBJ_META_CF = "cf";
    public static final byte[] OBJ_META_CF_BYTES = OBJ_META_CF.getBytes();
    public static final String OBJ_CONT_CF = "c";
    public static final byte[] OBJ_SUBDIR_CF_BYTES = OBJ_CONT_CF.getBytes();

    public static final byte[] DIR_SEQID_QUALIFIER = "u".getBytes();
    public static final byte[] OBJ_CONT_QUALIFIER = "c".getBytes();
    public static final byte[] OBJ_LEN_QUALIFIER = "l".getBytes();
    public static final byte[] OBJ_PROPS_QUALIFIER = "p".getBytes();
    public static final byte[] DIR_MEIDATYPE_QUALIFIER = "m".getBytes();

    public static final String FILE_STORE_ROOT = "/hos";
    public static final int FILE_STORE_THRESHOLD = 20 * 1024 * 1024;

    public static final String BUCKET_DIR_SEQ_TABLE = "hos_dir_seq";
    public static final String BUCKET_DIR_SEQ_CF = "s";
    public static final byte[] BUCKET_DIR_SEQ_CF_BYTES = BUCKET_DIR_SEQ_CF.getBytes();
    public static final byte[] BUCKET_DIR_SEQ_QUALIFIER = "s".getBytes();

    public static final byte[][] OBJ_REGIONS = new byte[][]{
            Bytes.toBytes("1"),
            Bytes.toBytes("4"),
            Bytes.toBytes("7")
    };

    public static String getDirTableName(String bucketName) {
        return OBJ_TABLE_PREFIX + bucketName;
    }

    public static String getObjTableName(String bucketName) {
        return OBJ_TABLE_PREFIX + bucketName;
    }

    public static String[] getDirColumnFamily() {
        return new String[]{DIR_META_CF, DIR_SUBDIR_CF};
    }

    public static String[] getObjColumnFamily() {
        return new String[]{OBJ_CONT_CF, OBJ_META_CF};
    }
}