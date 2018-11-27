package com.trey.bigdata.hos.common.service;

import com.trey.bigdata.hos.core.ErrorCodes;
import com.trey.bigdata.hos.common.exception.HosServerException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HBaseService {

    /**
     * 创建表
     *
     * @param conn
     * @param tableName
     * @param cfs
     * @param splitKeys
     * @return
     */
    public static boolean createTable(Connection conn, String tableName, String[] cfs, byte[][] splitKeys) {
        try {
            // 1、检查表是否存在，如果存在返回false
            HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
            if (admin.tableExists(tableName)) {
                return false;
            }
            // 2、如果不存在，则创建表
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "create table error");
        }
        return true;
    }

    /**
     * 删除表
     *
     * @param conn
     * @param tableName
     * @return
     */
    public static boolean deleteTable(Connection conn, String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "delete table error");
        }
        return true;
    }

    /**
     * 删除列族
     *
     * @param conn
     * @param tableName
     * @param cf
     * @return
     */
    public static boolean deleteColumnFamily(Connection conn, String tableName, String cf) {
        try {
            HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
            admin.deleteColumn(tableName, cf);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "delete cf error");
        }
        return true;
    }

    /**
     * 删除列限定符
     *
     * @param conn
     * @param tableName
     * @param rowKey
     * @param cf
     * @param column
     * @return
     */
    public static boolean deleteColumnQualifier(Connection conn, String tableName, String rowKey, String cf, String column) {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));
        deleteColumnQualifier(conn, tableName, delete);
        return true;
    }

    public static boolean deleteColumnQualifier(Connection conn, String tableName, Delete delete) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "delete qualifier error");
        }
        return true;
    }

    /**
     * 删除行
     *
     * @param conn
     * @param tableName
     * @param rowKey
     * @return
     */
    public static boolean deleteRow(Connection conn, String tableName, String rowKey) {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        deleteRow(conn, tableName, delete);
        return true;
    }

    public static boolean deleteRow(Connection conn, String tableName, Delete delete) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "delete rowKey error");
        }
        return true;
    }

    /**
     * 获取行数据
     *
     * @param conn
     * @param tableName
     * @param rowKey
     * @return
     */
    public static Result getRow(Connection conn, String tableName, String rowKey) {
        Get get = new Get(Bytes.toBytes(rowKey));
        return getRow(conn, tableName, get);
    }

    public static Result getRow(Connection conn, String tableName, Get get) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "get data error");
        }
    }

    public static ResultScanner getResultScanner(Connection conn, String tableName, String startRow, String stopRow, FilterList filterList) {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        scan.setFilter(filterList);
        scan.setCaching(1000);
        return getResultScanner(conn, tableName, scan);
    }

    public static ResultScanner getResultScanner(Connection conn, String tableName, Scan scan) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static boolean putRow(Connection conn, String tableName, Put put) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static boolean putRow(Connection conn, String tableName, List<Put> puts) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static long incrementColumnValue(Connection conn, String tableName, String row, String cf, String qualifier, int num) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            return table.incrementColumnValue(Bytes.toBytes(row), Bytes.toBytes(cf), Bytes.toBytes(qualifier), num);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "put row error");
        }
    }

    public static boolean existsRow(Connection conn, String tableName, String row) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(row));
            return table.exists(get);
        } catch (IOException e) {
            e.printStackTrace();
            throw new HosServerException(ErrorCodes.ERROR_HBASE, "get row error");
        }
    }
}
