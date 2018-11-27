package com.trey.bigdata.hos.common.service;

import com.trey.bigdata.hos.core.ErrorCodes;
import com.trey.bigdata.hos.core.config.HosConfiguration;
import com.trey.bigdata.hos.common.exception.HosServerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
public class HdfsService {

    private FileSystem fileSystem;

    private long defaultBlockSize = 128 * 1024 * 1024;

    private long initBlockSize = defaultBlockSize / 2;

    public HdfsService() {
        HosConfiguration hosConf = HosConfiguration.getHosConfiguration();
        String confDir = hosConf.getString("hadoop.conf.dir");
        String hdfsUri = hosConf.getString("hadoop.uri");

        Configuration conf = new Configuration();
        conf.addResource(new Path(confDir + "/hdfs-site.xml"));
        conf.addResource(new Path(confDir + "/core-site.xml"));

        try {
            fileSystem = FileSystem.get(new URI(hdfsUri), conf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     * 保存文件到Hdfs中
     *
     * @param dir
     * @param name
     * @param inputStream
     * @param replication
     */
    public void saveFile(String dir, String name, InputStream inputStream, long length, short replication) {
        // 判断dir是否存在，不存在则新建
        Path dirPath = new Path(dir);
        try {
            if (!fileSystem.exists(dirPath)) {
                boolean success = fileSystem.mkdirs(dirPath, FsPermission.getDirDefault());
                log.info("create dir " + dirPath);
                if (!success) {
                    throw new HosServerException(ErrorCodes.ERROR_HDFS, "create dir " + dirPath + " error");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 保存文件
        Path filePath = new Path("dir" + File.separator + name);
        long blockSize = length <= initBlockSize ? initBlockSize : defaultBlockSize;
        FSDataOutputStream fsos = null;
        try {
            fsos = fileSystem.create(filePath, true, 512 + 1024, replication, blockSize);
            fileSystem.setPermission(filePath, FsPermission.getFileDefault());
            byte[] buffer = new byte[512 * 1024];
            int len = -1;
            while ((len = inputStream.read(buffer)) > 0) {
                fsos.write(buffer, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fsos != null) {
                    fsos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 删除文件
     *
     * @param dir
     * @param name
     */
    public void deleteFile(String dir, String name) {
        try {
            fileSystem.delete(new Path(dir + File.separator + name), false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 打开文件
     *
     * @param dir
     * @param name
     * @return
     */
    public InputStream openFile(String dir, String name) {
        try {
            return fileSystem.open(new Path(dir + File.separator + name));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 创建目录
     *
     * @param dir
     */
    public void mkDir(String dir) {
        try {
            fileSystem.mkdirs(new Path(dir));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除目录
     */
    public void deleteDir(String dir) {
        try {
            fileSystem.delete(new Path(dir), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}