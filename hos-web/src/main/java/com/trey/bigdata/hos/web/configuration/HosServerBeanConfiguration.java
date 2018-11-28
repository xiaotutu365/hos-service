package com.trey.bigdata.hos.web.configuration;

import com.trey.bigdata.hos.common.service.HdfsService;
import com.trey.bigdata.hos.common.service.HosStore;
import com.trey.bigdata.hos.core.config.HosConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HosServerBeanConfiguration {

    /**
     * 获取HBase Connection
     *
     * @return
     * @throws IOException
     */
    @Bean
    public Connection getConnection() throws IOException {
        Configuration config = HBaseConfiguration.create();
        HosConfiguration hosConfiguration = HosConfiguration.getHosConfiguration();
        config.set("hbase.zookeeper.quorum", hosConfiguration.getString("hbase.zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort", hosConfiguration.getString("hbase.zookeeper.port"));
        return ConnectionFactory.createConnection(config);
    }

    /**
     * 实例化HosStore
     *
     * @param connection
     * @return
     */
    @Bean
    public HosStore getHosStore(Connection connection) {
        HosConfiguration hosConfiguration = HosConfiguration.getHosConfiguration();
        String zkHosts = hosConfiguration.getString("hbase.zookeeper.quorum");
        HosStore hosStore = new HosStore(connection, new HdfsService(), zkHosts);
        return hosStore;
    }



}