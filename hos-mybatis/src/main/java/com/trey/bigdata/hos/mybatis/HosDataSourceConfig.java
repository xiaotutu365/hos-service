package com.trey.bigdata.hos.mybatis;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

@Configuration
@MapperScan(basePackages = HosDataSourceConfig.PACKAGE, sqlSessionFactoryRef = "hosSqlSessionFactory")
public class HosDataSourceConfig {
    static final String PACKAGE = "com.trey.bigdata.hos.**";

    @Bean(name = "HosDataSource")
    @Primary
    public DataSource hosDataSource() throws IOException {
        // 1.huo qu datasource xiangguan xinxi
        ResourceLoader loader = new DefaultResourceLoader();
        InputStream inputStream = loader.getResource("classpath:application.properties").getInputStream();
        Properties properties = new Properties();123
        properties.load(inputStream);
        Set<Object> keys = properties.keySet();
        Properties dsProperties = new Properties();
        for (Object key : keys) {
            if (key.toString().startsWith("datasource")) {
                dsProperties.put(key.toString().replace("datasource.", ""), properties);
            }
        }
        HikariDataSourceFactory factory = new HikariDataSourceFactory();
        factory.setProperties(dsProperties);
        inputStream.close();
        return factory.getDataSource();
    }

    @Primary
    @Bean
    public SqlSessionFactory hosSqlSessionFactory(DataSource hosDataSource) throws Exception {
        SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
        sqlSessionFactoryBean.setDataSource(hosDataSource);
        ResourceLoader loader = new DefaultResourceLoader();
        sqlSessionFactoryBean.setConfigLocation(loader.getResource("classpath:mybatis-config.xml"));
        sqlSessionFactoryBean.setSqlSessionFactoryBuilder(new SqlSessionFactoryBuilder());
        return sqlSessionFactoryBean.getObject();
    }
}
