package com.trey.bigdata.hos.web;

import com.trey.bigdata.hos.web.security.SecurityInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

public class HosServerApp {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private SecurityInterceptor securityInterceptor;

    public static void main(String[] args) {
        SpringApplication.run(HosServerApp.class);
    }

    @Bean
    public WebMvcConfigurer configurer() {
        return new WebMvcConfigurerAdapter() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                super.addInterceptors(registry);
                registry.addInterceptor(securityInterceptor);
            }

            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**").allowedOrigins("*");
            }
        };
    }
}
