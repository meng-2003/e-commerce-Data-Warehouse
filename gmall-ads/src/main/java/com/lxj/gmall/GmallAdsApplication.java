package com.lxj.gmall;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author Laoxingjie
 * @description ads服务启动类
 * @create 2026/4/14 20:41
 **/
@SpringBootApplication // 核心：标记这是SpringBoot启动类
public class GmallAdsApplication {

    public static void main(String[] args) {
        // 启动SpringBoot应用
        SpringApplication.run(GmallAdsApplication.class, args);
    }

}