package com.atguigu.gmall.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr){
       log.info(jsonStr);
       kafkaTemplate.send("ods_base_log",jsonStr);
       return "success";
    }
}
