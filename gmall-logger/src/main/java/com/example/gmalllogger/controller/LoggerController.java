package com.example.gmalllogger.controller;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import constant.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author oranglzc
 * @Description:
 * @creat 2020-07-13-20:01
 */
//    @RequestMapping(value = "/log", method = RequestMethod.POST)
//    @ResponseBody  //表示返回值是一个 字符串, 而不是 页面名

@RestController
public class LoggerController {
    //表示post上传web服务器/log时，执行的动作
    @PostMapping("/log")
    public String doLog(String log){
        //1. 给日志添加一个时间戳
        log=addTS(log);
        // 2. 数据落盘(为离线数据做准备)
        saveToDisk(log);
        // 3. 把数据写入到kafka, 需要写入到topic
        sendToKafka(log);
        return "ok";
    }

    //可以对类成员变量、方法及构造函数进行标注，让 spring 完成 bean 自动装配的工作。
//@Autowired 默认是按照类去匹配，配合 @Qualifier 指定按照名称去装配 bean
    @Autowired
    KafkaTemplate kafka;

    /**
     * 日志发往kafka
     * 不同的日志写到不同的topic
     * @param log
     */
    private void sendToKafka(String log) {
        //为了避免除了type字段外的字符串出现相同字符串，加上""判定
        if (log.contains("\"startup\"")){
            kafka.send(Constant.STARTUP_TOPIC,log);
        }else {
            kafka.send(Constant.EVENT_TOPIC,log);
        }

    }
    Logger logger = LoggerFactory.getLogger(LoggerController.class);
    private void saveToDisk(String log) {
        logger.info(log);
    }

    /**
     * 添加时间戳
     * @param log
     * @return
     */
    public String addTS(String log){
        JSONObject jsonObject = JSON.parseObject(log);
        jsonObject.put("ts", System.currentTimeMillis());
        return JSON.toJSONString(jsonObject);
    }

}
