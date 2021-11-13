package com.atom.simplerocketmq.simplemessage;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * @author Atom
 */
public class SimpleProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("simple_producer_group");

        producer.setNamesrvAddr("10.16.118.230:9876");

        producer.start();

        // 创建消息
        Message message = new Message(
                "topic_demo",// topic,会自动创建，rocketmq 配置文件中配置了自动创建topic ： autoCreateTopicEnable=true
                "tag_demo",// tag ,主要用于消息过滤
                "keys_1",//消息的唯一值
                "hello, atom".getBytes(RemotingHelper.DEFAULT_CHARSET)// 消息内容
        );


        // 发送消息
        SendResult sendResult = producer.send(message);

        System.err.println("send result ::: " + sendResult);

        // 关闭 producer
        producer.shutdown();

    }
}
