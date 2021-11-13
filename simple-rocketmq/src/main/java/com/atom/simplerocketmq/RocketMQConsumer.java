package com.atom.simplerocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 消费者消费数据
 *
 * @author Atom
 **/
@Component
@Order(2)
public class RocketMQConsumer implements ApplicationRunner {
    @Override
    public void run(ApplicationArguments args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("FirstConsumerGroup");
        consumer.setNamesrvAddr("10.16.118.230:9876");

        consumer.setInstanceName("Consumer");
        // 订阅，可以订阅多个主题，多写几个
        // 订阅多个标签TagA、TagB、TagC：consumer.subscribe("TopicTest","TagA || TagB || TagC");
        // 订阅所有标签：consumer.subscribe("TopicTest","*");
        consumer.subscribe("TopicTest", "TagA");

        consumer.registerMessageListener((List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) -> {
            System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + list.size());
            MessageExt msg = list.get(0);
            if ("TopicTest".equals(msg.getTopic())) {
                if ("TagA".equals(msg.getTags())) {
                    System.out.println(new String(msg.getBody()));
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        // 启动consumer
        consumer.start();
        // 暂时不关闭，消费者可以持续消费数据；后续可以通过rocketmq-console发布消息，测试消费者接收
//        consumer.shutdown();
    }
}
