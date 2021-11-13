package com.atom.rocketconsumer;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 注意：
 * <p>
 * 1、消费端的注解@RocketMQMessageListener属性中consumerGroup，多个消费端（消费集群）消费同一个topic的时候，需要定义成一致；
 * <p>
 * 2、消费端消费的时候，是会多线程的形式消费topic里的4个MessageQueue的，
 * 如果要消费顺序消息，需要指定属性consumeMode为ConsumeMode.ORDERLY，表示同步消费；
 * <p>
 *
 * @author Atom
 */
@Component
@RocketMQMessageListener(consumerGroup = "${rocketmq.consumer.group}", topic = "test_producer")
public class DemoConsumer implements RocketMQListener<String> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void onMessage(String message) {
        logger.info("[onMessage][线程编号:{} 消息内容：{}]", Thread.currentThread().getId(), message);
    }
}
