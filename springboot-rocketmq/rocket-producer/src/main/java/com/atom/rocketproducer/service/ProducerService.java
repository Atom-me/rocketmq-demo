package com.atom.rocketproducer.service;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author Atom
 */
@Component
public class ProducerService {

    @Resource
    private RocketMQTemplate rocketMQTemplate;


}
