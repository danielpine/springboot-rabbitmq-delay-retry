package io.danielpine.delayqueue.consumer;

import com.rabbitmq.client.Channel;
import io.danielpine.delayqueue.conf.RabbitConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class Consumer {

    private final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Resource
    private RabbitTemplate rabbitTemplate;
    @Resource
    MessagePostProcessor correlationIdProcessor;

    @RabbitListener(queues = "working_demo_queue")
    protected void consumer(Message message, Channel channel) {
        String correlationId = message.getMessageProperties().getCorrelationId();
        try {
            logger.info("================================");
            logger.info("开始处理消息:" + correlationId);
            String number = new String(message.getBody());
            long result = System.currentTimeMillis() / Integer.parseInt(number);
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            logger.info("处理消息结果:" + result);
            logger.info("处理消息成功:" + correlationId);
        } catch (Exception e) {
            String correlationData = (String) message.getMessageProperties().getHeaders().get("spring_returned_message_correlation");
            logger.error("处理消息失败:[" + e.getMessage() + "],原始消息:[" + new String(message.getBody()) + "] correlationId:" + correlationData);
            long retryCount = getRetryCount(message.getMessageProperties());
            try {
                if (retryCount <= 3) {
                    // 重试次数小于3次,NACK REQUEUE FALSE 转到延时重试队列，超时后重新回到工作队列
                    logger.info("开始NACK消息 tag:" + message.getMessageProperties().getDeliveryTag() + " retryCount:" + retryCount);
                    channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, false);
                } else {
                    // 重试次数超过3次,则将消息发送到失败队列等待特定消费者处理或者人工处理
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                    rabbitTemplate.convertAndSend(RabbitConfiguration.FAIL_EXCHANGE_NAME, RabbitConfiguration.FAIL_ROUTING_KEY,
                            message, correlationIdProcessor, new CorrelationData(correlationData));
                    logger.info("连续失败三次，将消息发送到死信队列,发送消息:" + new String(message.getBody()));
                }
            } catch (Exception ee) {
                // TODO 文件或数据库兜底方案
                logger.error("发送死信异常:" + ee.getMessage() + ",原始消息:" + new String(message.getBody()), ee);
            }
        }
    }


    /**
     * 获取消息被重试的次数
     */
    public long getRetryCount(MessageProperties messageProperties) {
        Long retryCount = 0L;
        if (null != messageProperties) {
            List<Map<String, ?>> deaths = messageProperties.getXDeathHeader();
            if (deaths != null && deaths.size() > 0) {
                Map<String, Object> death = (Map<String, Object>) deaths.get(0);
                retryCount = (Long) death.get("count");
            }
        }
        return retryCount;
    }
}
