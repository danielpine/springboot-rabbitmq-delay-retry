package io.danielpine.delayqueue.conf;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class RabbitConfiguration {
    /**
     * working模块direct交换机的名字
     */
    public static final String WORKING_EXCHANGE = "working_direct_exchange";

    /**
     * demo业务的队列名称
     */
    public static final String WORKING_DEMO_QUEUE = "working_demo_queue";

    /**
     * demo业务的routekey
     */
    public static final String WORKING_DEMO_ROUTINGKEY = "working_demo_key";

    /**
     * 业务交换机交换机(一个项目一个业务交换机即可)
     * 1.定义direct exchange，绑定queueTest
     * 2.durable="true" rabbitmq重启的时候不需要创建新的交换机
     * 3.direct交换器相对来说比较简单，匹配规则为：如果路由键匹配，消息就被投送到相关的队列
     * fanout交换器中没有路由键的概念，他会把消息发送到所有绑定在此交换器上面的队列中。
     * topic交换器你采用模糊匹配路由键的原则进行转发消息到队列中
     */
    @Bean
    public DirectExchange workingExchange() {
        DirectExchange directExchange = new DirectExchange(WORKING_EXCHANGE, true, false);
        return directExchange;
    }

    /**
     * 新建队列(一个业务需要一个队列一个routekey 命名格式 项目名-业务名)
     * 1.队列名称
     * 2.durable="true" 持久化 rabbitmq重启的时候不需要创建新的队列
     * 3.exclusive 表示该消息队列是否只在当前connection生效,默认是false
     * 4.auto-delete 表示消息队列没有在使用时将被自动删除 默认是false
     * 5.对nack或者发送超时的 发送给死信队列 args是绑定死信队列
     */
    @Bean
    public Queue workingDemoQueue() {
        return QueueBuilder.durable(WORKING_DEMO_QUEUE)
                .withArgument(RETRY_LETTER_QUEUE_KEY, WORKING_RETRY_EXCHANGE_NAME)
                .withArgument(RETRY_LETTER_ROUTING_KEY, WORKING_DEMO_RETRY_ROUTING_KEY)
                .build();
    }

    /**
     * 交换机与routekey绑定
     *
     * @return
     */
    @Bean
    public Binding workingDemoBinding() {
        return BindingBuilder.bind(workingDemoQueue()).to(workingExchange())
                .with(WORKING_DEMO_ROUTINGKEY);
    }

    //
    /**
     * 延时队列 交换机配置标识符(固定)
     */
    public static final String RETRY_LETTER_QUEUE_KEY = "x-dead-letter-exchange";

    /**
     * 延时队列交换机绑定配置键标识符(固定)
     */
    public static final String RETRY_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";

    /**
     * 延时队列消息的配置超时时间枚举(固定)
     */
    public static final String RETRY_MESSAGE_TTL = "x-message-ttl";

    /**
     * working模块延时队列交换机
     */
    public final static String WORKING_RETRY_EXCHANGE_NAME = "working_retry_exchange";

    /**
     * working模块DEMO业务延时队列
     */
    public final static String WORKING_DEMO_RETRY_QUEUE_NAME = "working_demo_retry_queue";

    /**
     * working模块DEMO延时队列routekey
     */
    public final static String WORKING_DEMO_RETRY_ROUTING_KEY = "working_demo_retry_key";

    /**
     * 延时队列交换机
     *
     * @return
     */
    @Bean
    public DirectExchange workingRetryExchange() {
        DirectExchange directExchange = new DirectExchange(WORKING_RETRY_EXCHANGE_NAME, true, false);
        return directExchange;
    }

    /**
     * 新建延时队列 一个业务队列需要一个延时队列
     *
     * @return
     */
    @Bean
    public Queue workingDemoRetryQueue() {
        Map<String, Object> args = new ConcurrentHashMap<>(3);
        // 将消息重新投递到业务交换机Exchange中
        args.put(RETRY_LETTER_QUEUE_KEY, WORKING_EXCHANGE);
        args.put(RETRY_LETTER_ROUTING_KEY, WORKING_DEMO_ROUTINGKEY);
        // 消息在队列中延迟3s后超时，消息会重新投递到x-dead-letter-exchage对应的队列中，routingkey为自己指定
        args.put(RETRY_MESSAGE_TTL, 3 * 1000);
        return new Queue(WORKING_DEMO_RETRY_QUEUE_NAME, true, false, false, args);
    }

    /**
     * 绑定以上定义关系
     *
     * @return
     */
    @Bean
    public Binding retryDirectBinding() {
        return BindingBuilder.bind(workingDemoRetryQueue()).to(workingRetryExchange())
                .with(WORKING_DEMO_RETRY_ROUTING_KEY);
    }

    /**
     * 死信队列
     */
    public final static String FAIL_QUEUE_NAME = "fail_queue";

    /**
     * 死信交换机
     */
    public final static String FAIL_EXCHANGE_NAME = "fail_exchange";

    /**
     * 死信routing
     */
    public final static String FAIL_ROUTING_KEY = "fail_routing";

    /**
     * 创建配置死信队列
     */
    @Bean
    public Queue deadQueue() {
        return new Queue(FAIL_QUEUE_NAME, true, false, false);
    }

    /**
     * 死信交换机
     *
     * @return
     */
    @Bean
    public DirectExchange deadExchange() {
        DirectExchange directExchange = new DirectExchange(FAIL_EXCHANGE_NAME, true, false);
        return directExchange;
    }

    /**
     * 绑定关系
     *
     * @return
     */
    @Bean
    public Binding failBinding() {
        return BindingBuilder.bind(deadQueue()).to(deadExchange()).with(FAIL_ROUTING_KEY);
    }

    @Bean
    public MessagePostProcessor correlationIdProcessor() {
        MessagePostProcessor messagePostProcessor = new MessagePostProcessor() {
            @Override
            public Message postProcessMessage(Message message, Correlation correlation) {
                MessageProperties messageProperties = message.getMessageProperties();

                if (correlation instanceof CorrelationData) {
                    String correlationId = ((CorrelationData) correlation).getId();
                    messageProperties.setCorrelationId(correlationId);
                }
                // 可以设置持久化，但与本文无关，因此没有附上
                return message;
            }

            @Override
            public Message postProcessMessage(Message message) throws AmqpException {
                return message;
            }
        };
        return messagePostProcessor;
    }
}
