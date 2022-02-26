package gov.nasa.pds.harvest.mq.rmq;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import gov.nasa.pds.harvest.Constants;
import gov.nasa.pds.harvest.mq.ProductConsumer;
import gov.nasa.pds.registry.common.mq.msg.ProductMessage;


/**
 * A consumer of file messages from a RabbitMQ queue
 * @author karpenko
 */
public class ProductConsumerRabbitMQ extends DefaultConsumer
{
    private Logger log;
    private Gson gson;
    
    private ProductConsumer prodConsumer;
    
    /**
     * Constructor
     * @param channel RabbitMQ connection channel
     */
    public ProductConsumerRabbitMQ(Channel channel, ProductConsumer prodConsumer)
    {
        super(channel);
        this.prodConsumer = prodConsumer;
        
        log = LogManager.getLogger(this.getClass());        
        gson = new Gson();
    }

    
    /**
     * Start consuming messages
     * @throws Exception
     */
    public void start() throws Exception
    {
        getChannel().basicConsume(Constants.MQ_PRODUCTS, false, this);
    }

    
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, 
            AMQP.BasicProperties properties, byte[] body) throws IOException
    {
        long deliveryTag = envelope.getDeliveryTag();
        ProductMessage msg = null;
        
        try
        {
            String jsonStr = new String(body);
            msg = gson.fromJson(jsonStr, ProductMessage.class);            
        }
        catch(Exception ex)
        {
            log.error("Invalid message", ex);

            // ACK message (delete from the queue)
            getChannel().basicAck(deliveryTag, false);
            return;
        }

        if(prodConsumer.processMessage(msg))
        {
            // ACK message (delete from the queue)
            getChannel().basicAck(deliveryTag, false);
        }
        else
        {
            // Reject and requeue
            getChannel().basicReject(deliveryTag, true);
        }
    }
    
    
}
