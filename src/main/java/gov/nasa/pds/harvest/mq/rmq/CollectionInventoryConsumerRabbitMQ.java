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
import gov.nasa.pds.harvest.mq.CollectionInventoryConsumer;
import gov.nasa.pds.harvest.mq.msg.CollectionInventoryMessage;


/**
 * A consumer of file messages from a RabbitMQ queue
 * @author karpenko
 */
public class CollectionInventoryConsumerRabbitMQ extends DefaultConsumer
{
    private Logger log;
    private Gson gson;
    
    private CollectionInventoryConsumer collectionInventoryConsumer;
    
    /**
     * Constructor
     * @param channel RabbitMQ connection channel
     */
    public CollectionInventoryConsumerRabbitMQ(Channel channel, CollectionInventoryConsumer consumer)
    {
        super(channel);
        this.collectionInventoryConsumer = consumer;
        
        log = LogManager.getLogger(this.getClass());        
        gson = new Gson();
    }

    
    /**
     * Start consuming messages
     * @throws Exception
     */
    public void start() throws Exception
    {
        getChannel().basicConsume(Constants.MQ_COLLECTION_INVENTORY, false, this);
    }

    
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, 
            AMQP.BasicProperties properties, byte[] body) throws IOException
    {
        long deliveryTag = envelope.getDeliveryTag();
        CollectionInventoryMessage msg = null;
        
        try
        {
            String jsonStr = new String(body);
            msg = gson.fromJson(jsonStr, CollectionInventoryMessage.class);
        }
        catch(Exception ex)
        {
            log.error("Invalid message", ex);

            // ACK message (delete from the queue)
            getChannel().basicAck(deliveryTag, false);
            return;
        }

        if(collectionInventoryConsumer.processMessage(msg))
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
