package gov.nasa.pds.harvest.mq.rmq;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import gov.nasa.pds.harvest.Constants;
import gov.nasa.pds.harvest.dao.RegistryService;
import gov.nasa.pds.harvest.mq.msg.ProductMessage;


/**
 * A consumer of file messages from a RabbitMQ queue
 * @author karpenko
 */
public class ProductConsumerRabbitMQ extends DefaultConsumer
{
    private Logger log;
    private Gson gson;
    
    private RegistryService registry;
    
    
    /**
     * Constructor
     * @param channel RabbitMQ connection channel
     */
    public ProductConsumerRabbitMQ(Channel channel)
    {
        super(channel);
        log = LogManager.getLogger(this.getClass());
        
        gson = new Gson();
        registry = new RegistryService();
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
        log.info("Processing message " + properties.getMessageId());

        String jsonStr = new String(body);
        ProductMessage msg = gson.fromJson(jsonStr, ProductMessage.class);
        
        if(processMessage(msg))
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
    
    
    private boolean processMessage(ProductMessage msg) throws IOException
    {
        if(msg.files == null || msg.files.isEmpty()) return true;
        
        List<String> filesToProcess;
        
        // Process all products overwriting already registered products
        if(msg.overwrite)
        {
            filesToProcess = msg.files; 
        }
        // Only process unregistered products
        else
        {
            filesToProcess = registry.getUnregisteredFiles(msg);
            // There was an error. Reject the message.
            if(filesToProcess == null) return false;
            // All products from this message are already registered. Ack the message.
            if(filesToProcess.isEmpty()) return true;
        }

        // Harvest files
        System.out.println(filesToProcess);
        
        return true;
    }
    
    
}
