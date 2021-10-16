package gov.nasa.pds.harvest.mq;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import gov.nasa.pds.harvest.mq.msg.FilesMessage;


public class FilesConsumer extends DefaultConsumer
{
    private Logger log;
    private Gson gson;
    
    
    public FilesConsumer(Channel channel)
    {
        super(channel);
        log = LogManager.getLogger(this.getClass());
        
        gson = new Gson();
    }

    
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, 
            AMQP.BasicProperties properties, byte[] body) throws IOException
    {
        long deliveryTag = envelope.getDeliveryTag();
        
        String jsonStr = new String(body);
        FilesMessage msg = gson.fromJson(jsonStr, FilesMessage.class);
        
        processMessage(msg);
        
        // ACK message (delete from the queue)
        getChannel().basicAck(deliveryTag, false);        
    }
    
    
    private void processMessage(FilesMessage msg) throws IOException
    {
        log.info("Processing message " + msg.jobId);
        
    }
}
