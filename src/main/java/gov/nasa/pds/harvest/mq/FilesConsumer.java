package gov.nasa.pds.harvest.mq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import gov.nasa.pds.harvest.dao.RegistryDAO;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.mq.msg.FilesMessage;
import gov.nasa.pds.harvest.proc.FilesProcessor;
import gov.nasa.pds.harvest.util.ThreadUtils;


public class FilesConsumer extends DefaultConsumer
{
    private Logger log;
    private Gson gson;
    
    private FilesProcessor proc;
    
    
    public FilesConsumer(Channel channel)
    {
        super(channel);
        log = LogManager.getLogger(this.getClass());
        
        gson = new Gson();
        
        proc = new FilesProcessor();
    }

    
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, 
            AMQP.BasicProperties properties, byte[] body) throws IOException
    {
        long deliveryTag = envelope.getDeliveryTag();
        
        String jsonStr = new String(body);
        FilesMessage msg = gson.fromJson(jsonStr, FilesMessage.class);
        
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
    
    
    private boolean processMessage(FilesMessage msg) throws IOException
    {
        log.info("Processing message " + msg.jobId);
                
        List<String> files = proc.getFilesToProcess(msg);
        // Reject the message
        if(files == null) return false;
        // Ack the message
        if(files.isEmpty()) return true;
        
        System.out.println(files);
        
        return true;
    }
    
    
}
