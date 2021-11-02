package gov.nasa.pds.harvest.mq.rmq;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import gov.nasa.pds.harvest.cfg.IPAddress;
import gov.nasa.pds.harvest.cfg.RabbitMQCfg;
import gov.nasa.pds.harvest.mq.MQClient;
import gov.nasa.pds.harvest.util.CloseUtils;
import gov.nasa.pds.harvest.util.ExceptionUtils;


/**
 * RabbitMQ client
 * @author karpenko
 */
public class RabbitMQClient implements MQClient
{
    private Logger log;
    private RabbitMQCfg cfg;
    
    private ConnectionFactory factory;
    private Connection connection;
    
    private String connectionInfo;
    

    /**
     * Constructor
     * @param cfg RabbitMQ configuration
     */
    public RabbitMQClient(RabbitMQCfg cfg)
    {
        // Get logger
        log = LogManager.getLogger(this.getClass());
        
        // Validate and store configuration
        if(cfg == null || cfg.addresses == null || cfg.addresses.isEmpty()) 
        {
            throw new IllegalArgumentException("RabbitMQ address is not set.");
        }
        
        this.cfg = cfg;

        // Create connection factory
        factory = new ConnectionFactory();
        factory.setAutomaticRecoveryEnabled(true);
        
        if(cfg.userName != null)
        {
            factory.setUsername(cfg.userName);
            factory.setPassword(cfg.password);
        }
        
        // Build connection info string
        StringBuilder bld = new StringBuilder();
        for(int i = 0; i < cfg.addresses.size(); i++)
        {
            if(i != 0) bld.append(", ");
            IPAddress ipa = cfg.addresses.get(i);
            bld.append(ipa.getHost() + ":" + ipa.getPort());
        }
        
        this.connectionInfo = bld.toString();        
    }

    
    @Override
    public String getType()
    {
        return "RabbitMQ";
    }

    
    @Override
    public String getConnectionInfo()
    {
        return connectionInfo;
    }

    
    @Override
    public boolean isConnected()
    {
        if(connection == null) 
        {
            return false;
        }
        else
        {
            return connection.isOpen();
        }
    }

    
    @Override
    public void run() throws Exception
    {
        // Connect to RabbitMQ (wait until RabbitMQ is up)
        connect();

        // Start product Consumer
        ProductConsumerRabbitMQ productConsumer = createProductConsumer();
        productConsumer.start();
        log.info("Started product consumer");
    }

    
    /**
     * Connect to RabbitMQ server. Wait until RabbitMQ is up. 
     */
    public void connect()
    {
        if(connection != null) return;
        
        log.info("Connecting to RabbitMQ at " + connectionInfo);
        
        // Convert configuration model classes to RabbitMQ model classes
        List<Address> rmqAddr = new ArrayList<>();
        for(IPAddress ipa: cfg.addresses)
        {
            rmqAddr.add(new Address(ipa.getHost(), ipa.getPort()));
        }
        
        // Wait for RabbitMQ
        while(true)
        {
            try
            {
                connection = factory.newConnection(rmqAddr);
                break;
            }
            catch(Exception ex)
            {
                String msg = ExceptionUtils.getMessage(ex);
                log.warn("Could not connect to RabbitMQ. " + msg + ". Will retry in 10 sec.");
                sleepSec(10);
            }
        }

        log.info("Connected to RabbitMQ");
    }

    
    public void close()
    {
        CloseUtils.close(connection);    
    }
    
    
    private ProductConsumerRabbitMQ createProductConsumer() throws Exception
    {
        Channel channel = connection.createChannel();
        channel.basicQos(1);
        
        ProductConsumerRabbitMQ consumer = new ProductConsumerRabbitMQ(channel);
        return consumer;
    }
    
    
    private static void sleepSec(int sec)
    {
        try
        {
            Thread.sleep(sec * 1000);
        }
        catch(InterruptedException ex)
        {
            // Ignore
        }
    }

}
