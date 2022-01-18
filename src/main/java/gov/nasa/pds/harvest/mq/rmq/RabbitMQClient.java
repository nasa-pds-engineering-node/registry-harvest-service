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
import gov.nasa.pds.harvest.mq.CollectionInventoryConsumer;
import gov.nasa.pds.harvest.mq.MQClient;
import gov.nasa.pds.harvest.mq.ManagerCommandConsumer;
import gov.nasa.pds.harvest.mq.ProductConsumer;
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
    private ConsumerFactory consumerFactory;
    
    private ConnectionFactory rmqConnectionFactory;
    private Connection rmqConnection;
    private String rmqConnectionInfo;
    

    /**
     * Constructor
     * @param cfg RabbitMQ configuration
     */
    public RabbitMQClient(RabbitMQCfg cfg, ConsumerFactory consumerFactory)
    {
        // Get logger
        log = LogManager.getLogger(this.getClass());
        
        this.consumerFactory = consumerFactory;
        
        // Validate and store configuration
        if(cfg == null || cfg.addresses == null || cfg.addresses.isEmpty()) 
        {
            throw new IllegalArgumentException("RabbitMQ address is not set.");
        }
        
        this.cfg = cfg;

        // Create connection factory
        rmqConnectionFactory = new ConnectionFactory();
        rmqConnectionFactory.setAutomaticRecoveryEnabled(true);
        
        if(cfg.userName != null)
        {
            rmqConnectionFactory.setUsername(cfg.userName);
            rmqConnectionFactory.setPassword(cfg.password);
        }
        
        // Build connection info string
        StringBuilder bld = new StringBuilder();
        for(int i = 0; i < cfg.addresses.size(); i++)
        {
            if(i != 0) bld.append(", ");
            IPAddress ipa = cfg.addresses.get(i);
            bld.append(ipa.getHost() + ":" + ipa.getPort());
        }
        
        this.rmqConnectionInfo = bld.toString();        
    }

    
    @Override
    public String getType()
    {
        return "RabbitMQ";
    }

    
    @Override
    public String getConnectionInfo()
    {
        return rmqConnectionInfo;
    }

    
    @Override
    public boolean isConnected()
    {
        if(rmqConnection == null) 
        {
            return false;
        }
        else
        {
            return rmqConnection.isOpen();
        }
    }

    
    @Override
    public void run() throws Exception
    {
        // Connect to RabbitMQ (wait until RabbitMQ is up)
        connect();

        // Start product consumer
        ProductConsumerRabbitMQ productConsumer = createProductConsumer();
        productConsumer.start();
        log.info("Started product consumer");
        
        // Start Collection inventory consumer
        CollectionInventoryConsumerRabbitMQ inventoryConsumer = createCollectionInventoryConsumer();
        inventoryConsumer.start();
        log.info("Started collection inventory consumer");

        // Start Manager command consumer
        ManagerCommandConsumerRabbitMQ managerConsumer = createManagerCommandConsumer();
        managerConsumer.start();
        log.info("Started manager command consumer");
    }

    
    /**
     * Connect to RabbitMQ server. Wait until RabbitMQ is up. 
     */
    public void connect()
    {
        if(rmqConnection != null) return;
        
        log.info("Connecting to RabbitMQ at " + rmqConnectionInfo);
        
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
                rmqConnection = rmqConnectionFactory.newConnection(rmqAddr);
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
        CloseUtils.close(rmqConnection);    
    }
    
    
    private ProductConsumerRabbitMQ createProductConsumer() throws Exception
    {
        Channel channel = rmqConnection.createChannel();
        channel.basicQos(1);
        
        ProductConsumer genericConsumer = consumerFactory.createProductConsumer();
        ProductConsumerRabbitMQ consumer = new ProductConsumerRabbitMQ(channel, genericConsumer);
        return consumer;
    }

    
    private CollectionInventoryConsumerRabbitMQ createCollectionInventoryConsumer() throws Exception
    {
        Channel channel = rmqConnection.createChannel();
        channel.basicQos(1);
        
        CollectionInventoryConsumer genericConsumer = consumerFactory.createCollectionInventoryConsumer();
        CollectionInventoryConsumerRabbitMQ consumer = new CollectionInventoryConsumerRabbitMQ(channel, genericConsumer);
        return consumer;
    }

    
    private ManagerCommandConsumerRabbitMQ createManagerCommandConsumer() throws Exception
    {
        Channel channel = rmqConnection.createChannel();
        channel.basicQos(1);
        
        ManagerCommandConsumer genericConsumer = consumerFactory.createManagerCommandConsumer();
        ManagerCommandConsumerRabbitMQ consumer = new ManagerCommandConsumerRabbitMQ(channel, genericConsumer);
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
