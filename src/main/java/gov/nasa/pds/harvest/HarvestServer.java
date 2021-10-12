package gov.nasa.pds.harvest;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import gov.nasa.pds.harvest.util.CloseUtils;


/**
 * Harvest server
 * @author karpenko
 */
public class HarvestServer
{
    private Logger log;
    
    private ConnectionFactory rmqConFactory;
    private Connection rmqConnection;
    private List<Address> rmqAddr;
    

    public HarvestServer(String cfgFilePath)
    {
        log = LogManager.getLogger(this.getClass());
        
        rmqConFactory = new ConnectionFactory();
        rmqConFactory.setAutomaticRecoveryEnabled(true);
        
        rmqAddr = new ArrayList<>();
        rmqAddr.add(new Address("localhost", 5672));
    }
    
    
    public void run()
    {
        connect();
        
        CloseUtils.close(rmqConnection);
    }
    

    private void connect()
    {
        while(true)
        {
            try
            {
                rmqConnection = rmqConFactory.newConnection(rmqAddr);
                break;
            }
            catch(Exception ex)
            {
                log.warn("Could not connect to RabbitMQ. " + ex + ". Will retry in 10 sec.");
                sleepSec(10);
            }
        }

        log.info("Connected to RabbitMQ");
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
