package gov.nasa.pds.harvest;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import gov.nasa.pds.harvest.cfg.Configuration;
import gov.nasa.pds.harvest.cfg.ConfigurationReader;
import gov.nasa.pds.harvest.http.StatusHandler;
import gov.nasa.pds.harvest.mq.FilesConsumer;
import gov.nasa.pds.harvest.util.CloseUtils;
import gov.nasa.pds.harvest.util.ExceptionUtils;
import io.undertow.Undertow;


/**
 * Harvest server
 * @author karpenko
 */
public class HarvestServer
{
    private Logger log;
    private Configuration cfg;
    
    private ConnectionFactory rmqConFactory;
    private Connection rmqConnection;
    

    public HarvestServer(String cfgFilePath) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        // Read configuration file
        File file = new File(cfgFilePath);
        log.info("Reading configuration from " + file.getAbsolutePath());        
        ConfigurationReader cfgReader = new ConfigurationReader();
        cfg = cfgReader.read(file);
        
        // Init RabbitMQ connection factory
        rmqConFactory = new ConnectionFactory();
        rmqConFactory.setAutomaticRecoveryEnabled(true);
    }
    
    
    public void run()
    {
        connectToRabbitMQ();
        
        try
        {
            startFileConsumer();
            startWebServer(cfg.webPort);
        }
        catch(Exception ex)
        {
            log.error(ExceptionUtils.getMessage(ex));
            CloseUtils.close(rmqConnection);
        }
    }

    
    private void startFileConsumer() throws Exception
    {
        Channel channel = rmqConnection.createChannel();
        channel.basicQos(1);
        
        FilesConsumer consumer = new FilesConsumer(channel);
        channel.basicConsume(Constants.MQ_FILES, false, consumer);

        log.info("Started file consumer");
    }
    
    
    private void startWebServer(int port)
    {
        Undertow.Builder bld = Undertow.builder();
        bld.addHttpListener(port, "0.0.0.0");
        bld.setHandler(new StatusHandler());

        Undertow server = bld.build();
        server.start();
        
        log.info("Started web server on port " + port);
    }


    private void connectToRabbitMQ()
    {
        List<Address> rmqAddr = new ArrayList<>();
        rmqAddr.add(new Address("localhost", 5672));
        
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
