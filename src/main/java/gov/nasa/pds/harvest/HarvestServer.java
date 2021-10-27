package gov.nasa.pds.harvest;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import gov.nasa.pds.harvest.cfg.Configuration;
import gov.nasa.pds.harvest.cfg.ConfigurationReader;
import gov.nasa.pds.harvest.cfg.IPAddress;
import gov.nasa.pds.harvest.http.StatusServlet;
import gov.nasa.pds.harvest.mq.FileMessageConsumer;
import gov.nasa.pds.harvest.util.CloseUtils;
import gov.nasa.pds.harvest.util.ExceptionUtils;
import gov.nasa.pds.harvest.util.ThreadUtils;


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
        
        FileMessageConsumer consumer = new FileMessageConsumer(channel);
        channel.basicConsume(Constants.MQ_FILES, false, consumer);

        log.info("Started file consumer");
    }
    
    
    /**
     * Start embedded web server
     * @param port a port to listen for incoming connections
     */
    private void startWebServer(int port) throws Exception
    {
        Server server = new Server();
        
        // HTTP connector
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("0.0.0.0");
        connector.setPort(port);
        server.addConnector(connector);

        // Servlet handler
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping(StatusServlet.class, "/*");
        server.setHandler(handler);
        
        // Start web server
        server.start();
        
        log.info("Started web server on port " + port);
    }

    
    /**
     * Connect to RabbitMQ server. Wait until RabbitMQ is up. 
     */
    private void connectToRabbitMQ()
    {
        List<Address> rmqAddr = new ArrayList<>();
        for(IPAddress ipa: cfg.mqAddresses)
        {
            rmqAddr.add(new Address(ipa.getHost(), ipa.getPort()));
        }
        
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
                ThreadUtils.sleepSec(10);
            }
        }

        log.info("Connected to RabbitMQ");
    }
        
}
