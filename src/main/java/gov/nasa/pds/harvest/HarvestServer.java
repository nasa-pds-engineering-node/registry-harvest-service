package gov.nasa.pds.harvest;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import gov.nasa.pds.harvest.cfg.Configuration;
import gov.nasa.pds.harvest.cfg.ConfigurationReader;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.http.MemoryServlet;
import gov.nasa.pds.harvest.http.StatusServlet;
import gov.nasa.pds.harvest.mq.MQClient;
import gov.nasa.pds.harvest.mq.rmq.ConsumerFactory;
import gov.nasa.pds.harvest.mq.rmq.RabbitMQClient;
import gov.nasa.pds.registry.common.cfg.RegistryCfg;
import gov.nasa.pds.registry.common.util.ExceptionUtils;


/**
 * Harvest server
 * @author karpenko
 */
public class HarvestServer
{
    private Logger log;
    private Configuration cfg;
    
    private MQClient mqClient;

    
    /**
     * Constructor
     * @param cfgFilePath configuration file path
     * @throws Exception an exception
     */
    public HarvestServer(String cfgFilePath) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        // Read configuration file
        File file = new File(cfgFilePath);
        log.info("Reading configuration from " + file.getAbsolutePath());        
        ConfigurationReader cfgReader = new ConfigurationReader();
        cfg = cfgReader.read(file);
        
        mqClient = createMQClient(cfg);
    }

    
    private MQClient createMQClient(Configuration cfg) throws Exception
    {
        if(cfg == null || cfg.mqType == null)
        {
            throw new Exception("Invalid configuration. Message server type is not set.");
        }
        
        ConsumerFactory consumerFactory = new ConsumerFactory(cfg.harvestCfg, cfg.registryCfg);
        
        switch(cfg.mqType)
        {
        case ActiveMQ:
            //return new ActiveMQClient(cfg.amqCfg);
            throw new Exception("ActiveMQ client is not implemented yet.");
        case RabbitMQ:
            return new RabbitMQClient(cfg.rmqCfg, consumerFactory);
        }
        
        throw new Exception("Invalid message server type: " + cfg.mqType);
    }

    
    /**
     * Run the server
     * @return 0 - server started without errors; 1 or greater - there was an error
     */
    public int run()
    {
        try
        {
            // Init registry (elasticsearch) manager
            initRegistry(cfg.registryCfg);
            
            // Start embedded web server
            startWebServer(cfg.webPort);
            
            // Start message queue (ActiveMQ or RabbitMQ) client
            mqClient.run();
        }
        catch(Exception ex)
        {
            log.error(ExceptionUtils.getMessage(ex));
            return 1;
        }
        
        return 0;
    }

    
    private void initRegistry(RegistryCfg cfg) throws Exception
    {
        RegistryManager.init(cfg);
        RegistryManager.getInstance().getFieldNameCache().update();
    }
    
    
    /**
     * Start embedded web server
     * @param port a port to listen for incoming connections
     */
    private void startWebServer(int port) throws Exception
    {
        // Max threads = 10, min threads = 1
        QueuedThreadPool threadPool = new QueuedThreadPool(10, 1);
        Server server = new Server(threadPool);
        
        // HTTP connector
        ServerConnector connector = new ServerConnector(server);
        connector.setHost("0.0.0.0");
        connector.setPort(port);
        server.addConnector(connector);

        // Servlet handler
        ServletHandler handler = new ServletHandler();
        
        // Status servlet
        ServletHolder statusServlet = new ServletHolder(new StatusServlet(cfg, mqClient));
        handler.addServletWithMapping(statusServlet, "/");

        // Memory servlet
        handler.addServletWithMapping(MemoryServlet.class, "/memory");
        server.setHandler(handler);
        
        // Start web server
        server.start();
        
        log.info("Started web server on port " + port);
    }

}
