package gov.nasa.pds.harvest.cfg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.util.CloseUtils;


/**
 * Reads Harvest server configuration file.
 * @author karpenko
 */
public class ConfigurationReader
{
    // Message server / queue type
    private static final String PROP_MQ_TYPE = "mq.type";
    
    // RabbitMQ
    private static final String PROP_RMQ_HOST = "rmq.host";
    private static final String PROP_RMQ_USER = "rmq.user";
    private static final String PROP_RMQ_PASS = "rmq.password";
    private static final IPAddress DEFAULT_RMQ_HOST = new IPAddress("localhost", 5672);

    // ActiveMQ
    private static final String PROP_AMQ_URL = "amq.url";
    private static final String PROP_AMQ_USER = "amq.user";
    private static final String PROP_AMQ_PASS = "amq.password";
    private static final String DEFAULT_AMQ_URL = "tcp://localhost:61616";

    // Embedded web server
    private static final String PROP_WEB_PORT = "web.port";
    private static final int DEFAULT_WEB_PORT = 8005;
    
    // Registry
    private static final String PROP_ES_URL = "es.url";
    private static final String PROP_ES_INDEX = "es.index";
    private static final String PROP_ES_AUTH = "es.authFile";
    
    // Harvest
    private static final String PROP_HARVEST_STORE_LABELS = "harvest.storeLabels";
    private static final String PROP_HARVEST_STORE_JSON_LABELS = "harvest.storeJsonLabels";
    private static final String PROP_HARVEST_PROCESS_DATA_FILES = "harvest.processDataFiles";
        
    private Logger log;

    
    /**
     * Constructor
     */
    public ConfigurationReader()
    {
        log = LogManager.getLogger(this.getClass());
    }

    
    /**
     * Read Harvest configuration file
     * @param file Harvest server configuration file
     * @return configuration object
     * @throws Exception an exception
     */
    public Configuration read(File file) throws Exception
    {
        Configuration cfg = parseConfigFile(file);
        validate(cfg);
        
        return cfg;
    }
    
    /**
     * Validate configuration
     * @param cfg configuration object
     * @throws Exception an exception
     */
    private void validate(Configuration cfg) throws Exception
    {
        // Validate Message queue / server
        if(cfg.mqType == null)
        {
            String msg = String.format("Invalid configuration. Property '%s' is not set.", PROP_MQ_TYPE);
            throw new Exception(msg);
        }
        
        switch(cfg.mqType)
        {
        case ActiveMQ:
            validateAMQ(cfg.amqCfg);
            break;
        case RabbitMQ:
            validateRMQ(cfg.rmqCfg);
            break;
        }
        
        // Validate embedded web server
        validateWeb(cfg);
        
        // Validate Registry / Elasticsearch
        validateRegistry(cfg.registryCfg);
    }
    
    
    private void validateWeb(Configuration cfg)
    {
        if(cfg.webPort == 0)
        {
            cfg.webPort = DEFAULT_WEB_PORT;
            String msg = String.format("'%s' property is not set. Will use default value: %d", PROP_WEB_PORT, cfg.webPort);
            log.warn(msg);
        }
    }
    
    
    private void validateRMQ(RabbitMQCfg cfg) throws Exception
    {
        // Validate MQ address
        if(cfg.addresses.isEmpty())
        {
            cfg.addresses.add(DEFAULT_RMQ_HOST);
            String msg = String.format("'%s' property is not set. Will use default value: %s", 
                    PROP_RMQ_HOST, DEFAULT_RMQ_HOST.toString());
            log.warn(msg);
        }
    }

    
    private void validateAMQ(ActiveMQCfg cfg) throws Exception
    {
        if(cfg.url == null || cfg.url.isBlank())
        {
            cfg.url = DEFAULT_AMQ_URL;
            String msg = String.format("'%s' property is not set. Will use default value: %s", 
                    PROP_AMQ_URL, cfg.url);
            log.warn(msg);
        }
    }

    
    private void validateRegistry(RegistryCfg cfg)
    {
        if(cfg.url == null)
        {
            cfg.url = "http://localhost:9200";
            String msg = String.format("'%s' property is not set. Will use default value: %s", 
                    PROP_ES_URL, cfg.url);
            log.warn(msg);
        }
        
        if(cfg.indexName == null)
        {
            cfg.indexName = "registry";
            String msg = String.format("'%s' property is not set. Will use default value: %s", 
                    PROP_ES_INDEX, cfg.indexName);
            log.warn(msg);
        }
        
    }
    
    
    private Configuration parseConfigFile(File file) throws Exception
    {
        Configuration cfg = new Configuration();
        
        BufferedReader rd = null;
        try
        {
            rd = new BufferedReader(new FileReader(file));
            String line;
            while((line = rd.readLine()) != null)
            {
                line = line.trim();
                if(line.startsWith("#") || line.isEmpty()) continue;
                
                String[] tokens = line.split("=");
                if(tokens.length != 2) throw new Exception("Invalid property line: " + line);
                String key = tokens[0].trim();
                String value = tokens[1].trim();
                
                switch(key)
                {
                // Embedded web server
                case PROP_WEB_PORT:
                    cfg.webPort = parseWebPort(value);
                    break;

                // MQ type
                case PROP_MQ_TYPE:
                    cfg.mqType = parseMQType(value);
                    break;
                    
                // RabbitMQ
                case PROP_RMQ_HOST:
                    cfg.rmqCfg.addresses.add(parseMQAddresses(value));
                    break;
                case PROP_RMQ_USER:
                    cfg.rmqCfg.userName = value;
                    break;
                case PROP_RMQ_PASS:
                    cfg.rmqCfg.password = value;
                    break;

                // ActiveMQ
                case PROP_AMQ_URL:
                    cfg.amqCfg.url = value;
                    break;
                case PROP_AMQ_USER:
                    cfg.amqCfg.userName = value;
                    break;
                case PROP_AMQ_PASS:
                    cfg.amqCfg.password = value;
                    break;
                    
                // Registry / Elasticsearch
                case PROP_ES_URL:
                    cfg.registryCfg.url = value;
                    break;
                case PROP_ES_INDEX:
                    cfg.registryCfg.indexName = value;
                    break;
                case PROP_ES_AUTH:
                    cfg.registryCfg.authFile = value;
                    break;
                    
                // Harvest
                case PROP_HARVEST_PROCESS_DATA_FILES:
                    cfg.harvestCfg.processDataFiles = parseBoolean(PROP_HARVEST_PROCESS_DATA_FILES, value, true);
                    break;
                case PROP_HARVEST_STORE_JSON_LABELS:
                    cfg.harvestCfg.storeJsonLabels = parseBoolean(PROP_HARVEST_STORE_JSON_LABELS, value, true);
                    break;
                case PROP_HARVEST_STORE_LABELS:
                    cfg.harvestCfg.storeLabels = parseBoolean(PROP_HARVEST_STORE_LABELS, value, true);
                    break;

                default:
                    throw new Exception("Invalid property '" + key + "'");
                }
            }
        }
        finally
        {
            CloseUtils.close(rd);
        }
        
        return cfg;
    }

    
    private MQType parseMQType(String str) throws Exception
    {
        if("ActiveMQ".equalsIgnoreCase(str)) return MQType.ActiveMQ;
        if("RabbitMQ".equalsIgnoreCase(str)) return MQType.RabbitMQ;
        
        String msg = String.format("Invalid '%s' property value: '%s'. Expected 'ActiveMQ' or 'RabbitMQ'.", 
                PROP_MQ_TYPE, str);
        throw new Exception(msg);
    }

    
    private int parseWebPort(String port) throws Exception
    {
        try
        {
            return Integer.parseInt(port);
        }
        catch(Exception ex)
        {
            String msg = String.format("Could not parse '%s' property: '%s'", PROP_WEB_PORT, port);
            throw new Exception(msg);
        }
    }
    
    
    private IPAddress parseMQAddresses(String str) throws Exception
    {
        String[] tokens = str.split(":");
        if(tokens.length != 2) 
        {
            String msg = String.format("Invalid '%s' property value: '%s'. Expected 'host:port'.", PROP_RMQ_HOST, str);
            throw new Exception(msg);
        }
        
        String host = tokens[0];
        int port = 0;
        
        try
        {
            port = Integer.parseInt(tokens[1]);
        }
        catch(Exception ex)
        {
            String msg = String.format("Invalid port in '%s' property: '%s'", PROP_RMQ_HOST, str);
            throw new Exception(msg);
        }
            
        return new IPAddress(host, port);
    }
    
    
    private boolean parseBoolean(String property, String value, boolean defaultValue)
    {
        if(value == null) return defaultValue;
        if(value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes")) return true;
        if(value.equalsIgnoreCase("false") || value.equalsIgnoreCase("no")) return false;
        
        log.warn("Property '" + property + "' has invalid boolean value '" + value + "'. Will use " + defaultValue);
        return defaultValue;
    }
}
