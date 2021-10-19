package gov.nasa.pds.harvest.cfg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.util.CloseUtils;


public class ConfigurationReader
{
    private static final String PROP_MQ_HOST = "mq.host";
    private static final String PROP_WEB_PORT = "web.port";
    
    private static final String PROP_ES_URL = "es.url";
    private static final String PROP_ES_INDEX = "es.index";
    private static final String PROP_ES_AUTH = "es.authFile";
        
    private static final IPAddress DEFAULT_MQ_HOST = new IPAddress("localhost", 5672);
    private static final int DEFAULT_WEB_PORT = 8002;
    
    private Logger log;

    
    public ConfigurationReader()
    {
        log = LogManager.getLogger(this.getClass());
    }


    public Configuration read(File file) throws Exception
    {
        Configuration cfg = parseConfigFile(file);
    
        // Validate web port
        if(cfg.webPort == 0)
        {
            cfg.webPort = DEFAULT_WEB_PORT;
            String msg = String.format("'%s' property is not set. Will use default value: %d", PROP_WEB_PORT, cfg.webPort);
            log.warn(msg);
        }
        
        // Validate MQ address
        if(cfg.mqAddresses.isEmpty())
        {
            cfg.mqAddresses.add(DEFAULT_MQ_HOST);
            String msg = String.format("'%s' property is not set. Will use default value: %s", 
                    PROP_WEB_PORT, DEFAULT_MQ_HOST.toString());
            log.warn(msg);
        }
        
        return cfg;
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
                case PROP_WEB_PORT:
                    cfg.webPort = parseWebPort(value);
                    break;
                case PROP_MQ_HOST:
                    cfg.mqAddresses.add(parseMQAddresses(value));
                    break;
                case PROP_ES_URL:
                    cfg.registryCfg.url = value;
                    break;
                case PROP_ES_INDEX:
                    cfg.registryCfg.indexName = value;
                    break;
                case PROP_ES_AUTH:
                    cfg.registryCfg.authFile = value;
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
            String msg = String.format("Invalid '%s' property: '%s'. Expected 'host:port' value.", PROP_MQ_HOST, str);
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
            String msg = String.format("Invalid port in '%s' property: '%s'", PROP_MQ_HOST, str);
            throw new Exception(msg);
        }
            
        return new IPAddress(host, port);
    }
}
