package gov.nasa.pds.harvest.cfg;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.util.CloseUtils;


public class ConfigurationReader
{
    private static final String PROP_MQ_HOST = "mq.host";
    private static final String PROP_WEB_PORT = "web.port";
    
    private static final String DEFAULT_MQ_HOST = "localhost:5672";
    private static final String DEFAULT_WEB_PORT = "8002";
    
    private Logger log;
    
    
    public ConfigurationReader()
    {
        log = LogManager.getLogger(this.getClass());
    }

    
    public Configuration read(File file) throws Exception
    {
        Configuration cfg = new Configuration();
        
        Reader rd = null;
        try
        {
            // Read properties from a file 
            Properties props = new Properties();
            rd = new FileReader(file);
            props.load(rd);
            
            // Parse web port
            cfg.webPort = parseWebPort(props);
            
            // Parse message queue addresses
            cfg.mqAddresses = parseMQAddresses(props);
        }
        finally
        {
            CloseUtils.close(rd);
        }
                
        return cfg;
    }

    
    private int parseWebPort(Properties props) throws Exception
    {
        String tmp = props.getProperty(PROP_WEB_PORT);
        if(tmp == null)
        {
            tmp = DEFAULT_WEB_PORT;
            String msg = String.format("'%s' property is not set. Will use default value: %s", PROP_WEB_PORT, tmp);
            log.warn(msg);
        }

        try
        {
            return Integer.parseInt(tmp);
        }
        catch(Exception ex)
        {
            String msg = String.format("Could not parse '%s' property %s", PROP_WEB_PORT, tmp);
            throw new Exception(msg);
        }
    }
    
    
    private List<IPAddress> parseMQAddresses(Properties props) throws Exception
    {
        String tmp = props.getProperty(PROP_MQ_HOST);
        if(tmp == null)
        {
            tmp = DEFAULT_MQ_HOST;
            String msg = String.format("'%s' property is not set. Will use default value: %s", PROP_MQ_HOST, tmp);
            log.warn(msg);
        }
        
        List<IPAddress> list = new ArrayList<>();
        
        StringTokenizer tkz = new StringTokenizer(tmp, ",;");
        while(tkz.hasMoreTokens())
        {
            String item = tkz.nextToken();
            if(item == null) continue;
            item = item.trim();
            
            String[] tokens = item.split(":");
            if(tokens.length != 2) throw new Exception("Invalid host entry: '" + item + "'. Expected 'host:port' value.");
            
            String host = tokens[0];
            int port = 0;
            
            try
            {
                port = Integer.parseInt(tokens[1]);
            }
            catch(Exception ex)
            {
                throw new Exception("Invalid host entry: '" + item + "'. Could not parse port.");
            }
            
            list.add(new IPAddress(host, port));
        }
        
        return list;
    }
}
