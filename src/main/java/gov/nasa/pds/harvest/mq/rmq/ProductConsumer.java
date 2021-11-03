package gov.nasa.pds.harvest.mq.rmq;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.dao.RegistryService;
import gov.nasa.pds.harvest.mq.msg.ProductMessage;


public class ProductConsumer
{
    private Logger log;
    private RegistryService registry;

    
    public ProductConsumer()
    {
        log = LogManager.getLogger(this.getClass());
        registry = new RegistryService();
    }
    
    
    public boolean processMessage(ProductMessage msg) throws IOException
    {
        if(msg.files == null || msg.files.isEmpty()) return true;

        log.info("Processing batch of " + msg.files.size() + " products: " + msg.files.get(0) + ", ...");
        
        List<String> filesToProcess;
        
        // Process all products overwriting already registered products
        if(msg.overwrite)
        {
            filesToProcess = msg.files; 
        }
        // Only process unregistered products
        else
        {
            filesToProcess = registry.getUnregisteredFiles(msg);
            // There was an error. Reject the message.
            if(filesToProcess == null) return false;
            // All products from this message are already registered. Ack the message.
            if(filesToProcess.isEmpty()) return true;
        }

        // Harvest files
        System.out.println(filesToProcess);
        
        return true;
    }

}
