package gov.nasa.pds.harvest.mq;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.mq.msg.CollectionInventoryMessage;
import gov.nasa.pds.harvest.proc.CollectionInventoryProcessor;
import gov.nasa.pds.registry.common.cfg.RegistryCfg;
import gov.nasa.pds.registry.common.util.ExceptionUtils;

/**
 * Consume collection inventory messages
 * @author karpenko
 */
public class CollectionInventoryConsumer
{
    protected Logger log;
    private CollectionInventoryProcessor proc;
    
    
    /**
     * Constructor
     * @param cfg registry (Elasticsearch) configuration
     * @throws Exception an exception
     */
    public CollectionInventoryConsumer(RegistryCfg cfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        proc = new CollectionInventoryProcessor(cfg);
    }
    
    
    /**
     * Process collection inventory message
     * @param msg collection inventory message
     * @return true if message successfully processed
     */
    public boolean processMessage(CollectionInventoryMessage msg)
    {
        if(msg == null) return true;
        
        if(msg.collectionLidvid == null || msg.collectionLidvid.isBlank())
        {
            return true;
        }
        
        if(msg.inventoryFile == null || msg.inventoryFile.isBlank())
        {
            return true;
        }
        
        File inventoryFile = new File(msg.inventoryFile);
        if(!inventoryFile.exists())
        {
            log.error("Collection inventory file " + msg.inventoryFile + " doesn't exist.");
            return true;
        }
        
        log.info("Processing collection inventory file " + msg.inventoryFile);
        
        try
        {
            proc.writeCollectionInventory(msg.collectionLidvid, inventoryFile, msg.jobId);
            return true;
        }
        catch(Exception ex)
        {
            log.error(ExceptionUtils.getMessage(ex));
            return false;
        }
    }
}
