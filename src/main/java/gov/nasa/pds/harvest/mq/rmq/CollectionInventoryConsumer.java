package gov.nasa.pds.harvest.mq.rmq;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.mq.msg.CollectionInventoryMessage;
import gov.nasa.pds.harvest.proc.CollectionInventoryProcessor;
import gov.nasa.pds.harvest.util.ExceptionUtils;


public class CollectionInventoryConsumer
{
    protected Logger log;
    private CollectionInventoryProcessor proc;
    
    
    public CollectionInventoryConsumer(RegistryCfg cfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        proc = new CollectionInventoryProcessor(cfg);
    }
    
    
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
