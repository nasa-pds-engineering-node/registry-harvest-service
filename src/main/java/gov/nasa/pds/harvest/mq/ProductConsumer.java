package gov.nasa.pds.harvest.mq;

import java.io.File;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.cfg.HarvestCfg;
import gov.nasa.pds.harvest.dao.RegistryService;
import gov.nasa.pds.harvest.job.Job;
import gov.nasa.pds.harvest.job.JobFactory;
import gov.nasa.pds.harvest.mq.msg.ProductMessage;
import gov.nasa.pds.harvest.proc.ProductProcessor;
import gov.nasa.pds.registry.common.cfg.RegistryCfg;
import gov.nasa.pds.registry.common.es.dao.DataLoader;
import gov.nasa.pds.registry.common.util.ExceptionUtils;
import gov.nasa.pds.registry.common.util.doc.RegistryDocWriter;


/**
 * Generic product message consumer. 
 * It doesn't have any message server-specific code (RabbitMQ or ActiveMQ).
 * @author karpenko
 */
public class ProductConsumer
{
    private Logger log;
    private RegistryService registry;
    
    private RegistryDocWriter registryDocWriter;
    private ProductProcessor proc;
    private DataLoader dataLoader;

    
    /**
     * Constructor
     * @param harvestCfg Harvest server configuration
     * @param registryCfg Registry / Elasticsearch configuration
     * @throws Exception an exception
     */
    public ProductConsumer(HarvestCfg harvestCfg, RegistryCfg registryCfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        registry = new RegistryService();
        
        registryDocWriter = new RegistryDocWriter();
        proc = new ProductProcessor(harvestCfg, registryDocWriter);
        
        dataLoader = new DataLoader(registryCfg.url, registryCfg.indexName, registryCfg.authFile);
    }
    
    
    /**
     * Process product message
     * @param msg product message
     * @return true is the message was processed. False if the message could not be processed.
     */
    public boolean processMessage(ProductMessage msg)
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
        Job job = JobFactory.createJob(msg);
        boolean status = harvestFiles(filesToProcess, job);
        
        return status;
    }

    
    private boolean harvestFiles(List<String> filesToProcess, Job job)
    {
        // Clear cached batch of Elasticsearch JSON documents
        registryDocWriter.clearData();
        
        // Add Elasticsearch JSON documents to the batch
        for(String strFile: filesToProcess)
        {
            File file = new File(strFile);
            
            try
            {
                proc.processFile(file, job);
            }
            catch(Exception ex)
            {
                log.error("Could not process file " + file.getAbsolutePath() + ": " + ExceptionUtils.getMessage(ex));
                // Ignore this file
            }
        }
                
        // Load the data into Elasticsearch
        try
        {
            log.info("Loading data.");
            List<String> data = registryDocWriter.getData();
            int numRecords = dataLoader.loadBatch(data);
            log.info("Loaded " + numRecords + " products.");
        }
        catch(Exception ex)
        {
            log.error("Could not load data into Elasticsearch." + ExceptionUtils.getMessage(ex));
            return false;
        }
        
        return true;
    }
    
}
