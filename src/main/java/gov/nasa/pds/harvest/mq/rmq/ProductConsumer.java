package gov.nasa.pds.harvest.mq.rmq;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.cfg.HarvestCfg;
import gov.nasa.pds.harvest.dao.RegistryService;
import gov.nasa.pds.harvest.job.Job;
import gov.nasa.pds.harvest.job.JobFactory;
import gov.nasa.pds.harvest.mq.msg.ProductMessage;
import gov.nasa.pds.harvest.proc.ProductProcessor;
import gov.nasa.pds.harvest.util.ExceptionUtils;
import gov.nasa.pds.harvest.util.out.RegistryDocWriter;


public class ProductConsumer
{
    private Logger log;
    private RegistryService registry;    
    private RegistryDocWriter writer;
    private ProductProcessor proc;
    
    
    public ProductConsumer(HarvestCfg cfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        
        registry = new RegistryService();
        
        writer = new RegistryDocWriter();
        proc = new ProductProcessor(cfg, writer);
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
        Job job = JobFactory.createJob(msg);
        boolean status = harvestFiles(filesToProcess, job);
        
        return status;
    }

    
    private boolean harvestFiles(List<String> filesToProcess, Job job)
    {
        writer.clearData();
        
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
            }
        }
        
        List<String> data = writer.getData();

        System.out.println("************** DATA ***************");
        for(String str: data)
        {
            System.out.println(str);
        }

        return true;
    }
    

}
