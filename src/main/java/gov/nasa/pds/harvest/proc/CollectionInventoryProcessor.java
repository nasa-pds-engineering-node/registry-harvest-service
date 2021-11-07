package gov.nasa.pds.harvest.proc;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.dao.DataLoader;
import gov.nasa.pds.harvest.util.out.InventoryBatchReader;
import gov.nasa.pds.harvest.util.out.ProdRefsBatch;
import gov.nasa.pds.harvest.util.out.RefType;
import gov.nasa.pds.registry.common.util.CloseUtils;
import gov.nasa.pds.harvest.util.out.InventoryDocWriter;


/**
 * <p>Process inventory files of "Product_Collection" products (PDS4 label files)
 * 
 * <p>Parse collection inventory file, e.g., "document_collection_inventory.csv",
 * extract primary and secondary references (lidvids) and write extracted data
 * into a JSON or XML file. JSON files can be imported into Elasticsearch by 
 * Registry Manager tool.
 * 
 * <p>This class also uses "RefsCache" singleton to cache product ids (lidvids).
 * 
 * @author karpenko
 */
public class CollectionInventoryProcessor
{
    protected Logger log;
    
    private int REF_BATCH_SIZE = 500;
    private int ES_DOC_BATCH_SIZE = 10;
    
    private ProdRefsBatch batch = new ProdRefsBatch();
    private InventoryDocWriter writer = new InventoryDocWriter();
    
    private DataLoader loader;
    
    
    /**
     * Constructor
     */
    public CollectionInventoryProcessor(RegistryCfg cfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        loader = new DataLoader(cfg);
    }
    
    
    /**
     * Parse collection inventory file, e.g., "document_collection_inventory.csv",
     * extract primary and secondary references (lidvids) and write extracted data
     * into a JSON or XML file. JSON files can be imported into Elasticsearch by 
     * Registry Manager tool.
     * 
     * @param collectionLidVid Collection LIDVID
     * @param inventoryFile Collection inventory file, e.g., "document_collection_inventory.csv"
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    public void writeCollectionInventory(String collectionLidVid, File inventoryFile, String jobId) throws Exception
    {
        writeRefs(collectionLidVid, inventoryFile, jobId, RefType.PRIMARY);
        writeRefs(collectionLidVid, inventoryFile, jobId, RefType.PRIMARY);
    }
    
    
    /**
     * Write primary product references
     * @param collectionLidVid Collection LIDVID
     * @param inventoryFile Collection inventory file, e.g., "document_collection_inventory.csv"
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    private void writeRefs(String collectionLidVid, File inventoryFile, String jobId, RefType refType) throws Exception
    {
        batch.batchNum = 0;
        writer.clearData();
        
        InventoryBatchReader rd = null;
        
        try
        {
            rd = new InventoryBatchReader(new FileReader(inventoryFile), refType);
            
            while(true)
            {
                int count = rd.readNextBatch(REF_BATCH_SIZE, batch);
                if(count == 0) break;
                
                writer.writeBatch(collectionLidVid, batch, refType, jobId);
                if(batch.batchNum % ES_DOC_BATCH_SIZE == 0)
                {
                    List<String> data = writer.getData();
                    loader.loadBatch(data);
                    writer.clearData();
                }
                
                if(count < REF_BATCH_SIZE) break;
            }
    
            // Load last page if size > 0
            List<String> data = writer.getData();
            loader.loadBatch(data);
        }
        finally
        {
            CloseUtils.close(rd);
        }
    }

}
