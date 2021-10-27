package gov.nasa.pds.harvest.proc;

import java.io.File;
import java.io.FileReader;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.dao.RegistryDAO;
import gov.nasa.pds.harvest.dao.RegistryManager;
import gov.nasa.pds.harvest.meta.Metadata;
import gov.nasa.pds.harvest.util.out.InventoryBatchReader;
import gov.nasa.pds.harvest.util.out.ProdRefsBatch;
import gov.nasa.pds.harvest.util.out.RefType;
import gov.nasa.pds.harvest.util.out.RefsDocWriter;
import gov.nasa.pds.harvest.util.out.WriterManager;


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
    
    private int WRITE_BATCH_SIZE = 500;
    private int ELASTIC_BATCH_SIZE = 50;
    
    private ProdRefsBatch batch = new ProdRefsBatch();
    
    
    /**
     * Constructor
     */
    public CollectionInventoryProcessor()
    {
        log = LogManager.getLogger(this.getClass());
    }
    
    
    /**
     * Parse collection inventory file, e.g., "document_collection_inventory.csv",
     * extract primary and secondary references (lidvids) and write extracted data
     * into a JSON or XML file. JSON files can be imported into Elasticsearch by 
     * Registry Manager tool.
     * 
     * @param meta Collection metadata
     * @param inventoryFile Collection inventory file, e.g., "document_collection_inventory.csv"
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    public void writeCollectionInventory(Metadata meta, File inventoryFile, String jobId) throws Exception
    {
        writePrimaryRefs(meta, inventoryFile, jobId);
        writeSecondaryRefs(meta, inventoryFile, jobId);
    }
    
    
    /**
     * Write primary product references
     * @param meta Collection metadata
     * @param inventoryFile Collection inventory file, e.g., "document_collection_inventory.csv"
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    private void writePrimaryRefs(Metadata meta, File inventoryFile, String jobId) throws Exception
    {
        batch.batchNum = 0;
        
        InventoryBatchReader rd = new InventoryBatchReader(new FileReader(inventoryFile), RefType.PRIMARY);
        
        while(true)
        {
            int count = rd.readNextBatch(WRITE_BATCH_SIZE, batch);
            if(count == 0) break;
            
            // Write batch
            RefsDocWriter writer = WriterManager.getInstance().getRefsWriter();
            writer.writeBatch(meta, batch, RefType.PRIMARY, jobId);
            
            if(count < WRITE_BATCH_SIZE) break;
        }
        
        rd.close();
    }

    
    /**
     * Write secondary product references
     * @param meta Collection metadata 
     * @param inventoryFile Collection inventory file, e.g., "document_collection_inventory.csv"
     * @param jobId Harvest job id
     * @throws Exception Generic exception
     */
    private void writeSecondaryRefs(Metadata meta, File inventoryFile, String jobId) throws Exception
    {
        batch.batchNum = 0;
        
        InventoryBatchReader rd = new InventoryBatchReader(new FileReader(inventoryFile), RefType.SECONDARY);
        
        while(true)
        {
            int count = rd.readNextBatch(WRITE_BATCH_SIZE, batch);
            if(count == 0) break;
            
            // Write batch
            RefsDocWriter writer = WriterManager.getInstance().getRefsWriter();
            writer.writeBatch(meta, batch, RefType.SECONDARY, jobId);
            
            if(count < WRITE_BATCH_SIZE) break;
        }
        
        rd.close();
    }
    
}
