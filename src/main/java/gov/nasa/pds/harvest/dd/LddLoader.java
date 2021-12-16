package gov.nasa.pds.harvest.dd;

import java.io.File;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.dao.DataLoader;
import gov.nasa.pds.harvest.dd.parser.AttributeDictionaryParser;
import gov.nasa.pds.harvest.dd.parser.ClassAttrAssociationParser;
import gov.nasa.pds.harvest.dd.parser.DDAttribute;


/**
 * Loads PDS LDD JSON file into Elasticsearch data dictionary index
 * 
 * @author karpenko
 */
public class LddLoader
{
    private Logger log;

    private Pds2EsDataTypeMap dtMap;
    private DataLoader loader;
    
    
    /**
     * Constructor
     * @param esUrl Elasticsearch URL
     * @param esIndex Elasticsearch index name
     * @param esAuthFile Elasticsearch authentication configuration file
     * @throws Exception an exception
     */
    public LddLoader(String esUrl, String indexName, String authFilePath) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        dtMap = new Pds2EsDataTypeMap();
        
        loader = new DataLoader(esUrl, indexName + "-dd", authFilePath);
    }
 
    
    /**
     * Load PDS to Elasticsearch data type map
     * @param file configuration file
     * @throws Exception an exception
     */
    public void loadPds2EsDataTypeMap(File file) throws Exception
    {
        dtMap.load(file);
    }
    
    
    /**
     * Load PDS LDD JSON file into Elasticsearch data dictionary index
     * @param lddFile PDS LDD JSON file
     * @param namespace Namespace filter. Only load classes having this namespace.
     * @throws Exception an exception
     */
    public void load(File lddFile, String lddFileName, String namespace, Instant lastDate) throws Exception
    {
        File tempEsDataFile = File.createTempFile("es-", ".json");
        log.info("Creating temporary ES data file " + tempEsDataFile.getAbsolutePath());

        try
        {
            createEsDataFile(lddFile, lddFileName, namespace, tempEsDataFile, lastDate);
            loader.loadFile(tempEsDataFile);
        }
        finally
        {
            // Delete temporary file
            tempEsDataFile.delete();
        }
    }

    
    private static class CaaCallback implements ClassAttrAssociationParser.Callback
    {
        private LddEsJsonWriter writer;
        
        public CaaCallback(LddEsJsonWriter writer)
        {
            this.writer = writer;
        }

        @Override
        public void onAssociation(String classNs, String className, String attrId) throws Exception
        {
            writer.writeFieldDefinition(classNs, className, attrId);
        }
    }

    
    /**
     * Create Elasticsearch data file to be loaded into data dictionary index.
     * @param lddFile PDS LDD JSON file
     * @param namespace Namespace filter. Only load classes having this namespace.
     * @param tempEsFile Write to this Elasticsearch file
     * @throws Exception an exception
     */
    private void createEsDataFile(File lddFile, String lddFileName, String namespace, 
            File tempEsFile, Instant lastDate) throws Exception
    {
        // Parse and cache LDD attributes
        Map<String, DDAttribute> ddAttrCache = new TreeMap<>();
        AttributeDictionaryParser.Callback acb = (attr) -> { ddAttrCache.put(attr.id, attr); }; 
        AttributeDictionaryParser attrParser = new AttributeDictionaryParser(lddFile, acb);
        attrParser.parse();
        
        // If this LDD date is after the last stored in Elasticsearch, overwrite old records
        boolean overwrite = overwriteLdd(lastDate, attrParser.getLddDate());
        
        // Create a writer to save LDD data in Elasticsearch JSON data file
        LddEsJsonWriter writer = null; 
        try
        {
            writer = new LddEsJsonWriter(tempEsFile, dtMap, ddAttrCache, overwrite);
            writer.setNamespaceFilter(namespace);
            
            // Parse class attribute associations and write to ES data file
            CaaCallback ccb = new CaaCallback(writer);
            ClassAttrAssociationParser caaParser = new ClassAttrAssociationParser(lddFile, ccb); 
            caaParser.parse();
    
            // Write data dictionary version and date
            writer.writeLddInfo(namespace, lddFileName, attrParser.getImVersion(), 
                    attrParser.getLddVersion(), attrParser.getLddDate());
        }
        finally
        {
            writer.close();
        }
    }
    
    
    private boolean overwriteLdd(Instant lastDate, String strLddDate)
    {
        try
        {
            Instant lddDate = LddUtils.lddDateToIsoInstant(strLddDate);
            return lddDate.isAfter(lastDate);
        }
        catch(Exception ex)
        {
            log.warn("Could not parse LDD date " + strLddDate);
            return false;
        }
    }
}
