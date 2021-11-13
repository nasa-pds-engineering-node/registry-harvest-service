package gov.nasa.pds.harvest.dd;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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

    private File tempDir;
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
        tempDir = new File(System.getProperty("java.io.tmpdir"));
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
     * @param ddFile PDS LDD JSON file
     * @param namespace Namespace filter. Only load classes having this namespace.
     * @throws Exception an exception
     */
    public void load(File ddFile, String namespace) throws Exception
    {
        File tempEsDataFile = new File(tempDir, "pds-registry-dd.tmp.json");
        log.info("Creating temporary ES data file " + tempEsDataFile.getAbsolutePath());
        createEsDataFile(ddFile, namespace, tempEsDataFile);

        // Load temporary file into data dictionary index
        loader.loadFile(tempEsDataFile);
        
        // Delete temporary file
        tempEsDataFile.delete();
    }

    
    /**
     * Create Elasticsearch data file to be loaded into data dictionary index.
     * @param ddFile PDS LDD JSON file
     * @param namespace Namespace filter. Only load classes having this namespace.
     * @param esFile Write to this Elasticsearch file
     * @throws Exception an exception
     */
    public void createEsDataFile(File ddFile, String namespace, File esFile) throws Exception
    {
        // Parse and cache LDD attributes
        Map<String, DDAttribute> ddAttrCache = new TreeMap<>();
        AttributeDictionaryParser attrParser = new AttributeDictionaryParser(ddFile, 
                (attr) -> { ddAttrCache.put(attr.id, attr); } );
        attrParser.parse();

        // Create a writer to save LDD data in Elasticsearch JSON data file
        LddEsJsonWriter writer = new LddEsJsonWriter(esFile, dtMap, ddAttrCache);
        writer.setNamespaceFilter(namespace);
        
        // Parse class attribute associations and write to ES data file
        Set<String> namespaces = new TreeSet<>();
        ClassAttrAssociationParser caaParser = new ClassAttrAssociationParser(ddFile, 
                (classNs, className, attrId) -> { 
                    writer.writeFieldDefinition(classNs, className, attrId);
                    namespaces.add(classNs);
        });
        caaParser.parse();

        // Determine LDD namespace
        if(namespace == null)
        {
            if(namespaces.size() == 1)
            {
                namespace = namespaces.iterator().next();
            }
            else
            {
                throw new Exception("Data dictionary has multiple namespaces. Specify one namespace to use.");
            }
        }
        
        // Write data dictionary version and date
        writer.writeDataDictionaryVersion(namespace, attrParser.getImVersion(), 
                attrParser.getLddVersion(), attrParser.getLddDate());
        
        writer.close();
    }
}
