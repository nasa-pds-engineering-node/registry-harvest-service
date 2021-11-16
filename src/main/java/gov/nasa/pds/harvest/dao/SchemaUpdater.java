package gov.nasa.pds.harvest.dao;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.dd.LddLoader;
import gov.nasa.pds.harvest.dd.LddUtils;
import gov.nasa.pds.harvest.util.file.FileDownloader;

/**
 * Update Elasticsearch schema and LDDs
 * @author karpenko
 *
 */
public class SchemaUpdater
{
    private Logger log;
    private FileDownloader fileDownloader;
    private LddLoader lddLoader;

    /**
     * Constructor
     * @param cfg Registry (Elasticsearch) configuration
     * @throws Exception
     */
    public SchemaUpdater(RegistryCfg cfg) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        fileDownloader = new FileDownloader();
        
        lddLoader = new LddLoader(cfg.url, cfg.indexName, cfg.authFile);
        lddLoader.loadPds2EsDataTypeMap(LddUtils.getPds2EsDataTypeCfgFile());
    }
    
    
    public void updateSchema(Set<String> fields, Map<String, String> xsds) throws Exception
    {
        SchemaDao dao = RegistryManager.getInstance().getSchemaDAO();

        log.info("Updating LDDs.");
        for(Map.Entry<String, String> xsd: xsds.entrySet())
        {
            String uri = xsd.getKey();
            String prefix = xsd.getValue();
            
            updateLdd(uri, prefix);
        }
        
        log.info("Updating Elasticsearch schema.");
        
        List<Tuple> newFields = dao.getDataTypes(fields);
        if(newFields != null)
        {
            //dao.updateSchema(newFields);
            log.info("Updated " + newFields.size() + " fields");
        }
    }

    
    private void updateLdd(String uri, String prefix) throws Exception
    {
        if(uri == null || uri.isEmpty()) return;
        if(prefix == null || prefix.isEmpty()) return;

        log.info("Updating '" + prefix  + "' LDD from " + uri);
        
        // Get JSON schema URL from XSD URL
        String jsonUrl = getJsonUrl(uri);

        // Get schema file name
        int idx = jsonUrl.lastIndexOf('/');
        if(idx < 0) throw new Exception("Invalid schema URI." + uri);
        String schemaFileName = jsonUrl.substring(idx+1);
        
        // Get stored LDDs info
        SchemaDao dao = RegistryManager.getInstance().getSchemaDAO();
        LddInfo lddInfo = dao.getLddInfo(prefix);

        // LDD already loaded
        if(lddInfo.files.contains(schemaFileName)) 
        {
            log.info("This LDD already loaded.");
            return;
        }

        // Download LDD
        File lddFile = File.createTempFile("LDD-", ".JSON");
        
        try
        {
            fileDownloader.download(jsonUrl, lddFile);
        }
        catch(Exception ex)
        {
            if(lddInfo.isEmpty())
            {
                log.warn("Will use 'keyword' data type.");
            }
            else
            {
                log.warn("Will use field definitions from " + lddInfo.files);
            }
        }
        finally
        {
            lddFile.delete();
        }


    }
    
    
    private String getJsonUrl(String uri) throws Exception
    {
        if(uri.endsWith(".xsd"))
        {
            String jsonUrl = uri.substring(0, uri.length()-3) + "JSON";
            return jsonUrl;
        }
        else
        {
            throw new Exception("Invalid schema URI. URI doesn't end with '.xsd': " + uri);
        }
    }

}
