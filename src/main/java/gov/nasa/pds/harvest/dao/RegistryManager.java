package gov.nasa.pds.harvest.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.harvest.cfg.RegistryCfg;
import gov.nasa.pds.harvest.util.CloseUtils;
import gov.nasa.pds.registry.common.es.client.EsClientFactory;


/**
 * A singleton object to query Elasticsearch.
 *  
 * @author karpenko
 */
public class RegistryManager
{
    private static RegistryManager instance = null;
    
    private RestClient esClient;
    private RegistryDAO registryDAO;
    
    
    /**
     * Private constructor. Use getInstance() instead.
     * @param cfg Registry (Elasticsearch) configuration parameters.
     * @throws Exception Generic exception
     */
    private RegistryManager(RegistryCfg cfg) throws Exception
    {
        if(cfg.url == null || cfg.url.isEmpty()) throw new IllegalArgumentException("Missing Registry URL");
        
        esClient = EsClientFactory.createRestClient(cfg.url, cfg.authFile);
        
        String indexName = cfg.indexName;
        if(indexName == null || indexName.isEmpty()) 
        {
            indexName = "registry";
        }

        Logger log = LogManager.getLogger(this.getClass());
        log.info("Registry URL: " + cfg.url);
        log.info("Registry index: " + indexName);
        
        registryDAO = new RegistryDAO(esClient, indexName);
    }
    
    
    /**
     * Initialize the singleton.
     * @param cfg Registry (Elasticsearch) configuration parameters.
     * @throws Exception Generic exception
     */
    public static void init(RegistryCfg cfg) throws Exception
    {
        instance = new RegistryManager(cfg);
    }
    
    
    /**
     * Clean up resources (close Elasticsearch client / connection).
     */
    public static void destroy()
    {
        if(instance == null) return;
        
        CloseUtils.close(instance.esClient);
        instance = null;
    }
    
    
    /**
     * Get the singleton instance.
     * @return Registry manager singleton
     */
    public static RegistryManager getInstance()
    {
        return instance;
    }
    
    
    /**
     * Get registry DAO object.
     * @return Registry DAO
     */
    public RegistryDAO getRegistryDAO()
    {
        return registryDAO;
    }
}
