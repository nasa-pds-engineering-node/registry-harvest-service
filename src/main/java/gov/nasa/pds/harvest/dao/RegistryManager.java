package gov.nasa.pds.harvest.dao;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.cfg.RegistryCfg;
import gov.nasa.pds.registry.common.es.client.EsClientFactory;
import gov.nasa.pds.registry.common.es.dao.ProductDao;
import gov.nasa.pds.registry.common.es.dao.dd.DataDictionaryDao;
import gov.nasa.pds.registry.common.es.dao.schema.SchemaDao;
import gov.nasa.pds.registry.common.es.service.MissingFieldsProcessor;
import gov.nasa.pds.registry.common.es.service.ProductService;
import gov.nasa.pds.registry.common.es.service.SchemaUpdater;
import gov.nasa.pds.registry.common.meta.FieldNameCache;
import gov.nasa.pds.registry.common.meta.MetadataNormalizer;
import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * A singleton object to query Elasticsearch.
 *  
 * @author karpenko
 */
public class RegistryManager
{
    // Singleton
    private static RegistryManager instance = null;

    private RegistryCfg cfg;
    
    // Elasticsearch client
    private RestClient esClient;
    
    // DAOs
    private RegistryDao registryDao;
    private SchemaDao schemaDao;
    private DataDictionaryDao ddDao;
    private ProductDao productDao;
    
    // Services
    private ProductService productService;    
    private FieldNameCache fieldNameCache;
    
    
    /**
     * Private constructor. Use getInstance() instead.
     * @param cfg Registry (Elasticsearch) configuration parameters.
     * @throws Exception Generic exception
     */
    private RegistryManager(RegistryCfg cfg) throws Exception
    {
        this.cfg = cfg;
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
        
        // DAOs
        registryDao = new RegistryDao(esClient, indexName);
        schemaDao = new SchemaDao(esClient, indexName);
        ddDao = new DataDictionaryDao(esClient, indexName);
        productDao = new ProductDao(esClient, indexName);
        
        // Services
        productService = new ProductService(productDao);
        fieldNameCache = new FieldNameCache(ddDao, schemaDao);
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
    public RegistryDao getRegistryDao()
    {
        return registryDao;
    }


    /**
     * Get schema DAO object.
     * @return Schema DAO
     */
    public SchemaDao getSchemaDao()
    {
        return schemaDao;
    }


    /**
     * Get data dictionary DAO object.
     * @return Schema DAO
     */
    public DataDictionaryDao getDataDictionaryDao()
    {
        return ddDao;
    }

    
    /**
     * Get product DAO object.
     * @return Product DAO
     */
    public ProductDao getProductDao()
    {
        return productDao;
    }

    
    /**
     * Get product service object
     * @return product service
     */
    public ProductService getProductService()
    {
        return productService;
    }


    /**
     * Get Field name cache
     * @return Schema DAO
     */
    public FieldNameCache getFieldNameCache()
    {
        return fieldNameCache;
    }

    
    /**
     * Create new missing field processor
     * @return new missing field processor object
     * @throws Exception an exception
     */
    public MissingFieldsProcessor createMissingFieldsProcessor() throws Exception
    {
        SchemaUpdater su = new SchemaUpdater(cfg, ddDao, schemaDao);
        return new MissingFieldsProcessor(su, fieldNameCache);
    }


    /**
     * Create new metadata normalizer
     * @return new metadata normalizer object
     */
    public MetadataNormalizer createMetadataNormalizer()
    {
        return new MetadataNormalizer(fieldNameCache);
    }

}
