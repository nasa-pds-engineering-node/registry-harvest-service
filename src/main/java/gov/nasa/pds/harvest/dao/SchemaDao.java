package gov.nasa.pds.harvest.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;


/**
 * Elasticsearch schema DAO (Data Access Object).
 * This class provides methods to read and update Elasticsearch schema
 * and data dictionary. 
 * 
 * @author karpenko
 */
public class SchemaDao
{
    private Logger log;
    private RestClient client;
    private String indexName;
    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name
     */
    public SchemaDao(RestClient client, String indexName)
    {
        log = LogManager.getLogger(this.getClass());
        this.client = client;
        this.indexName = indexName;
    }
    
    
    /**
     * Call Elasticsearch "mappings" API to get a list of field names.
     * @return a collection of field names
     * @throws Exception an exception
     */
    public Set<String> getFieldNames() throws Exception
    {
        Request req = new Request("GET", "/" + indexName + "/_mappings");
        Response resp = client.performRequest(req);
        
        MappingsParser parser = new MappingsParser(indexName);
        return parser.parse(resp.getEntity());
    }
    
    
    /**
     * Inner private class to parse LDD information response from Elasticsearch.
     * @author karpenko
     */
    private static class GetLddDateRespParser extends SearchResponseParser implements SearchResponseParser.Callback
    {
        public LddInfo info;
        
        public GetLddDateRespParser()
        {
            info = new LddInfo();
        }
        
        @Override
        public void onRecord(String id, Object rec) throws Exception
        {
            if(rec instanceof Map)
            {
                @SuppressWarnings("rawtypes")
                Map map = (Map)rec;
                
                String strDate = (String)map.get("date");
                info.updateDate(strDate);
                
                String file = (String)map.get("attr_name");
                info.addSchemaFile(file);
            }
        }
    }
    
    /**
     * Get LDD date from data dictionary index in Elasticsearch.
     * @param indexName Elasticsearch base index name, e.g., "registry". 
     * NOTE: don't use full index name, like "registry-dd". 
     * @param namespace LDD namespace, e.g., "pds", "geom", etc.
     * @return ISO instant class representing LDD date.
     * @throws Exception an exception
     */
    public LddInfo getLddInfo(String namespace) throws Exception
    {
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createGetLddInfoRequest(namespace);

        Request req = new Request("GET", "/" + indexName + "-dd/_search");
        req.setJsonEntity(json);
        Response resp = client.performRequest(req);
        
        GetLddDateRespParser parser = new GetLddDateRespParser();
        parser.parseResponse(resp, parser); 
        return parser.info;
    }
    
    
    /**
     * Add new fields to Elasticsearch schema.
     * @param fields A list of fields to add. Each field tuple has a name and a data type.
     * @throws Exception an exception
     */
    public void updateSchema(List<Tuple> fields) throws Exception
    {
        if(fields == null || fields.isEmpty()) return;
        
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createUpdateSchemaRequest(fields);
        
        Request req = new Request("PUT", "/" + indexName + "/_mapping");
        req.setJsonEntity(json);
        client.performRequest(req);
    }
    
    
    /**
     * Query Elasticsearch data dictionary to get data types for a list of field ids.
     * @param ids A list of field IDs, e.g., "pds:Array_3D/pds:axes".
     * @return Data types information object
     * @throws Exception an exception.
     */
    public List<Tuple> getDataTypes(Collection<String> ids) throws Exception
    {
        if(ids == null || ids.isEmpty()) return null;

        List<Tuple> dtInfo = new ArrayList<Tuple>();
        
        // Create request
        Request req = new Request("GET", "/" + indexName + "-dd/_mget?_source=es_data_type");
        
        // Create request body
        SchemaRequestBuilder bld = new SchemaRequestBuilder();
        String json = bld.createMgetRequest(ids);
        req.setJsonEntity(json);
        
        // Call ES
        Response resp = client.performRequest(req);
        GetDataTypesResponseParser parser = new GetDataTypesResponseParser();
        List<GetDataTypesResponseParser.Record> records = parser.parse(resp.getEntity());
        
        for(GetDataTypesResponseParser.Record rec: records)
        {
            if(rec.found)
            {
                dtInfo.add(new Tuple(rec.id, rec.esDataType));
            }
            // There is no data type for this field in ES registry-dd index
            else
            {
                // Automatically assign data type for known fields
                if(rec.id.startsWith("ref_lid_") || rec.id.startsWith("ref_lidvid_"))
                {
                    dtInfo.add(new Tuple(rec.id, "keyword"));
                    continue;
                }

                log.warn("Could not find datatype for field " + rec.id + ". Will use 'keyword'");
                dtInfo.add(new Tuple(rec.id, "keyword"));
            }
        }
        
        return dtInfo;
    }
        
}
