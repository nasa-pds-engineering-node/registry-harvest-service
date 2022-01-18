package gov.nasa.pds.harvest.dao;

import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import gov.nasa.pds.registry.common.es.client.SearchResponseParser;
import gov.nasa.pds.registry.common.util.CloseUtils;

/**
 * Product data access object. 
 * Provides methods to query Elasticsearch for product information.
 * @author karpenko
 */
public class ProductDao
{
    private Logger log;
    
    private RestClient client;
    private String indexName;
    private boolean pretty;

    private EsRequestBuilder requestBld;
    private SearchResponseParser parser;

    
    /**
     * Constructor
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name, e.g., "registry".
     */
    public ProductDao(RestClient client, String indexName)
    {
        this(client, indexName, false);
    }
    
    
    /**
     * Constructor.
     * @param client Elasticsearch client
     * @param indexName Elasticsearch index name, e.g., "registry".
     * @param pretty Pretty-format Elasticsearch response JSON. Used for debugging.
     */
    public ProductDao(RestClient client, String indexName, boolean pretty)
    {
        log = LogManager.getLogger(this.getClass());
        
        this.client = client;
        this.indexName = indexName;
        this.pretty = pretty;
        
        requestBld = new EsRequestBuilder();
        parser = new SearchResponseParser();
    }

    
    /**
     * Get product class by LIDVID
     * @param lidvid product LIDVID
     * @return product class, such as "Product_Bundle" or null if the LIDVID doesn't exist.
     * @throws Exception an exception
     */
    public String getProductClass(String lidvid) throws Exception
    {
        if(lidvid == null) return null;
        
        String reqUrl = "/" + indexName + "/_doc/" + lidvid + "?_source=product_class";
        Request req = new Request("GET", reqUrl);
        Response resp = null;
        
        try
        {
            resp = client.performRequest(req);
        }
        catch(ResponseException ex)
        {
            resp = ex.getResponse();
            int code = resp.getStatusLine().getStatusCode();
            // Invalid LIDVID
            if(code == 404 || code == 405) 
            {
                return null;
            }
            else
            {
                throw ex;
            }
        }

        InputStream is = null;
        
        try
        {
            is = resp.getEntity().getContent();
            JsonReader rd = new JsonReader(new InputStreamReader(is));
            
            rd.beginObject();
            
            while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
            {
                String name = rd.nextName();
                if("_source".equals(name))
                {
                    return parseProductClassSource(rd);
                }
                else
                {
                    rd.skipValue();
                }
            }
            
            rd.endObject();
        }
        finally
        {
            CloseUtils.close(is);
        }
        
        return null;
    }
    
    
    private String parseProductClassSource(JsonReader rd) throws Exception
    {
        rd.beginObject();

        while(rd.hasNext() && rd.peek() != JsonToken.END_OBJECT)
        {
            String name = rd.nextName();
            if("product_class".equals(name))
            {
                return rd.nextString();
            }
            else
            {
                rd.skipValue();
            }
        }
        
        rd.endObject();
        
        return null;
    }
}
