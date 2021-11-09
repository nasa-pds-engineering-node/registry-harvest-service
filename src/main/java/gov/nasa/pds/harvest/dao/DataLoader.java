package gov.nasa.pds.harvest.dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import gov.nasa.pds.harvest.util.CloseUtils;
import gov.nasa.pds.registry.common.es.client.EsUtils;
import gov.nasa.pds.registry.common.es.client.HttpConnectionFactory;


/**
 * Loads data from an NJSON (new-line-delimited JSON) file into Elasticsearch.
 * NJSON file has 2 lines per record: 1 - primary key, 2 - data record.
 * This is the standard file format used by Elasticsearch bulk load API.
 * Data are loaded in batches.
 * 
 * @author karpenko
 */
public class DataLoader
{
    private Logger log;
    private HttpConnectionFactory conFactory; 


    /**
     * Constructor
     * @param esUrl Elasticsearch URL
     * @param esIndex Elasticsearch index name
     * @param esAuthFile Elasticsearch authentication configuration file
     * @throws Exception an exception
     */
    public DataLoader(String esUrl, String esIndex, String esAuthFile) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        conFactory = new HttpConnectionFactory(esUrl, esIndex, "_bulk");
        conFactory.initAuth(esAuthFile);
    }
    
    
    /**
     * Load data into Elasticsearch
     * @param data NJSON data. (2 lines per record)
     * @throws Exception an exception
     */
    public void loadBatch(List<String> data) throws Exception
    {
        if(data == null || data.isEmpty()) return;
        if(data.size() % 2 != 0) throw new Exception("Data list size should be an even number.");
        
        HttpURLConnection con = null;
        
        try
        {
            con = conFactory.createConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("content-type", "application/x-ndjson; charset=utf-8");
            
            OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8");
            
            for(int i = 0; i < data.size(); i+=2)
            {
                writer.write(data.get(i));
                writer.write("\n");
                writer.write(data.get(i+1));
                writer.write("\n");
            }
            
            writer.flush();
            writer.close();
        
            // Check for Elasticsearch errors.
            String respJson = getLastLine(con.getInputStream());
            log.debug(respJson);
            
            if(responseHasErrors(respJson))
            {
                throw new Exception("Could not load data.");
            }
        }
        catch(UnknownHostException ex)
        {
            throw new Exception("Unknown host " + conFactory.getHostName());
        }
        catch(IOException ex)
        {
            // Get HTTP response code
            int respCode = getResponseCode(con);
            if(respCode <= 0) throw ex;
            
            // Try extracting JSON from multi-line error response (last line) 
            String json = getLastLine(con.getErrorStream());
            if(json == null) throw ex;
            
            // Parse error JSON to extract reason.
            String msg = EsUtils.extractReasonFromJson(json);
            if(msg == null) msg = json;
            
            throw new Exception(msg);
        }
    }
    
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private boolean responseHasErrors(String resp)
    {
        try
        {
            // Parse JSON response
            Gson gson = new Gson();
            Map json = (Map)gson.fromJson(resp, Object.class);
            
            Boolean hasErrors = (Boolean)json.get("errors");
            if(hasErrors)
            {
                List<Object> list = (List)json.get("items");
                
                // List size = batch size (one item per document)
                // NOTE: Only few items in the list could have errors
                for(Object item: list)
                {
                    Map index = (Map)((Map)item).get("index");
                    Map error = (Map)index.get("error");
                    if(error != null)
                    {
                        String message = (String)error.get("reason");
                        log.error(message);
                        return true;
                    }
                }
            }

            return false;
        }
        catch(Exception ex)
        {
            return false;
        }
    }
    
    
    /**
     * Get HTTP response code, e.g., 200 (OK)
     * @param con HTTP connection
     * @return HTTP response code, e.g., 200 (OK)
     */
    private static int getResponseCode(HttpURLConnection con)
    {
        if(con == null) return -1;
        
        try
        {
            return con.getResponseCode();
        }
        catch(Exception ex)
        {
            return -1;
        }
    }

    
    /**
     * This method is used to parse multi-line Elasticsearch error responses.
     * JSON error response is on the last line of a message.
     * @param is input stream
     * @return Last line
     */
    private static String getLastLine(InputStream is)
    {
        String lastLine = null;

        try
        {
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));

            String line;
            while((line = rd.readLine()) != null)
            {
                lastLine = line;
            }
        }
        catch(Exception ex)
        {
            // Ignore
        }
        finally
        {
            CloseUtils.close(is);
        }
        
        return lastLine;
    }
}
