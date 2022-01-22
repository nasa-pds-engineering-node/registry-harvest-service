package gov.nasa.pds.harvest.util.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gov.nasa.pds.registry.common.util.CloseUtils;


/**
 * File downloader with retry logic. 
 * By default, SSL certificate and host verification is disabled for HTTPS 
 * connections to support self-signed certificates. This can be turned off.
 *  
 * @author karpenko
 */
public class FileDownloader
{
    private Logger log;
    private int numRetries = 3;
    
    private CloseableHttpClient httpClient;
    

    /**
     * Constructor
     * @param sslTrustAll Enable or disable SSL certificate and host validation 
     * to support self-signed certificates.
     * @throws Exception
     */
    public FileDownloader(boolean sslTrustAll) throws Exception
    {
        log = LogManager.getLogger(this.getClass());
        httpClient = createHttpClient(sslTrustAll);
    }

    
    /**
     * Download a file from a URL. Retry several times on error.
     * @param fromUrl Download a file from this URL.
     * @param toFile Save to this file
     * @throws Exception an exception
     */
    public void download(String fromUrl, File toFile) throws Exception
    {
        int count = 0;
        
        while(true)
        {
            try
            {
                count++;
                downloadOnce(fromUrl, toFile);
                return;
            }
            catch(Exception ex)
            {
                log.error(ex.getMessage());
                if(count < numRetries)
                {
                    log.info("Will retry in 5 seconds");
                    Thread.sleep(5000);
                }
                else
                {
                    throw new Exception("Could not download " + fromUrl);
                }
            }
        }
    }
    
    
    /**
     * Try downloading file once.
     * @param fromUrl source URL
     * @param toFile target file
     * @throws Exception an exception
     */
    private void downloadOnce(String fromUrl, File toFile) throws Exception
    {
        InputStream is = null;
        FileOutputStream os = null;
        CloseableHttpResponse resp = null;
        
        log.info("Downloading " + fromUrl + " to " + toFile.getAbsolutePath());
        
        try
        {
            HttpGet httpGet = new HttpGet(fromUrl);
            resp = httpClient.execute(httpGet);
            StatusLine status = resp.getStatusLine();
            
            if(status.getStatusCode() != 200)
            {
                throw new Exception(status.getStatusCode()  + " - " + status.getReasonPhrase());
            }
            
            HttpEntity entity = resp.getEntity();
            is = entity.getContent();            
            os = new FileOutputStream(toFile);            
            is.transferTo(os);
        }
        finally
        {
            CloseUtils.close(os);
            CloseUtils.close(is);
            CloseUtils.close(resp);
        }
    }


    private CloseableHttpClient createHttpClient(boolean sslTrustAll) throws Exception
    {
        if(sslTrustAll)
        {
            TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
            
            RegistryBuilder<ConnectionSocketFactory> sfRegistryBld = RegistryBuilder.<ConnectionSocketFactory>create();
            sfRegistryBld.register("https", sslsf);
            sfRegistryBld.register("http", new PlainConnectionSocketFactory());
            Registry<ConnectionSocketFactory> sfRegistry = sfRegistryBld.build();

            BasicHttpClientConnectionManager connectionManager = new BasicHttpClientConnectionManager(sfRegistry);
            
            HttpClientBuilder clientBld = HttpClients.custom();
            clientBld.setSSLSocketFactory(sslsf);
            clientBld.setConnectionManager(connectionManager);
            
            CloseableHttpClient httpClient = clientBld.build();
            return httpClient;
        }
        else
        {
            return HttpClients.createDefault();
        }
    }

}
