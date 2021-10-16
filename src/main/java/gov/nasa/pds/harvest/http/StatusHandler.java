package gov.nasa.pds.harvest.http;

import java.util.jar.Attributes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import gov.nasa.pds.harvest.util.ManifestUtils;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;


public class StatusHandler implements HttpHandler
{
    private Gson gson;
    
    private String version;
    private String buildTime;

    
    private static class Status
    {
        public String application = "Harvest";
        public String version;
        public String buildTime;
        
        public String maxMemory;
        public String totalMemory;
        public String freeMemory;
    }
    
    
    public StatusHandler()
    {
        gson = new GsonBuilder().setPrettyPrinting().create();
        setVersioninfo();
    }
    
    
    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception
    {
        Status status = new Status();
        status.version = version;
        status.buildTime = buildTime;
        status.maxMemory = (Runtime.getRuntime().maxMemory() / 1000000) + " MB";
        status.totalMemory = (Runtime.getRuntime().totalMemory() / 1000000) + " MB";
        status.freeMemory = (Runtime.getRuntime().freeMemory() / 1000000) + " MB";
        
        String jsonStr = gson.toJson(status);
        
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
        exchange.getResponseSender().send(jsonStr);
    }


    private void setVersioninfo()
    {
        version = StatusHandler.class.getPackage().getImplementationVersion();
        Attributes attrs = ManifestUtils.getAttributes();
        if(attrs != null)
        {
            buildTime = attrs.getValue("Build-Time");
        }
    }
}
