package gov.nasa.pds.harvest.dao;

import java.util.Set;

import gov.nasa.pds.harvest.meta.FieldNameCache;

public class SchemaUtils
{
    public static void updateFieldsCache() throws Exception
    {
        SchemaDao schemaDao = RegistryManager.getInstance().getSchemaDAO();
        Set<String> fields = schemaDao.getFieldNames();
        FieldNameCache.getInstance().set(fields);
    }
}
