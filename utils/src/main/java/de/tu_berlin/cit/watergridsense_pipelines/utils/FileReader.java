package de.tu_berlin.cit.watergridsense_pipelines.utils;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public enum FileReader { GET;

    public static final Logger LOG = Logger.getLogger(FileReader.class);

    private static Map<String, Object> fileMap = new HashMap<>();

    public <T> T read(String fileName, Class<T> clazz) throws Exception {

        if (!fileMap.containsKey(fileName)) {
            try (InputStream input = FileReader.GET.getClass().getClassLoader().getResourceAsStream(fileName)) {
                if (clazz == Properties.class) {
                    Object o = clazz.newInstance();
                    Method method = clazz.getMethod("load", InputStream.class);
                    method.invoke(o, input);
                    fileMap.put(fileName, o);
                }
                else if (clazz == File.class) {
                    final File tempFile = File.createTempFile("stream2file", ".tmp");
                    tempFile.deleteOnExit();
                    try (FileOutputStream output = new FileOutputStream(tempFile)) {
                        assert input != null;
                        IOUtils.copy(input, output);
                    }
                    fileMap.put(fileName, tempFile);
                }
                else {
                    throw new IllegalStateException("Unrecognized file type: " + clazz);
                }
            }
            catch (IOException ex) {
                LOG.error(ex);
                throw ex;
            }
        }
        return clazz.cast(fileMap.get(fileName));
    }
}
