package com.sql.parse.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Properties;


public class PropertyFileUtil {
    private static Logger logger = Logger.getLogger(PropertyFileUtil.class);

    private static Properties properties = null;

    public static synchronized boolean isLoaded() {
        return properties != null;
    }

    public static void init(String path) throws FileNotFoundException {
        InputStream is = new FileInputStream(new File(path));
//        InputStream is = PropertyFileUtil.class.getClass().getResourceAsStream(path);
        if (null != is) {
            loadProperty(is);
        } else {
            logger.error("Can not find propertyFile");
        }
        return;
    }

//    static  {
//        InputStream is = PropertyFileUtil.class.getClass().getResourceAsStream("/app.properties");
//        if (null != is) {
//            loadProperty(is);
//        }
//    }

    private static void loadProperty(String file) {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void loadProperty(InputStream is) {
        properties = new Properties();
        try {
            properties.load(is);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}