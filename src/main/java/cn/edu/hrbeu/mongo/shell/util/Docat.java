/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hrbeu.mongo.shell.util;

import java.io.*;
import java.util.List;
import java.util.Set;
import org.bson.Document;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

/**
 *
 * @author mazhiqiang
 */
public class Docat {

    public static Document copy(Document doc, String... fields) {
        Document d = new Document();
        if (fields == null || fields.length <= 0) {
            d.putAll(doc);
        } else {
            for (String field : fields) {
                d.put(field, doc.get(field));
            }
        }
        return d;
    }

    public static Object get(Document doc, String field) {
        return doc.get(field);
    }
    
    public static Object get(Document doc, String field, Object defaultValue) {
        Object o = get(doc, field);
        if (o == null) {
            return defaultValue;
        }
        return o;
    }

    public static List getList(Document doc, String field, List defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null && value instanceof List) {
            return (List) value;
        }
        return defaultValue;
    }

    public static List getList(Document doc, String field) {
        return getList(doc, field, null);
    }

    public static Document getDocument(Document doc, String field, Document defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null && value instanceof Document) {
            return (Document) value;
        }
        return defaultValue;
    }

    public static Document getDocument(Document doc, String field) {
        return getDocument(doc, field, null);
    }

    public static String getString(Document doc, String field, String defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null) {
            if (value instanceof String) {
                return (String) value;
            }
            return value.toString();
        }
        return defaultValue;
    }

    public static String getString(Document doc, String field) {
        return getString(doc, field, null);
    }

    public static int getInteger(Document doc, String field, int defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null) {
            if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof Long) {
                return ((Long) value).intValue();
            } else if (value instanceof Boolean) {
                return ((Boolean) value)  ? 1 : -1;
            }
        }
        return defaultValue;
    }

    public static int getInteger(Document doc, String field) {
        return getInteger(doc, field, 0);
    }

    public static long getLong(Document doc, String field, long defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null) {
            if (value instanceof Long) {
                return (Long) value;
            } else if (value instanceof Integer) {
                return ((Integer) value).longValue();
            }
        }
        return defaultValue;
    }

    public static long getLong(Document doc, String field) {
        return getLong(doc, field, 0);
    }

    public static boolean getBoolean(Document doc, String field, boolean defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            } else if (value instanceof String) {
                if ("true".equalsIgnoreCase((String) value)) {
                    return true;
                } else if ("false".equalsIgnoreCase((String) value)) {
                    return false;
                }
            } else if (value instanceof Integer) {
                return (Integer) value > 0;
            }
        }
        return defaultValue;
    }

    public static boolean getBoolean(Document doc, String field) {
        return getBoolean(doc, field, false);
    }
    
    public static double getDouble(Document doc, String field, double defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null) {
            if (value instanceof String) {
                try {
                    double v = Double.parseDouble((String) value);
                    return v;
                } catch (Exception e) {
                    return defaultValue;
                }
            } else if (value instanceof Float) {
                return (Double) value;
            } else if (value instanceof Long || value instanceof Integer) {
                return (Double) value;
            }
        }
        return defaultValue;
    }

    public static double getDouble(Document doc, String field) {
        return getDouble(doc, field, 0.0);
    }
    
    public static ObjectId getObjectId(Document doc, String field, ObjectId defaultValue) {
        if (doc == null) {
            return defaultValue;
        }
        Object value = doc.get(field);
        if (value != null) {
            if (value instanceof ObjectId) {
                return (ObjectId) value;
            } else if (value instanceof String) {
                return new ObjectId((String) value);
            }
        }
        return defaultValue;
    }

    public static ObjectId getObjectId(Document doc, String field) {
        return getObjectId(doc, field, null);
    }

    public static ObjectId getOid(Document doc) {
        return getObjectId(doc, "_id");
    }

    public static String getStringId(Document doc) {
        return getString(doc, "_id");
    }

    public static Object getId(Document doc) {
        return doc.get("_id");
    }
    
    public static String getRetunCode(Document output) {
        return getString(output, "RC");
    }

    public static String getName(Document doc) {
        return getString(doc, "name");
    }

    public static boolean getReturnCodeOK(Document output) {
        return "OK".equalsIgnoreCase(getRetunCode(output));
    }

    public static Document getReturnValue(Document output) {
        return getDocument(output, "value");
    }

    public static List<Document> getReturnValues(Document output) {
        return getList(output, "values");
    }

    

    private static String prefactList(List list, String spaces) {
        if (list.isEmpty()) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        int j = 0;
        String nextLevelSpaces = spaces + "    ";
        for (Object v : list) {
            sb.append(nextLevelSpaces);
            if (v instanceof String) {
                sb.append("\"").append(v).append("\"");
            } else if (v instanceof Document) {
                sb.append(perfact((Document) v, nextLevelSpaces));
            } else if (v instanceof List) {
                sb.append(prefactList((List) v, nextLevelSpaces));
            } else {
                sb.append(v);
            }
            if (j != list.size() - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
            j++;
        }
        sb.append(spaces).append("]");
        return sb.toString();
    }
    
    public static Document select(String... fields) {
        Document select = null;
        if (fields != null && fields.length > 0) {
            select = new Document();
            for (String field : fields) {
                select.append(field, 1);
            }
        }
        return select;
    }
    
    public Document __unselect(String... fields) {
        Document select = null;
        if (fields != null && fields.length > 0) {
            select = new Document();
            for (String field : fields) {
                select.append(field, 0);
            }
        }
        return select;
    }
    
    // 输出优美的格式化的json字符串
    private static String perfact(Document doc, String spaces) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        Set<String> keys = doc.keySet();
        int i = 0;
        String nextLevelSpaces = spaces + "    ";
        for (String key : doc.keySet()) {
            sb.append(nextLevelSpaces).append("\"").append(key).append("\"").append(": ");
            Object value = doc.get(key);
            if (value instanceof String) {
                sb.append("\"").append(value).append("\"");
            } else if (value instanceof Document) {
                sb.append(perfact((Document) value, nextLevelSpaces));
            } else if (value instanceof List) {
                sb.append(prefactList((List) value, nextLevelSpaces));
            } else {
                sb.append(value);
            }
            if (i != keys.size() - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
            i++;
        }
        sb.append(spaces).append("}");
        return sb.toString();

    }
    // 输出优美的格式化的json字符串
    public static String perfact(Document doc) {
        return perfact(doc, "");
    }    
    
    private static byte[] readBytesFromFile(File file) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            byte[] fileData = new byte[(int) file.length()];
            //convert file into array of bytes
            fis.read(fileData);
            return fileData;
        } catch (FileNotFoundException e) {
        } catch (Exception e) {
        } finally {
            try {
                fis.close();
            } catch (IOException ex) {
            }
        }
        return null;
    }

    public static Document readFromFile(File file) {
        if (!file.exists() || !file.isFile() || file.length() <= 0) {
            return null;
        }
        try {
            byte[] bytes = readBytesFromFile(file);
            Document doc = Document.parse(new String(bytes, "UTF-8"));
            return doc;
        } catch (Exception e) {
        }
        return null;
    }

    public static String doc2json(Document doc) {
        CustomJsonWriter writer = new CustomJsonWriter(new StringWriter(), new JsonWriterSettings());
        CustomDocumentCodec encoder = new CustomDocumentCodec();
        encoder.encode(writer, doc, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getWriter().toString();
    }


    public static Document json2doc(String json) {
        if (!Strings.isEmptyStrictly(json)) {
            try {
                return Document.parse(json);
            } catch (Exception ex) {
            }
        }
        return null;
    }

    // 获得指定路径下的字段值
    public static Object valueofDocumentField(Object defaultValue, Document doc, String... fieldPath) {
        Document d = doc;
        for (int i = 0; i < fieldPath.length - 1; i++) {
            Object t = d.get(fieldPath[i]);
            if (t instanceof Document) {
                d = (Document) t;
            } else {
                return defaultValue;
            }
        }
        Object o = d.get(fieldPath[fieldPath.length - 1]);
        return o == null ? defaultValue : o;
    }

    // 判断指定路径下的值是否相等
    public static Object equalsDocumentField(Object value, Document doc, String... fields) {
        Object v = valueofDocumentField(null, doc, fields);
        if (v == null && value == null) {
            return true;
        } else {
            return value.equals(v);
        }
    }

}
