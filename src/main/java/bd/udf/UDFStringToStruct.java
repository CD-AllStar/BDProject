package bd.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

public class UDFStringToStruct extends GenericUDF {

    private Object[] results;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        // Define the field names for the struct<> and their types
        ArrayList<String> structFieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

        // fill struct field names
        // 消息id
        structFieldNames.add("msgid");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        // 消息发送时间戳
        structFieldNames.add("time");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        // 分词
        structFieldNames.add("word");
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        StructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames,
                structFieldObjectInspectors);
        return si;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        results = new Object[3];
        String text = ((Text)deferredObjects[0].get()).toString();
        results[0] = new Text(text.split(":")[0]);
        results[1] = new LongWritable(Long.parseLong(text.split(":")[1]));
        results[2] = new Text(text.split(":")[2]);
        return results;

    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
