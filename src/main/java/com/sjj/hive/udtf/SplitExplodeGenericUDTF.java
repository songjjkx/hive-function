package com.sjj.hive.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.STRING;

/**
 * @author Jiajun Song
 * @version 1.0.0
 * @date 2024/5/7
 */
@Description(
        name = "split_explode_udtf",
        value = "_FUNC_(data,delimiter)",
        extended = "Example:\n"
                + "  > SELECT name, str FROM table LATERAL VIEW _FUNC_(data,delimiter) temp_table AS str;"
)
@SuppressWarnings("deprecation")
public class SplitExplodeGenericUDTF extends GenericUDTF {
    /**
     * 输入参数对象检查器
     */
    private transient PrimitiveObjectInspector[] inputOIs;

    /**
     * 函数返回的结果
     */
    private transient Object forwardObj[] = new Object[1];

    /**
     * 初始化
     *
     * @param argOIs
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if (argOIs.length != 2) {
            throw new UDFArgumentException("The split_explode_udtf takes only two arguments.");
        } else {
            inputOIs = new PrimitiveObjectInspector[argOIs.length];
            for (int i = 0; i < argOIs.length; i++) {
                if (!argOIs[i].getCategory().equals(ObjectInspector.Category.PRIMITIVE)
                        || !((PrimitiveObjectInspector) argOIs[i]).getPrimitiveCategory().equals(STRING)) {
                    throw new UDFArgumentException(
                            "The split_explode_udtf takes only string types, got " + argOIs[i].getTypeName());
                }
                inputOIs[i] = (PrimitiveObjectInspector) argOIs[i];
            }
            ArrayList<String> fname = new ArrayList<>();
            ArrayList<ObjectInspector> foi = new ArrayList<>();
            fname.add("col");
            foi.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
            // 函数出参类型
            return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
        }
    }

    /**
     * 实现具体的处理逻辑
     *
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        Object data = args[0];
        Object delimiter = args[1];
        if (null != data && null != delimiter) {
            String dataStr = PrimitiveObjectInspectorUtils.getString(data, inputOIs[0]);
            String delimiterStr = PrimitiveObjectInspectorUtils.getString(delimiter, inputOIs[1]);
            String[] arr = dataStr.split(delimiterStr);
            for (String s : arr) {
                forwardObj[0] = new Text(s);
                // 输出计算结果
                forward(forwardObj);
            }
        }
    }

    /**
     * 计算结束后调用该方法
     *
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {
    }
}
