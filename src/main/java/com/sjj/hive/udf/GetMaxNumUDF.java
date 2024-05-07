package com.sjj.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author Jiajun Song
 * @version 1.0.0
 * @date 2024/4/13
 */
@Description(
        name = "get_max_num",
        value = "_FUNC_(x, y, ...) - return the maximum value in x,y,...",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(1, 5) FROM table;\n"
                + "  5\n"
                + "  > SELECT _FUNC_(-123, 789, 0) FROM table;\n"
                + "  789"
)
public class GetMaxNumUDF extends UDF {
    /**
     * 返回两个数字中的最大值，都为空则返回null
     * 方法名称必须为evaluate
     *
     * @param n1  第一个数字
     * @param n2  第二个数字
     * @return  两个数字中的最大值
     */
    public String evaluate(Long n1, Long n2) {
        Long maxNum = n1;
        if (maxNum == null
                || (n2 != null && n2 > maxNum)) {
            maxNum = n2;
        }
        return "The max num (Long,Long) is " + (maxNum == null ? null : String.valueOf(maxNum));
    }

    /**
     * 返回两个数字中的最大值，都为空则返回null
     *
     * @param n1  第一个数字
     * @param n2  第二个数字
     * @return  两个数字中的最大值
     */
    public String evaluate(Integer n1, Integer n2) {
        Integer maxNum = n1;
        if (maxNum == null
                || (n2 != null && n2 > maxNum)) {
            maxNum = n2;
        }
        return "The max num (Integer,Integer) is " + (maxNum == null ? null : String.valueOf(maxNum));
    }

    /**
     * 返回数字集合中的最大值，都为空则返回null
     * 方法名称必须为evaluate
     *
     * @param nums  入参数字集合
     * @return  入参数字集合中的最大值
     */
    public String evaluate(Integer... nums) {
        Integer maxNum = null;
        for (Integer num : nums) {
            if (null == maxNum
                    || (null != num && num > maxNum)) {
                maxNum = num;
            }
        }

        return "The max num (Integer...) is " + (maxNum == null ? null : String.valueOf(maxNum));
    }

    /**
     * 返回数字集合中的最大值，都为空则返回null
     * 方法名称必须为evaluate
     *
     * @param nums  入参数字集合
     * @return  入参数字集合中的最大值
     */
    public String evaluate(Long[] nums) {
        Long maxNum = null;
        for (Long num : nums) {
            if (null == maxNum
                    || (null != num && num > maxNum)) {
                maxNum = num;
            }
        }

        return "The max num (Long[]) is " + (maxNum == null ? null : String.valueOf(maxNum));
    }
}
