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
    public Long evaluate(Long n1, Long n2) {
        if (null == n2
                || (null != n1 && n1 > n2)) {
            return n1;
        }
        return n2;
    }

    /**
     * 返回数字集合中的最大值，都为空则返回null
     * 方法名称必须为evaluate
     *
     * @param nums  入参数字集合
     * @return  入参数字集合中的最大值
     */
    public Long evaluate(Long[] nums) {
        Long maxNum = null;
        for (Long num : nums) {
            if (null == maxNum
                    || (null != num && num > maxNum)) {
                maxNum = num;
            }
        }
        return maxNum;
    }
}
