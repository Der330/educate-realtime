package com.atguigu.edu.realtime.dws.function;

import com.atguigu.edu.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author: 刘大大
 * @CreateTime: 2024/9/13  13:54
 */


@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            //将分词结果向下游传递
            collect(Row.of(keyword));
        }
    }
}
