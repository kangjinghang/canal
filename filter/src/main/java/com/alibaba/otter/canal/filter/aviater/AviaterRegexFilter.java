package com.alibaba.otter.canal.filter.aviater;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.filter.CanalEventFilter;
import com.alibaba.otter.canal.filter.exception.CanalFilterException;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

/**
 * 基于aviater进行tableName正则匹配的过滤算法
 * 
 * @author jianghang 2012-7-20 下午06:01:34
 */
public class AviaterRegexFilter implements CanalEventFilter<String> {
    // 我们的配置的 binlog 过滤规则可以由多个正则表达式组成，使用逗号”,"进行分割
    private static final String             SPLIT             = ",";
    private static final String             PATTERN_SPLIT     = "|"; // 将经过逗号 ",” 分割后的过滤规则重新使用 "|" 串联起来
    private static final String             FILTER_EXPRESSION = "regex(pattern,target)"; // canal 定义的 Aviator 过滤表达式，使用了 regex 自定义函数，接受 pattern 和 target 两个参数
    private static final RegexFunction      regexFunction     = new RegexFunction(); // regex自定义函数实现，RegexFunction 的 getName 方法返回regex，call 方法接受两个参数
    private final Expression                exp               = AviatorEvaluator.compile(FILTER_EXPRESSION, true); // 对自定义表达式进行编译，得到 Expression 对象
    static {
        AviatorEvaluator.addFunction(regexFunction); // 将自定义函数添加到 AviatorEvaluator 中
    }
    // 用于比较两个字符串的大小
    private static final Comparator<String> COMPARATOR        = new StringComparator();
    // 用户设置的过滤规则，需要使用 SPLIT 进行分割
    final private String                    pattern;
    final private boolean                   defaultEmptyValue; // 在没有指定过滤规则 pattern 情况下的默认值，例如默认为 true，表示用户不指定过滤规则情况下，总是返回所有的 binlog event

    public AviaterRegexFilter(String pattern){
        this(pattern, true);
    }

    public AviaterRegexFilter(String pattern, boolean defaultEmptyValue){
        this.defaultEmptyValue = defaultEmptyValue; // 1. 给 defaultEmptyValue 字段赋值
        List<String> list = null; // 2. 给 pattern 字段赋值
        if (StringUtils.isEmpty(pattern)) {
            list = new ArrayList<>();
        } else {
            String[] ss = StringUtils.split(pattern, SPLIT); // 2.1 将传入 pattern 以逗号",”进行分割，放到 list 中；如果没有指定 pattern，则 list 为空，意味着不需要过滤
            list = Arrays.asList(ss);
        }

        // 2.2 对list中的pattern元素，按照从长到短的排序
        // 因为 foo|foot 匹配 foot 会出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot
        // 的长度不一样
        list.sort(COMPARATOR);
        // 2.3 对 pattern 进行头尾完全匹配
        list = completionPattern(list);
        this.pattern = StringUtils.join(list, PATTERN_SPLIT); // 2.4 将过滤规则重新使用"|"串联起来赋值给 pattern
    }
    // 1. 参数：前面已经分析过 parser 模块的 LogEventConvert 中，会将 binlog event 的 dbName+”."+tableName 当做参数过滤
    public boolean filter(String filtered) throws CanalFilterException {
        if (StringUtils.isEmpty(pattern)) { // 2. 如果没有指定匹配规则，返回默认值
            return defaultEmptyValue;
        }
        // 3. 如果需要过滤的 dbName+”.”+tableName 是一个空串，返回默认值
        if (StringUtils.isEmpty(filtered)) { // 提示：一些类型的 binlog event，如 heartbeat，并不是真正修改数据，这种类型的 event 是没有库名和表名的
            return defaultEmptyValue;
        }
        // 4. 将传入的 dbName+”."+tableName 通过 canal 自定义的 Aviator 扩展函数 RegexFunction 进行计算
        Map<String, Object> env = new HashMap<>();
        env.put("pattern", pattern);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }

    /**
     * 修复正则表达式匹配的问题，因为使用了 oro 的 matches，会出现：
     * 
     * <pre>
     * foo|foot 匹配 foot 出错，原因是 foot 匹配了 foo 之后，会返回 foo，但是 foo 的长度和 foot 的长度不一样
     * </pre>
     * 
     * 因此此类对正则表达式进行了从长到短的排序
     * 
     * @author zebin.xuzb 2012-10-22 下午2:02:26
     * @version 1.0.0
     */
    private static class StringComparator implements Comparator<String> {

        @Override
        public int compare(String str1, String str2) {
            return Integer.compare(str2.length(), str1.length());
        }
    }

    /**
     * 修复正则表达式匹配的问题，即使按照长度递减排序，还是会出现以下问题：
     * 
     * <pre>
     * foooo|f.*t 匹配 fooooot 出错，原因是 fooooot 匹配了 foooo 之后，会将 fooo 和数据进行匹配，但是 foooo 的长度和 fooooot 的长度不一样
     * </pre>
     * 
     * 因此此类对正则表达式进行头尾完全匹配
     * 
     * @author simon
     * @version 1.0.0
     */

    private List<String> completionPattern(List<String> patterns) {
        List<String> result = new ArrayList<>();
        for (String pattern : patterns) {
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("^");
            stringBuffer.append(pattern);
            stringBuffer.append("$");
            result.add(stringBuffer.toString());
        }
        return result;
    }

    @Override
    public String toString() {
        return pattern;
    }

}
