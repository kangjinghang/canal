package com.alibaba.otter.canal.filter.aviater;

import java.util.Map;

import org.apache.oro.text.regex.Perl5Matcher;

import com.alibaba.otter.canal.filter.PatternUtils;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;

/**
 * 提供aviator regex的代码扩展
 *
 * @author jianghang 2012-7-23 上午10:29:23
 */
public class RegexFunction extends AbstractFunction {
    // 实际上是根据配置的过滤规则 pattern，以及需要过滤的内容 text(即dbName+”.”+tableName)，通过 jarkata-oro 中 Perl5Matcher 类进行正则表达式匹配
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        String pattern = FunctionUtils.getStringValue(arg1, env);
        String text = FunctionUtils.getStringValue(arg2, env);
        Perl5Matcher matcher = new Perl5Matcher();
        boolean isMatch = matcher.matches(text, PatternUtils.getPattern(pattern));
        return AviatorBoolean.valueOf(isMatch);
    }

    public String getName() {
        return "regex";
    }

}
