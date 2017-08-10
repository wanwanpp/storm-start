package com.wp.trident.wordcount;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
/**
 * <B>系统名称：</B>SplitFunction<BR>
 * <B>模块名称：</B>SplitFunction<BR>
 * <B>中文类名：</B>SplitFunction<BR>
 * <B>概要说明：</B>Trident Function As Storm Bolt : Execute<BR>
 * @author bhz（Alienware）
 * @since 2013年5月2日
 */
public class SplitFunction extends BaseFunction {
	/** serialVersionUID */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String subjects = tuple.getStringByField("subjects");
		//获取tuple输入内容
		//逻辑处理，然后发射给下一个组件
		for(String sub : subjects.split(" ")) {
			collector.emit(new Values(sub));
		}
	}
}
