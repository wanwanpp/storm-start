package com.wp.trident.wordcount;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
/**
 * <B>系统名称：</B>ResultFunction<BR>
 * <B>模块名称：</B>ResultFunction<BR>
 * <B>中文类名：</B>ResultFunction<BR>
 * <B>概要说明：</B>ResultFunction<BR>
 * @author bhz（Alienware）
 * @since 2013年5月2日
 */
public class ResultFunction extends BaseFunction {
	/** serialVersionUID */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		//获取tuple输入内容
		String sub = tuple.getStringByField("sub");
		Long count = tuple.getLongByField("count");
		System.out.println(sub +" : "+ count);
	}
}
