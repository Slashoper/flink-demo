// 1、根据时间进行分窗 : timewindow 1. Tumbling window 2.sliding window 3.session window
// 2、根据数据的数量进行分窗 : count window 1.Tumbling window 2.sliding window
// 

// stream.keyby() // 是Keyed 类型数据集
// .window() 指定窗口分配器类型
// .trigger() 指定触发器类型
// .evictor 指定 evictor或者不指定
// .allowedLateness 指定是否延迟处理数据
// .sideOutputLateData 指定Output Lag
// .reduce/aggregate/fold/apply() 指定窗口计算函数 ProcessWindowFunction (sum.max)

## window 的分类
``根据 是否keyby 来分为 keyed window / global window``
