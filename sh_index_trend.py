"""
上证指数趋势判断 - 基于均线形态+成交量验证的量化模型
使用AKShare获取数据，输出趋势判断结果
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time


def get_sh_index_trend_analysis():
    """
    获取上证指数趋势分析结果
    基于：均线排列 + 20日线方向 + 股价位置 + 成交量配合 的四维评分模型
    
    Returns:
        dict: 包含各项评分、总分和趋势判断的字典
    """
    print("=" * 60)
    print("上证指数趋势分析系统")
    print("=" * 60)
    
    # 1. 获取上证指数历史数据
    try:
        # 获取最近120天的数据（确保有足够数据计算60日均线）
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        
        print(f"正在获取上证指数数据 ({start_date} 至 {end_date})...")
        
        # 调用AKShare接口获取上证指数数据
        df = ak.stock_zh_a_hist(
            symbol="000001",
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        
        if df.empty:
            print("❌ 获取数据失败：返回为空")
            return None
        
        print(f"✅ 成功获取 {len(df)} 条数据")
        
        # 重命名列名
        df = df.rename(columns={
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '涨跌幅': 'pct_chg'
        })
        
        # 将日期列转换为datetime类型
        df['date'] = pd.to_datetime(df['date'])
        
        # 按日期升序排列
        df = df.sort_values('date')
        df = df.reset_index(drop=True)
        
        print(f"数据范围: {df['date'].iloc[0].strftime('%Y-%m-%d')} 至 {df['date'].iloc[-1].strftime('%Y-%m-%d')}")
        
    except Exception as e:
        print(f"❌ 获取数据失败: {e}")
        return None
    
    # 2. 计算各项技术指标
    df = calculate_technical_indicators(df)
    
    # 3. 获取最新一天的数据
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    
    # 4. 计算各项评分
    scores = calculate_trend_scores(df, latest, prev)
    
    # 5. 输出详细结果
    print_trend_analysis(scores, latest, df)
    
    return scores


def calculate_technical_indicators(df):
    """计算各项技术指标"""
    
    # 计算均线
    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA10'] = df['close'].rolling(window=10).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['MA30'] = df['close'].rolling(window=30).mean()
    df['MA60'] = df['close'].rolling(window=60).mean()
    
    # 计算均线斜率（使用线性回归或简单差分）
    # 这里使用5日变化率近似表示斜率
    df['MA20_slope'] = df['MA20'].pct_change(5) * 100  # 5日变化百分比
    
    # 计算成交量均线
    df['VOL_MA5'] = df['volume'].rolling(window=5).mean()
    
    # 计算价格与均线的距离
    df['dist_to_MA20'] = (df['close'] - df['MA20']) / df['MA20'] * 100
    
    return df


def calculate_trend_scores(df, latest, prev):
    """根据量化判断表计算各项评分"""
    
    scores = {}
    
    # ========== 1. 均线排列评分 ==========
    # 获取最新的均线值
    ma5, ma10, ma20, ma30, ma60 = latest['MA5'], latest['MA10'], latest['MA20'], latest['MA30'], latest['MA60']
    
    # 判断均线排列
    if ma5 > ma10 > ma20 > ma30 > ma60:
        scores['排列评分'] = 2
        scores['排列描述'] = "标准多头排列 (5>10>20>30>60)"
    elif ma20 > ma30 > ma60 and not (ma5 > ma10 > ma20):  # 中期多头但短期混乱
        scores['排列评分'] = 1
        scores['排列描述'] = "中期多头排列，短期纠缠"
    elif ma60 > ma30 > ma20 > ma10 > ma5:
        scores['排列评分'] = -2
        scores['排列描述'] = "标准空头排列 (60>30>20>10>5)"
    elif ma60 > ma30 > ma20 and not (ma10 > ma5 > ma20):  # 中期空头但短期反弹
        scores['排列评分'] = -1
        scores['排列描述'] = "中期空头排列，短期反弹"
    else:
        # 检查是否均线粘合
        ma_values = [ma5, ma10, ma20, ma30, ma60]
        max_ma = max(ma_values)
        min_ma = min(ma_values)
        if (max_ma - min_ma) / min_ma < 0.03:  # 均线最大差距小于3%
            scores['排列评分'] = 0
            scores['排列描述'] = "均线粘合缠绕"
        else:
            scores['排列评分'] = 0
            scores['排列描述'] = "无明显排列规律"
    
    # ========== 2. 20日线方向评分 ==========
    ma20_slope = latest['MA20_slope']
    
    if not pd.isna(ma20_slope):
        if ma20_slope > 3:  # 5日涨幅>3%，斜率较陡
            scores['20线方向评分'] = 2
            scores['20线描述'] = f"向上且斜率较大 ({ma20_slope:.2f}%)"
        elif ma20_slope > 0:
            scores['20线方向评分'] = 1
            scores['20线描述'] = f"向上 ({ma20_slope:.2f}%)"
        elif ma20_slope > -0.5:
            scores['20线方向评分'] = 0
            scores['20线描述'] = f"走平 ({ma20_slope:.2f}%)"
        elif ma20_slope > -3:
            scores['20线方向评分'] = -1
            scores['20线描述'] = f"向下 ({ma20_slope:.2f}%)"
        else:
            scores['20线方向评分'] = -2
            scores['20线描述'] = f"向下且斜率较大 ({ma20_slope:.2f}%)"
    else:
        scores['20线方向评分'] = 0
        scores['20线描述'] = "数据不足"
    
    # ========== 3. 股价位置评分 ==========
    close = latest['close']
    
    if close > latest['MA60'] and close > latest['MA30'] and close > latest['MA20']:
        scores['位置评分'] = 2
        scores['位置描述'] = "在所有均线上方"
    elif close > latest['MA20'] and close < latest['MA60']:
        scores['位置评分'] = 1
        scores['位置描述'] = "在20-60线之间"
    elif close > latest['MA20'] and close < latest['MA30']:
        scores['位置评分'] = 1
        scores['位置描述'] = "在20-30线之间"
    elif close < latest['MA20'] and close > latest['MA60']:
        scores['位置评分'] = -1
        scores['位置描述'] = "在20-60线下方"
    elif close < latest['MA20'] and close < latest['MA30'] and close < latest['MA60']:
        scores['位置评分'] = -2
        scores['位置描述'] = "在所有均线下方"
    else:
        # 检查是否在均线之间穿梭
        ma_values = [latest['MA5'], latest['MA10'], latest['MA20'], latest['MA30'], latest['MA60']]
        if min(ma_values) < close < max(ma_values):
            scores['位置评分'] = 0
            scores['位置描述'] = "在均线之间穿梭"
        else:
            scores['位置评分'] = 0
            scores['位置描述'] = "位置不明确"
    
    # ========== 4. 成交量配合评分 ==========
    # 获取最近10天的数据
    last_10 = df.tail(10)
    
    # 计算上涨日和下跌日的成交量对比
    up_days = last_10[last_10['pct_chg'] > 0]
    down_days = last_10[last_10['pct_chg'] < 0]
    
    up_vol_avg = up_days['volume'].mean() if len(up_days) > 0 else 0
    down_vol_avg = down_days['volume'].mean() if len(down_days) > 0 else 0
    vol_ma5 = latest['VOL_MA5']
    
    # 判断成交量特征
    if up_vol_avg > down_vol_avg * 1.2 and latest['volume'] > vol_ma5 * 0.8:
        scores['成交量评分'] = 2
        scores['成交量描述'] = "上涨放量、回调缩量"
    elif up_vol_avg > down_vol_avg:
        scores['成交量评分'] = 1
        scores['成交量描述'] = "量能温和，上涨量略大"
    elif abs(up_vol_avg - down_vol_avg) / max(up_vol_avg, down_vol_avg) < 0.1:
        scores['成交量评分'] = 0
        scores['成交量描述'] = "持续缩量，无明显变化"
    elif down_vol_avg > up_vol_avg * 1.2:
        scores['成交量评分'] = -1
        scores['成交量描述'] = "下跌放量、上涨缩量"
    elif latest['volume'] > vol_ma5 * 1.5 and abs(latest['pct_chg']) < 1:
        scores['成交量评分'] = -2
        scores['成交量描述'] = "异常放量滞涨/下跌"
    else:
        scores['成交量评分'] = 0
        scores['成交量描述'] = "成交量无明显特征"
    
    # 计算总分
    scores['总分'] = (scores['排列评分'] + 
                      scores['20线方向评分'] + 
                      scores['位置评分'] + 
                      scores['成交量评分'])
    
    # 判断趋势
    if scores['总分'] >= 5:
        scores['趋势判断'] = "上涨趋势"
        scores['操作建议'] = "持股待涨，回调加仓"
        scores['风险提示'] = "关注成交量是否持续配合"
    elif scores['总分'] >= 2:
        scores['趋势判断'] = "上涨震荡"
        scores['操作建议'] = "高抛低吸，不追高"
        scores['风险提示'] = "注意20日线支撑，跌破需谨慎"
    elif scores['总分'] >= -1:
        scores['趋势判断'] = "横盘震荡"
        scores['操作建议'] = "观望为主，等待方向选择"
        scores['风险提示'] = "放量突破箱体前不参与"
    elif scores['总分'] >= -4:
        scores['趋势判断'] = "下跌震荡"
        scores['操作建议'] = "逢高减仓，控制仓位"
        scores['风险提示'] = "反弹至20日线注意减仓"
    else:
        scores['趋势判断'] = "下跌趋势"
        scores['操作建议'] = "空仓回避，不接飞刀"
        scores['风险提示'] = "等待底部信号出现"
    
    return scores


def print_trend_analysis(scores, latest, df):
    """打印趋势分析结果"""
    
    print("\n" + "=" * 60)
    print("📊 【上证指数趋势分析报告】")
    print("=" * 60)
    
    print(f"\n📅 分析日期: {latest['date'].strftime('%Y-%m-%d')}")
    print(f"📈 最新收盘: {latest['close']:.2f}")
    print(f"📉 今日涨跌幅: {latest['pct_chg']:.2f}%")
    
    print("\n【均线系统】")
    print(f"  MA5: {latest['MA5']:.2f}")
    print(f"  MA10: {latest['MA10']:.2f}")
    print(f"  MA20: {latest['MA20']:.2f}")
    print(f"  MA30: {latest['MA30']:.2f}")
    print(f"  MA60: {latest['MA60']:.2f}")
    
    print("\n【四维评分】")
    print(f"  1. 均线排列: {scores['排列评分']}分 - {scores['排列描述']}")
    print(f"  2. 20线方向: {scores['20线方向评分']}分 - {scores['20线描述']}")
    print(f"  3. 股价位置: {scores['位置评分']}分 - {scores['位置描述']}")
    print(f"  4. 成交量配合: {scores['成交量评分']}分 - {scores['成交量描述']}")
    
    print(f"\n【总分】: {scores['总分']}分")
    
    print("\n【最终判断】")
    print(f"  🎯 趋势状态: {scores['趋势判断']}")
    print(f"  💡 操作建议: {scores['操作建议']}")
    print(f"  ⚠️  风险提示: {scores['风险提示']}")
    
    # 额外信息：最近5日涨跌情况
    print("\n【最近5日表现】")
    last_5 = df.tail(5)
    for i, row in last_5.iterrows():
        arrow = "↑" if row['pct_chg'] > 0 else "↓" if row['pct_chg'] < 0 else "→"
        print(f"  {row['date'].strftime('%m-%d')}: {row['close']:.2f} {arrow} {row['pct_chg']:+.2f}%")


def get_trend_signal():
    """
    简化版：只返回趋势信号，供其他程序调用
    Returns:
        str: 趋势信号 (strong_buy/buy/neutral/sell/strong_sell)
    """
    scores = get_sh_index_trend_analysis()
    
    if scores is None:
        return "unknown"
    
    if scores['总分'] >= 5:
        return "strong_buy"
    elif scores['总分'] >= 2:
        return "buy"
    elif scores['总分'] >= -1:
        return "neutral"
    elif scores['总分'] >= -4:
        return "sell"
    else:
        return "strong_sell"


# ==================== 主程序 ====================
if __name__ == "__main__":
    # 运行趋势分析
    result = get_sh_index_trend_analysis()
    
    print("\n" + "=" * 60)
    print("📝 量化判断表对照")
    print("=" * 60)
    print("""
    评分标准对照表:
    ┌──────────────┬─────────────────────────────┐
    │  总分范围    │        趋势判断              │
    ├──────────────┼─────────────────────────────┤
    │   5 ~ 8分    │  上涨趋势  (持股待涨)        │
    │   2 ~ 4分    │  上涨震荡  (高抛低吸)        │
    │  -1 ~ 1分    │  横盘震荡  (观望等待)        │
    │  -4 ~ -2分   │  下跌震荡  (逢高减仓)        │
    │  -8 ~ -5分   │  下跌趋势  (空仓回避)        │
    └──────────────┴─────────────────────────────┘
    """)
    
    print("\n⚠️ 免责声明：本分析仅供参考，不构成投资建议")
    print("   市场有风险，投资需谨慎")