import akshare as ak
import pandas as pd
import numpy as np

# ==============================================
# 1. 获取所有A股股票代码和名称
# ==============================================
def get_all_a_stocks():
    print("正在获取A股列表...")
    # 使用 stock_info_sh_name_code 接口获取沪股列表
    print("使用 stock_info_sh_name_code 接口...")
    stock_list = ak.stock_info_sh_name_code()
    # 提取证券代码和证券简称
    stock_list = stock_list[['证券代码', '证券简称']].rename(columns={'证券代码': 'code', '证券简称': 'name'})
    print(f"获取到 {len(stock_list)} 只沪股股票")
    return stock_list

# ==============================================
# 2. 获取股票历史数据 (核心接口)
# ==============================================
def get_stock_data(symbol, start_date, end_date):
    """
    使用 stock_zh_a_hist 获取股票历史数据
    """
    try:
        # 参数说明:
        # symbol: 股票代码, 例如 "000001"
        # period: "daily" 日频
        # adjust: "hfq" 后复权 (用于计算指标更准确), 也可用 "qfq" 前复权
        df = ak.stock_zh_a_hist(symbol=symbol,
                                period="daily",
                                start_date=start_date,
                                end_date=end_date,
                                adjust="hfq")  # 推荐使用后复权数据进行技术分析
        if df.empty:
            print(f"未获取到 {symbol} 的数据，请检查股票代码或日期范围。")
            return None
        # 将日期列转换为datetime格式，并设置为索引
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        # 确保数据按时间正序排列
        df.sort_index(inplace=True)
        # 只保留需要的列，并重命名为英文，方便计算
        df = df[['开盘', '收盘', '最高', '最低', '成交量']]
        df.columns = ['open', 'close', 'high', 'low', 'volume']
        # 成交量单位是手，转换为股 (1手=100股) 便于计算，也可以不转换，保持一致性即可
        df['volume'] = df['volume'] * 100
        return df
    except Exception as e:
        print(f"获取数据时出错: {e}")
        return None

# ==============================================
# 3. 计算技术指标
# ==============================================
def calculate_indicators(df):
    """
    计算移动平均线、MACD、RSI等指标
    """
    df = df.copy()

    # --- 移动平均线 ---
    df['MA5'] = df['close'].rolling(window=5).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['MA60'] = df['close'].rolling(window=60).mean()

    # --- MACD (指数平滑移动平均线) ---
    exp12 = df['close'].ewm(span=12, adjust=False).mean()
    exp26 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp12 - exp26
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_bar'] = 2 * (df['DIF'] - df['DEA'])

    # --- RSI (相对强弱指标) ---
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # --- 成交量均线 ---
    df['VOL_MA20'] = df['volume'].rolling(window=20).mean()

    return df

# ==============================================
# 4. 趋势判断与打分系统
# ==============================================
def trend_scoring_system(df):
    """
    基于最新交易日的数据，根据多个条件进行打分
    """
    if df is None or df.empty:
        return None

    # 获取最新数据 (最后一行)
    latest = df.iloc[-1]
    # 获取前一日数据，用于计算某些条件的变动
    prev = df.iloc[-2] if len(df) > 1 else latest

    score = 0
    reasons = []

    # --- 趋势类 (满分3分) ---
    # 1. MA5 > MA20 (短期均线在中期之上)
    if latest['MA5'] > latest['MA20']:
        score += 1
        reasons.append("MA5 > MA20")
    # 2. MA20 > MA60 (中期均线在长期之上，多头排列)
    if latest['MA20'] > latest['MA60']:
        score += 1
        reasons.append("MA20 > MA60")
    # 3. 收盘价 > MA20 (价格站在20日均线上方)
    if latest['close'] > latest['MA20']:
        score += 1
        reasons.append("Close > MA20")

    # --- 动量类 (满分3分) ---
    # 4. MACD柱为正 (DIF > DEA)
    if latest['DIF'] > latest['DEA']:
        score += 1
        reasons.append("MACD柱为正")
    # 5. DIF > 0 (快线在零轴之上)
    if latest['DIF'] > 0:
        score += 1
        reasons.append("DIF > 0")
    # 6. RSI > 50 (处于强势区域)
    if latest['RSI'] > 50:
        # 可选：同时避免严重超买 RSI < 80，可根据需要调整
        if latest['RSI'] < 80:
            score += 1
            reasons.append("RSI在50-80之间")
        else:
            # 如果RSI > 80，可能过热，但仍算强势，给0.5分或1分？这里保守给0.5分
            score += 0.5
            reasons.append("RSI > 80 (超买区，注意风险)")
    else:
        # RSI <= 50，不加分
        pass

    # --- 能量类 (满分1分) ---
    # 7. 价涨量增：今日成交量 > 20日均量 且 今日收盘价 > 昨日收盘价
    volume_condition = (latest['volume'] > latest['VOL_MA20']) and (latest['close'] > prev['close'])
    if volume_condition:
        score += 1
        reasons.append("价涨量增")
    elif latest['volume'] > latest['VOL_MA20']:
        # 如果仅放量但价格未涨，不加分，但可以记录
        reasons.append("放量但价未涨")
    else:
        reasons.append("量能不足")

    # 综合判断
    max_score = 7  # 理论最高分
    score_percentage = (score / max_score) * 100

    result = {
        'score': round(score, 2),
        'score_percentage': round(score_percentage, 2),
        'reasons': reasons,
        'latest_close': latest['close'],
        'latest_date': df.index[-1].strftime('%Y-%m-%d')
    }

    # 根据得分率判断趋势强度
    if score_percentage >= 80:
        result['trend_judgment'] = "强势向上"
    elif score_percentage >= 60:
        result['trend_judgment'] = "震荡偏强"
    elif score_percentage >= 40:
        result['trend_judgment'] = "震荡或弱势"
    else:
        result['trend_judgment'] = "明显弱势"

    return result

# ==============================================
# 5. 主程序：执行分析
# ==============================================
if __name__ == "__main__":
    # 配置参数
    start = "20251201"     # 分析起始日期
    end = "20260310"       # 分析结束日期

    print(f"开始分析所有A股股票从 {start} 到 {end}...")

    # 1. 获取所有A股股票列表
    stock_list = get_all_a_stocks()
    total_stocks = len(stock_list)
    print(f"开始分析 {total_stocks} 只股票...")

    # 存储筛选结果
    selected_stocks = []

    # 遍历所有股票
    for index, row in stock_list.iterrows():
        code = row['code']
        name = row['name']
        print(f"\n分析第 {index+1}/{total_stocks} 只股票: {code} {name}")

        # 2. 获取股票数据
        stock_data = get_stock_data(code, start, end)

        if stock_data is not None:
            print(f"成功获取 {len(stock_data)} 条日线数据。")

            # 3. 计算指标
            stock_data_with_indicators = calculate_indicators(stock_data)

            # 4. 趋势打分
            analysis_result = trend_scoring_system(stock_data_with_indicators)

            # 5. 筛选结果
            if analysis_result:
                trend = analysis_result['trend_judgment']
                score = analysis_result['score']
                print(f"趋势判断: {trend}, 得分: {score}")
                
                # 筛选出强势向上和震荡偏强的股票
                if trend in ["强势向上", "震荡偏强"]:
                    selected_stocks.append({
                        'code': code,
                        'name': name,
                        'trend_judgment': trend,
                        'score': score
                    })
                    print(f"✓ 股票 {code} {name} 符合条件，已加入筛选结果")
        else:
            print(f"✗ 股票 {code} {name} 数据获取失败，跳过")

    # 6. 输出结果到Excel
    if selected_stocks:
        print(f"\n筛选完成，符合条件的股票有 {len(selected_stocks)} 只")
        
        # 创建结果DataFrame
        result_df = pd.DataFrame(selected_stocks)
        
        # 保存为Excel文件
        output_path = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/evaluate.xlsx"
        result_df.to_excel(output_path, index=False)
        print(f"已将筛选结果保存到: {output_path}")
        
        # 打印前几行结果
        print("\n筛选结果预览:")
        print(result_df.head())
    else:
        print("\n没有符合条件的股票")