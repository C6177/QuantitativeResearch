import akshare as ak
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta

# ==============================================
# 1. 获取所有主板A股股票代码和名称
# ==============================================
def get_all_main_board_stocks():
    print("正在获取主板A股列表...")
    # 使用 stock_info_sh_name_code 接口获取沪股列表，指定主板A股
    stock_list = ak.stock_info_sh_name_code(symbol="主板A股")
    # 提取证券代码和证券简称
    stock_list = stock_list[['证券代码', '证券简称']].rename(columns={'证券代码': 'code', '证券简称': 'name'})
    print(f"获取到 {len(stock_list)} 只主板A股股票")
    return stock_list

# ==============================================
# 2. 获取股票历史数据
# ==============================================
def get_stock_data(symbol, start_date, end_date):
    """
    使用 stock_zh_a_hist 获取股票历史数据
    """
    try:
        # 添加延迟，避免频繁请求被封
        time.sleep(0.5)
        # 参数说明:
        # symbol: 股票代码, 例如 "000001"
        # period: "daily" 日频
        # adjust: "hfq" 后复权 (用于计算指标更准确)
        df = ak.stock_zh_a_hist(symbol=symbol,
                                period="daily",
                                start_date=start_date,
                                end_date=end_date,
                                adjust="hfq")
        if df.empty:
            print(f"未获取到 {symbol} 的数据，请检查股票代码或日期范围。")
            return None
        # 将日期列转换为datetime格式，并设置为索引
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        # 确保数据按时间正序排列
        df.sort_index(inplace=True)
        # 保留需要的列，并重命名为英文，方便计算
        df = df[['开盘', '收盘', '涨跌幅', '涨跌额', '成交量', '成交额']]
        df.columns = ['open', 'close', 'change_pct', 'change_amt', 'volume', 'amount']
        # 成交量单位是手，转换为股 (1手=100股) 便于计算
        df['volume'] = df['volume'] * 100
        return df
    except Exception as e:
        print(f"获取数据时出错: {e}")
        # 出错后再延迟一下
        time.sleep(1)
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
    df['MA10'] = df['close'].rolling(window=10).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()

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
# 4. 技术指标筛选
# ==============================================
def technical_analysis(df):
    """
    基于最新交易日的数据，检查是否满足所有技术指标条件
    """
    if df is None or df.empty:
        return False

    # 获取最新数据 (最后一行)
    latest = df.iloc[-1]

    # 检查所有条件
    # 1. MA5 > MA10 (短期均线在中期之上)
    condition1 = latest['MA5'] > latest['MA10']
    # 2. MA5 > MA20 (短期均线在长期之上)
    condition2 = latest['MA5'] > latest['MA20']
    # 3. 收盘价 > MA20 (价格站在20日均线上方)
    condition3 = latest['close'] > latest['MA20']
    # 4. MACD柱为正 (DIF > DEA)
    condition4 = latest['DIF'] > latest['DEA']
    # 5. DIF > 0 (快线在零轴之上)
    condition5 = latest['DIF'] > 0
    # 6. RSI > 50 (处于强势区域)
    condition6 = latest['RSI'] > 50
    # 7. 价涨量增：今日成交量 > 20日均量
    condition7 = latest['volume'] > latest['VOL_MA20']

    # 判断是否满足所有条件
    all_conditions_met = all([condition1, condition2, condition3, condition4, condition5, condition6, condition7])

    return all_conditions_met

# ==============================================
# 5. 成长指标筛选
# ==============================================
def growth_analysis(code):
    """
    检查基本每股收益增长率-3年复合是否>10%，并且排名前20
    """
    try:
        # 构建完整的股票代码（添加市场前缀）
        full_code = f"SH{code}" if code.startswith('6') else f"SZ{code}"
        df = ak.stock_zh_growth_comparison_em(symbol=full_code)
        if df.empty:
            return False
        
        # 查找基本每股收益增长率-3年复合数据
        growth_data = df[df['指标名称'] == '基本每股收益增长率-3年复合']
        if growth_data.empty:
            return False
        
        # 获取增长率和排名
        growth_rate = growth_data.iloc[0]['最新']
        rank = growth_data.iloc[0]['排名']
        
        # 检查条件：增长率>10%且排名前20
        if growth_rate > 10 and rank <= 20:
            return True
        return False
    except Exception as e:
        print(f"获取成长数据时出错: {e}")
        return False

# ==============================================
# 6. 规模指标筛选
# ==============================================
def scale_analysis(code):
    """
    检查营业收入排名是否>=10
    """
    try:
        # 构建完整的股票代码（添加市场前缀）
        full_code = f"SH{code}" if code.startswith('6') else f"SZ{code}"
        df = ak.stock_zh_scale_comparison_em(symbol=full_code)
        if df.empty:
            return False
        
        # 查找营业收入排名数据
        scale_data = df[df['指标名称'] == '营业收入']
        if scale_data.empty:
            return False
        
        # 获取排名
        rank = scale_data.iloc[0]['排名']
        
        # 检查条件：排名>=10
        if rank >= 10:
            return True
        return False
    except Exception as e:
        print(f"获取规模数据时出错: {e}")
        return False

# ==============================================
# 7. 主程序：执行分析
# ==============================================
if __name__ == "__main__":
    # 配置参数

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=20)).strftime("%Y%m%d")

    print(f"开始分析所有主板A股股票从 {start_date} 到 {end_date}...")

    # 1. 获取所有主板A股股票列表
    stock_list = get_all_main_board_stocks()
    total_stocks = len(stock_list)
    print(f"开始分析 {total_stocks} 只股票...")

    # 存储筛选结果
    technical_selected = []
    growth_selected = []
    scale_selected = []

    # 分批处理，每次处理50只股票
    batch_size = 50
    for i in range(0, total_stocks, batch_size):
        batch_end = min(i + batch_size, total_stocks)
        print(f"\n处理第 {i+1}-{batch_end} 只股票...")
        
        # 处理当前批次的股票
        for index in range(i, batch_end):
            row = stock_list.iloc[index]
            code = row['code']
            name = row['name']
            print(f"\n分析第 {index+1}/{total_stocks} 只股票: {code} {name}")

            # 2. 获取股票数据
            stock_data = get_stock_data(code, start_date, end_date)

            if stock_data is not None:
                print(f"成功获取 {len(stock_data)} 条日线数据。")

                # 3. 计算指标
                stock_data_with_indicators = calculate_indicators(stock_data)

                # 4. 技术指标筛选
                if technical_analysis(stock_data_with_indicators):
                    technical_selected.append({"code": code, "name": name})
                    print(f"✓ 股票 {code} {name} 满足技术指标条件，已加入筛选结果")
        
        # 批次处理完成后，添加较长的延迟
        print(f"第 {i+1}-{batch_end} 只股票处理完成，休息3秒...")
        time.sleep(3)

    # 5. 输出技术指标筛选结果
    if technical_selected:
        print(f"\n技术指标筛选完成，符合条件的股票有 {len(technical_selected)} 只")
        
        # 创建结果DataFrame
        technical_df = pd.DataFrame(technical_selected)
        
        # 保存为Excel文件
        output_path1 = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/newselector-1.xlsx"
        technical_df.to_excel(output_path1, index=False)
        print(f"已将技术指标筛选结果保存到: {output_path1}")
        
        # 打印前几行结果
        print("\n技术指标筛选结果预览:")
        print(technical_df.head())
    else:
        print("\n没有符合技术指标条件的股票")

    # 6. 成长指标筛选
    if technical_selected:
        print("\n开始进行成长指标筛选...")
        for stock in technical_selected:
            code = stock['code']
            name = stock['name']
            print(f"分析股票: {code} {name}")
            if growth_analysis(code):
                growth_selected.append(stock)
                print(f"✓ 股票 {code} {name} 满足成长指标条件，已加入筛选结果")
        
        # 输出成长指标筛选结果
        if growth_selected:
            print(f"\n成长指标筛选完成，符合条件的股票有 {len(growth_selected)} 只")
            
            # 创建结果DataFrame
            growth_df = pd.DataFrame(growth_selected)
            
            # 保存为Excel文件
            output_path2 = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/newselector-2.xlsx"
            growth_df.to_excel(output_path2, index=False)
            print(f"已将成长指标筛选结果保存到: {output_path2}")
            
            # 打印前几行结果
            print("\n成长指标筛选结果预览:")
            print(growth_df.head())
        else:
            print("\n没有符合成长指标条件的股票")
    else:
        print("\n没有技术指标筛选结果，跳过成长指标筛选")

    # 7. 规模指标筛选
    if growth_selected:
        print("\n开始进行规模指标筛选...")
        for stock in growth_selected:
            code = stock['code']
            name = stock['name']
            print(f"分析股票: {code} {name}")
            if scale_analysis(code):
                scale_selected.append(stock)
                print(f"✓ 股票 {code} {name} 满足规模指标条件，已加入筛选结果")
        
        # 输出规模指标筛选结果
        if scale_selected:
            print(f"\n规模指标筛选完成，符合条件的股票有 {len(scale_selected)} 只")
            
            # 创建结果DataFrame
            scale_df = pd.DataFrame(scale_selected)
            
            # 保存为Excel文件
            output_path3 = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/newselector-3.xlsx"
            scale_df.to_excel(output_path3, index=False)
            print(f"已将规模指标筛选结果保存到: {output_path3}")
            
            # 打印前几行结果
            print("\n规模指标筛选结果预览:")
            print(scale_df.head())
        else:
            print("\n没有符合规模指标条件的股票")
    else:
        print("\n没有成长指标筛选结果，跳过规模指标筛选")
