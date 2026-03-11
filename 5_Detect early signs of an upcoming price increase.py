import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入 stock_anaylse 模块
import stock_anaylse

class EarlyTrendDetector:
    """
    早期上涨趋势发现器
    基于四个核心维度：资金、形态、指标、盘口
    """
    
    def __init__(self, stock_code):
        """初始化，设置股票代码"""
        self.stock_code = stock_code
        # 自动判断市场标识（用于部分接口）
        if stock_code.startswith('6'):
            self.market = 'sh'
        else:
            self.market = 'sz'
    
    # ==================== 1. 资金条件 ====================
    def check_fund_condition(self, days=10, main_flow_threshold=0.3):
        """
        条件1：最近10日，主力净流入占比 > 0.3%，且下跌日主力未明显流出
        """
        try:
            # 获取资金流向数据
            fund_df = ak.stock_individual_fund_flow(
                stock=self.stock_code, 
                market=self.market
            )
            
            # 取最近days天数据，按日期正序排列
            fund_df = fund_df.head(days).iloc[::-1].reset_index(drop=True)
            
            # 转换数值列
            fund_df['主力净流入-净额'] = pd.to_numeric(fund_df['主力净流入-净额'], errors='coerce')
            fund_df['主力净流入-净占比'] = pd.to_numeric(fund_df['主力净流入-净占比'], errors='coerce')
            fund_df['涨跌幅'] = pd.to_numeric(fund_df['涨跌幅'], errors='coerce')
            
            # 条件1.1：主力净流入占比均值 > 阈值
            avg_main_ratio = fund_df['主力净流入-净占比'].mean()
            
            # 条件1.2：下跌日主力未明显流出（下跌日主力净流出小于平均流入的绝对值）
            down_days = fund_df[fund_df['涨跌幅'] < 0]
            if len(down_days) > 0:
                avg_down_flow = down_days['主力净流入-净额'].mean()
                avg_up_flow = fund_df[fund_df['涨跌幅'] > 0]['主力净流入-净额'].mean()
                # 下跌日流出不超过上涨日平均流入的30%
                flow_condition = abs(avg_down_flow) < abs(avg_up_flow) * 0.3 if not pd.isna(avg_up_flow) else False
            else:
                flow_condition = True  # 没有下跌日，视为满足
            
            print(f"📊 资金条件诊断:")
            print(f"   - 近{days}日主力净流入平均占比: {avg_main_ratio:.2f}%")
            print(f"   - 下跌日主力净流出控制: {'✓' if flow_condition else '✗'}")
            
            condition_met = avg_main_ratio > main_flow_threshold and flow_condition
            return condition_met, {
                'avg_main_ratio': avg_main_ratio,
                'flow_condition': flow_condition,
                'data': fund_df
            }
        except Exception as e:
            print(f"资金条件分析出错: {e}")
            return False, {}
    
    # ==================== 2. 形态条件 ====================
    def check_pattern_condition(self, days=20, amplitude_threshold=20, ma_period=20):
        """
        条件2：最近20日，股价在区间内震荡（最高/最低 < 20%），且今日收盘价站上20日均线
        """
        try:
            # 获取历史数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')  # 多取一些数据用于计算均线
            
            hist_df = ak.stock_zh_a_hist(
                symbol=self.stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            if hist_df.empty:
                return False, {}
            
            # 数据预处理
            hist_df['日期'] = pd.to_datetime(hist_df['日期'])
            hist_df.set_index('日期', inplace=True)
            hist_df.sort_index(inplace=True)
            
            # 取最近days天
            recent_df = hist_df.tail(days)
            
            # 条件2.1：震荡幅度 < 阈值
            highest = recent_df['最高'].max()
            lowest = recent_df['最低'].min()
            amplitude = ((highest - lowest) / lowest) * 100
            amplitude_condition = amplitude < amplitude_threshold
            
            # 计算20日均线
            hist_df['MA20'] = hist_df['收盘'].rolling(window=20).mean()
            
            # 条件2.2：今日收盘价站上20日均线
            latest_close = hist_df['收盘'].iloc[-1]
            latest_ma20 = hist_df['MA20'].iloc[-1]
            ma_condition = latest_close > latest_ma20
            
            print(f"📈 形态条件诊断:")
            print(f"   - 近{days}日震荡幅度: {amplitude:.2f}% (阈值: {amplitude_threshold}%)")
            print(f"   - 今日收盘价 {latest_close:.2f} {'>' if ma_condition else '<'} MA20 {latest_ma20:.2f}")
            
            condition_met = amplitude_condition and ma_condition
            return condition_met, {
                'amplitude': amplitude,
                'ma_condition': ma_condition,
                'latest_close': latest_close,
                'latest_ma20': latest_ma20,
                'data': hist_df
            }
        except Exception as e:
            print(f"形态条件分析出错: {e}")
            return False, {}
    
    # ==================== 3. 指标条件 ====================
    def check_indicator_condition(self):
        """
        条件3：MACD出现底背离，DIF线上穿零轴
        """
        try:
            # 获取更多历史数据用于背离识别
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=180)).strftime('%Y%m%d')
            
            df = ak.stock_zh_a_hist(
                symbol=self.stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if df.empty or len(df) < 50:
                return False, {}
            
            # 数据预处理
            df['日期'] = pd.to_datetime(df['日期'])
            df.set_index('日期', inplace=True)
            df.sort_index(inplace=True)
            
            # 计算MACD
            close_prices = df['收盘'].values
            
            # 手动计算MACD
            exp12 = df['收盘'].ewm(span=12, adjust=False).mean()
            exp26 = df['收盘'].ewm(span=26, adjust=False).mean()
            df['DIF'] = exp12 - exp26
            df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
            df['MACD'] = 2 * (df['DIF'] - df['DEA'])
            
            # 取最近60天数据判断背离
            recent = df.tail(60).copy()
            
            # 条件3.1：DIF线上穿零轴
            latest_dif = recent['DIF'].iloc[-1]
            prev_dif = recent['DIF'].iloc[-2]
            cross_zero_condition = latest_dif > 0 and prev_dif <= 0
            
            # 条件3.2：底背离检测（股价创新低但DIF未创新低）
            # 找到最近一段时间的低点
            recent_low_idx = recent['收盘'].idxmin()
            recent_low_price = recent.loc[recent_low_idx, '收盘']
            recent_low_dif = recent.loc[recent_low_idx, 'DIF']
            
            # 找到之前一段时间的低点
            earlier = df.tail(120).head(60)
            earlier_low_idx = earlier['收盘'].idxmin()
            earlier_low_price = earlier.loc[earlier_low_idx, '收盘']
            earlier_low_dif = earlier.loc[earlier_low_idx, 'DIF']
            
            # 底背离：近期股价更低，但DIF更高
            divergence_condition = (
                recent_low_price < earlier_low_price and 
                recent_low_dif > earlier_low_dif
            )
            
            print(f"📐 指标条件诊断:")
            print(f"   - DIF上穿零轴: {'✓' if cross_zero_condition else '✗'}")
            print(f"   - MACD底背离: {'✓' if divergence_condition else '✗'}")
            
            condition_met = cross_zero_condition or divergence_condition
            return condition_met, {
                'cross_zero': cross_zero_condition,
                'divergence': divergence_condition,
                'latest_dif': latest_dif,
                'data': df
            }
        except Exception as e:
            print(f"指标条件分析出错: {e}")
            return False, {}
    
    # ==================== 4. 盘口条件 ====================
    def check_tick_condition(self, volume_multiplier=2, min_aggressive_orders=10):
        """
        条件4：盘中连续出现攻击性买单
        volume_multiplier: 攻击性买单定义为大于平均手数的倍数
        min_aggressive_orders: 最少攻击性买单数量
        """
        try:
            # 获取日内分时数据
            tick_df = ak.stock_intraday_em(symbol=self.stock_code)
            
            if tick_df.empty:
                return False, {}
            
            # 筛选买盘
            buy_orders = tick_df[tick_df['买卖盘性质'] == '买盘'].copy()
            
            if len(buy_orders) == 0:
                return False, {}
            
            # 将手数转换为数值
            buy_orders['手数'] = pd.to_numeric(buy_orders['手数'], errors='coerce')
            
            # 计算平均买盘手数
            avg_volume = buy_orders['手数'].mean()
            
            # 定义攻击性买单：手数大于平均值的倍数
            buy_orders['is_aggressive'] = buy_orders['手数'] > avg_volume * volume_multiplier
            
            # 统计攻击性买单
            aggressive_orders = buy_orders[buy_orders['is_aggressive']]
            
            # 检测是否连续出现（例如，在30分钟内出现多次）
            if len(aggressive_orders) >= min_aggressive_orders:
                # 检查时间分布（可选）
                times = aggressive_orders['时间'].tolist()
                is_continuous = True  # 简化处理，可进一步优化
            else:
                is_continuous = False
            
            print(f"⏱️ 盘口条件诊断:")
            print(f"   - 总买盘笔数: {len(buy_orders)}")
            print(f"   - 攻击性买单笔数: {len(aggressive_orders)} (阈值: {min_aggressive_orders})")
            if len(aggressive_orders) > 0:
                print(f"   - 平均买盘手数: {avg_volume:.0f}手")
                print(f"   - 最大攻击性买单: {aggressive_orders['手数'].max()}手")
            
            condition_met = len(aggressive_orders) >= min_aggressive_orders
            return condition_met, {
                'aggressive_count': len(aggressive_orders),
                'avg_volume': avg_volume,
                'aggressive_orders': aggressive_orders.head(5) if not aggressive_orders.empty else None
            }
        except Exception as e:
            print(f"盘口条件分析出错: {e}")
            return False, {}
    
    # # ==================== 综合评分系统 ====================
    def save_top3_conditions_stocks(self, stock_name):
        """
        检查前3个条件，如果都满足则保存到Excel文件
        """
        print(f"\n{'='*60}")
        print(f"【早期上涨趋势发现器】股票代码: {self.stock_code}, 名称: {stock_name}")
        print(f"{'='*60}")
        
        # 检查前3个条件
        fund_met, fund_info = self.check_fund_condition()
        pattern_met, pattern_info = self.check_pattern_condition()
        indicator_met, indicator_info = self.check_indicator_condition()
        
        # 输出每个条件的结果
        conditions_met = []
        
        if fund_met:
            print("✅ 资金条件: 满足")
            conditions_met.append(True)
        else:
            print("❌ 资金条件: 不满足")
            conditions_met.append(False)
            
        if pattern_met:
            print("✅ 形态条件: 满足")
            conditions_met.append(True)
        else:
            print("❌ 形态条件: 不满足")
            conditions_met.append(False)
            
        if indicator_met:
            print("✅ 指标条件: 满足")
            conditions_met.append(True)
        else:
            print("❌ 指标条件: 不满足")
            conditions_met.append(False)
        
        # 判断前3个条件是否都满足
        if all(conditions_met):
            print("🎉 前3个条件全部满足！正在保存到Excel...")
            
            # 准备数据并保存到Excel
            result_data = {
                '股票代码': [self.stock_code],
                '股票名称': [stock_name],
                '检测时间': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                '资金条件': ['满足' if fund_met else '不满足'],
                '形态条件': ['满足' if pattern_met else '不满足'],
                '指标条件': ['满足' if indicator_met else '不满足']
            }
            
            df_result = pd.DataFrame(result_data)
            
            excel_path = r"C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/findup.xlsx"
            
            # 检查文件是否存在，如果存在则追加，否则创建新文件
            try:
                existing_df = pd.read_excel(excel_path)
                updated_df = pd.concat([existing_df, df_result], ignore_index=True)
            except FileNotFoundError:
                updated_df = df_result
            
            # 保存到Excel
            updated_df.to_excel(excel_path, index=False)
            print(f"✅ 股票 {self.stock_code} ({stock_name}) 已保存到 {excel_path}")
            
            return True
        else:
            print("❌ 前3个条件未全部满足，跳过保存")
            return False

# ==================== 使用示例 ====================
if __name__ == "__main__":
    # 获取所有A股股票
    stock_list = stock_anaylse.get_all_a_stocks()
    print(f"获取到 {len(stock_list)} 只股票")
    
    # 提取股票代码和名称列表
    stock_codes = stock_list['code'].tolist()
    stock_names = stock_list['name'].tolist()
    
    # 遍历股票列表，检查前3个条件
    print("\n开始检查股票...")
    print("="*60)
    
    # 用于记录满足条件的股票数量
    satisfied_count = 0
    
    # 遍历每个股票
    for i, (code, name) in enumerate(zip(stock_codes, stock_names)):
        print(f"\n处理股票 {i+1}/{len(stock_codes)}: {code} - {name}")
        
        # 创建EarlyTrendDetector实例
        detector = EarlyTrendDetector(code)
        
        # 检查前3个条件并保存结果
        if detector.save_top3_conditions_stocks(name):
            satisfied_count += 1
    
    # 输出最终结果
    print("\n" + "="*60)
    print(f"检查完成！")
    print(f"满足前3个条件的股票数量: {satisfied_count}")
    print(f"结果已保存到: C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/findup.xlsx")
    print("="*60)
    