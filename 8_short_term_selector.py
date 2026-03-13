"""
五步量化选股模型 - 完整优化版
包含：指数退避重试、请求间隔控制、分批处理、板块代码优先、浏览器请求头
"""

import akshare as ak
import pandas as pd
import numpy as np
import time
import random
import os
import warnings
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

warnings.filterwarnings('ignore')

class QuantStockSelector:
    """五步量化选股器 - 优化版"""
    
    def __init__(self):
        self.today = datetime.now().strftime("%Y-%m-%d")
        # 设置回测日期范围（最近60天）
        self.end_date = self.today
        self.start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
        
        # 输出目录
        self.output_dir = r"C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/"
        
        # 确保输出目录存在
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # 创建带浏览器请求头的session
        self.session = self._create_session()
        
        # 请求计数器
        self.request_count = 0
        self.last_request_time = None
        
        print("="*80)
        print("五步量化选股模型启动（优化版）")
        print(f"分析周期: {self.start_date} 至 {self.end_date}")
        print("="*80)
        print("⚠️ 重要提示：")
        print("   1. 请确保已登录东方财富网 https://www.eastmoney.com/")
        print("   2. 程序将自动控制请求频率避免被封")
        print("   3. 整个流程可能需要30-60分钟，请耐心等待")
        print("="*80)
    
    def _create_session(self):
        """创建带浏览器请求头的session"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        return session
    
    def _wait_before_request(self):
        """智能等待，控制请求频率"""
        current_time = time.time()
        
        # 基础等待（每次请求至少间隔3-8秒）
        base_wait = random.uniform(3, 8)
        
        # 如果是当天第一次请求，等待时间可以短一些
        if self.last_request_time is None:
            wait_time = base_wait
        else:
            elapsed = current_time - self.last_request_time
            if elapsed < base_wait:
                wait_time = base_wait - elapsed + random.uniform(1, 3)
            else:
                wait_time = random.uniform(1, 3)
        
        # 每10次请求后，增加额外等待
        self.request_count += 1
        if self.request_count % 10 == 0:
            extra_wait = random.uniform(10, 20)
            print(f"  已完成{self.request_count}次请求，额外等待{extra_wait:.1f}秒...")
            wait_time += extra_wait
        
        time.sleep(wait_time)
        self.last_request_time = time.time()
    
    def step1_get_all_sectors(self):
        """
        步骤1：获取所有板块列表
        调用接口：stock_board_industry_name_em
        返回：板块名称，板块代码
        """
        print("\n【步骤1】获取所有板块列表")
        
        try:
            # 首次请求前等待
            self._wait_before_request()
            
            sector_df = ak.stock_board_industry_name_em()
            
            # 只保留需要的字段
            result = sector_df[['名称', '代码']].copy()
            result.columns = ['板块名称', '板块代码']
            
            print(f"✅ 成功获取 {len(result)} 个板块")
            return result
            
        except Exception as e:
            print(f"❌ 获取板块列表失败: {e}")
            return pd.DataFrame()
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=5, max=40),
        retry=retry_if_exception_type(Exception),
        before_sleep=lambda retry_state: print(f"  等待{retry_state.next_action.sleep}s后重试...")
    )
    def step2_get_sector_history(self, sector_code, sector_name):
        """
        步骤2：获取单个板块的历史K线数据
        使用板块代码优先，包含重试机制
        """
        try:
            # 请求前智能等待
            self._wait_before_request()
            
            # 优先使用板块代码
            symbol = sector_code if sector_code and len(sector_code) > 0 else sector_name
            
            print(f"  正在获取 {sector_name} 的历史数据...")
            
            hist_df = ak.stock_board_industry_hist_em(
                symbol=symbol,
                period="日k",
                start_date=self.start_date,
                end_date=self.end_date,
                adjust="hfq"
            )
            
            if hist_df.empty:
                print(f"  ⚠️ {sector_name} 返回空数据")
                return pd.DataFrame()
            
            # 重命名列
            hist_df = hist_df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'change',
                '成交量': 'volume',
                '成交额': 'amount'
            })
            
            print(f"  ✅ {sector_name} 获取成功 ({len(hist_df)}条数据)")
            return hist_df
            
        except Exception as e:
            print(f"  ❌ {sector_name} 失败: {str(e)[:50]}")
            # 失败后等待更长时间再重试
            time.sleep(random.uniform(10, 20))
            raise
    
    def step3_calc_sector_tech_score(self):
        """
        步骤3：计算每个板块的技术指标得分
        条件：板块MA5 > MA10，MA5 > MA20，收盘价 > MA20
        输出：short_term_selecor-1.xlsx
        """
        print("\n【步骤3】计算板块技术指标得分")
        
        # 获取所有板块
        sectors_df = self.step1_get_all_sectors()
        if sectors_df.empty:
            print("❌ 无法获取板块列表，流程终止")
            return pd.DataFrame()
        
        qualified_sectors = []
        
        # 分批处理，每批3个板块
        batch_size = 3
        total_batches = (len(sectors_df) + batch_size - 1) // batch_size
        
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(sectors_df))
            batch = sectors_df.iloc[start_idx:end_idx]
            
            print(f"\n处理第{batch_idx+1}/{total_batches}批，共{len(batch)}个板块")
            
            for _, row in batch.iterrows():
                sector_name = row['板块名称']
                sector_code = row['板块代码']
                
                try:
                    # 获取板块历史数据
                    hist_df = self.step2_get_sector_history(sector_code, sector_name)
                    
                    if hist_df.empty or len(hist_df) < 20:
                        print(f"  ⚠️ {sector_name} 数据不足，跳过")
                        continue
                    
                    # 计算MA5, MA10, MA20
                    hist_df['MA5'] = hist_df['close'].rolling(5).mean()
                    hist_df['MA10'] = hist_df['close'].rolling(10).mean()
                    hist_df['MA20'] = hist_df['close'].rolling(20).mean()
                    
                    # 获取最新一天的数据
                    latest = hist_df.iloc[-1]
                    
                    # 判断条件
                    ma5_gt_ma10 = latest['MA5'] > latest['MA10']
                    ma5_gt_ma20 = latest['MA5'] > latest['MA20']
                    close_gt_ma20 = latest['close'] > latest['MA20']
                    
                    if ma5_gt_ma10 and ma5_gt_ma20 and close_gt_ma20:
                        qualified_sectors.append({
                            '板块名称': sector_name,
                            '板块代码': sector_code,
                            '最新收盘': round(latest['close'], 2),
                            'MA5': round(latest['MA5'], 2),
                            'MA10': round(latest['MA10'], 2),
                            'MA20': round(latest['MA20'], 2),
                            '得分': 1
                        })
                        print(f"  ✅ {sector_name}: 满足技术条件")
                    else:
                        print(f"  ❌ {sector_name}: 不满足技术条件")
                        
                except Exception as e:
                    print(f"  ❌ 处理{sector_name}时发生异常: {str(e)[:50]}")
                    continue
            
            # 每批处理完后，如果不是最后一批，强制休息较长时间
            if batch_idx < total_batches - 1:
                rest_time = random.uniform(60, 90)
                print(f"\n⏸️ 第{batch_idx+1}批完成，休息{rest_time:.0f}秒避免被封...")
                time.sleep(rest_time)
        
        # 保存结果
        result_df = pd.DataFrame(qualified_sectors)
        output_path = os.path.join(self.output_dir, "short_term_selecor-1.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"\n✅ 步骤3完成：{len(qualified_sectors)} 个板块满足技术条件")
        print(f"   结果保存至: {output_path}")
        
        return result_df
    
    def step4_get_market_data(self):
        """
        步骤4：获取上证指数历史数据
        调用接口：stock_zh_a_hist
        """
        print("\n【步骤4】获取上证指数历史数据")
        
        try:
            self._wait_before_request()
            
            market_df = ak.stock_zh_a_hist(
                symbol="000001",
                period="daily",
                start_date=self.start_date,
                end_date=self.end_date,
                adjust="hfq"
            )
            
            if market_df.empty:
                print("❌ 获取上证指数数据失败")
                return pd.DataFrame()
            
            # 重命名列
            market_df = market_df.rename(columns={
                '日期': 'date',
                '收盘': 'close',
                '涨跌幅': 'pct_chg'
            })
            
            print(f"✅ 成功获取上证指数数据，共 {len(market_df)} 天")
            return market_df
            
        except Exception as e:
            print(f"❌ 获取上证指数数据失败: {e}")
            return pd.DataFrame()
    
    def step5_calc_sector_rs_score(self, sectors_df):
        """
        步骤5：计算板块相对强度得分
        条件：最近10天内，连续3天以上板块涨幅 - 上证涨幅 > 0
        输出：short_term_selecor-2.xlsx
        """
        print("\n【步骤5】计算板块相对强度得分")
        
        if sectors_df.empty:
            print("  ❌ 无输入板块数据，跳过")
            return pd.DataFrame()
        
        # 获取上证指数数据
        market_df = self.step4_get_market_data()
        if market_df.empty:
            print("❌ 无法获取上证指数数据，流程终止")
            return pd.DataFrame()
        
        # 将上证指数数据转换为日期索引
        market_df['date'] = pd.to_datetime(market_df['date'])
        market_dict = dict(zip(market_df['date'].dt.strftime('%Y-%m-%d'), market_df['pct_chg']))
        
        qualified_sectors = []
        
        # 分批处理
        batch_size = 5
        for i in range(0, len(sectors_df), batch_size):
            batch = sectors_df.iloc[i:i+batch_size]
            
            for _, row in batch.iterrows():
                sector_name = row['板块名称']
                sector_code = row['板块代码']
                
                try:
                    # 获取板块历史数据
                    hist_df = self.step2_get_sector_history(sector_code, sector_name)
                    
                    if hist_df.empty or len(hist_df) < 10:
                        continue
                    
                    hist_df['date'] = pd.to_datetime(hist_df['date'])
                    
                    # 计算最近10天每天的相对强度
                    rs_records = []
                    for j in range(min(10, len(hist_df))):
                        idx = -1 - j
                        date_str = hist_df.iloc[idx]['date'].strftime('%Y-%m-%d')
                        sector_ret = hist_df.iloc[idx]['pct_chg']
                        market_ret = market_dict.get(date_str, 0)
                        
                        rs = sector_ret - market_ret
                        rs_records.append(rs > 0)
                    
                    # 判断是否有连续3天以上满足条件
                    max_consecutive = 0
                    current_consecutive = 0
                    
                    for val in rs_records:
                        if val:
                            current_consecutive += 1
                            max_consecutive = max(max_consecutive, current_consecutive)
                        else:
                            current_consecutive = 0
                    
                    if max_consecutive >= 3:
                        row_dict = row.to_dict()
                        row_dict['最大连续跑赢天数'] = max_consecutive
                        row_dict['得分'] = 1
                        qualified_sectors.append(row_dict)
                        print(f"  ✅ {sector_name}: 连续{max_consecutive}天跑赢大盘")
                    
                except Exception as e:
                    print(f"  ❌ 处理{sector_name}相对强度失败")
                    continue
            
            # 每批之间休息
            if i + batch_size < len(sectors_df):
                time.sleep(random.uniform(30, 45))
        
        # 保存结果
        result_df = pd.DataFrame(qualified_sectors)
        output_path = os.path.join(self.output_dir, "short_term_selecor-2.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"\n✅ 步骤5完成：{len(qualified_sectors)} 个板块满足相对强度条件")
        print(f"   结果保存至: {output_path}")
        
        return result_df
    
    def step6_calc_sector_volume_score(self, sectors_df):
        """
        步骤6：计算板块成交量得分
        条件：板块成交量 > 板块20日均量 * 1.2
        输出：short_term_selecor-3.xlsx
        """
        print("\n【步骤6】计算板块成交量得分")
        
        if sectors_df.empty:
            print("  ❌ 无输入板块数据，跳过")
            return pd.DataFrame()
        
        qualified_sectors = []
        
        for _, row in sectors_df.iterrows():
            sector_name = row['板块名称']
            sector_code = row['板块代码']
            
            try:
                # 获取板块历史数据
                hist_df = self.step2_get_sector_history(sector_code, sector_name)
                
                if hist_df.empty or len(hist_df) < 20:
                    continue
                
                # 计算20日均量
                hist_df['volume_ma20'] = hist_df['volume'].rolling(20).mean()
                
                # 获取最新数据
                latest = hist_df.iloc[-1]
                
                # 判断成交量条件
                volume_gt_ma20 = latest['volume'] > latest['volume_ma20'] * 1.2
                
                if volume_gt_ma20:
                    row_dict = row.to_dict()
                    row_dict['最新成交量'] = int(latest['volume'])
                    row_dict['20日均量'] = int(latest['volume_ma20'])
                    row_dict['成交量比率'] = round(latest['volume'] / latest['volume_ma20'], 2)
                    row_dict['得分'] = 1
                    qualified_sectors.append(row_dict)
                    print(f"  ✅ {sector_name}: 成交量是20日均量的{row_dict['成交量比率']}倍")
                
            except Exception as e:
                print(f"  ❌ 处理{sector_name}成交量失败")
                continue
        
        # 保存结果
        result_df = pd.DataFrame(qualified_sectors)
        output_path = os.path.join(self.output_dir, "short_term_selecor-3.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"\n✅ 步骤6完成：{len(qualified_sectors)} 个板块满足成交量条件")
        print(f"   结果保存至: {output_path}")
        
        return result_df
    
    def step7_get_sector_stocks(self, sector_name):
        """
        步骤7：获取板块内所有个股
        调用接口：stock_board_industry_cons_em
        """
        try:
            self._wait_before_request()
            
            stocks_df = ak.stock_board_industry_cons_em(symbol=sector_name)
            
            if stocks_df.empty:
                return pd.DataFrame()
            
            # 返回需要的字段
            result = stocks_df[['代码', '名称']].copy()
            return result
            
        except Exception as e:
            print(f"  获取板块 {sector_name} 成分股失败: {e}")
            return pd.DataFrame()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=3, max=20)
    )
    def step8_get_stock_history(self, stock_code):
        """
        步骤8：获取个股历史K线数据
        """
        try:
            self._wait_before_request()
            
            # 补齐6位代码
            if len(stock_code) < 6:
                stock_code = stock_code.zfill(6)
            
            hist_df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=self.start_date,
                end_date=self.end_date,
                adjust="hfq"
            )
            
            if hist_df.empty:
                return pd.DataFrame()
            
            # 重命名列
            hist_df = hist_df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '涨跌幅': 'pct_chg',
                '涨跌额': 'change',
                '成交量': 'volume',
                '成交额': 'amount'
            })
            
            return hist_df
            
        except Exception as e:
            time.sleep(random.uniform(5, 10))
            raise
    
    def step9_calc_stock_tech_score(self, sectors_df):
        """
        步骤9：计算个股技术指标得分
        条件：个股MA5 > MA10，MA5 > MA20，收盘价 > MA20
        输出：short_term_selecor-4.xlsx
        """
        print("\n【步骤9】计算个股技术指标得分")
        
        if sectors_df.empty:
            print("  ❌ 无输入板块数据，跳过")
            return pd.DataFrame()
        
        qualified_stocks = []
        
        for _, sector_row in sectors_df.iterrows():
            sector_name = sector_row['板块名称']
            print(f"\n分析板块: {sector_name}")
            
            # 获取板块内所有个股
            stocks_df = self.step7_get_sector_stocks(sector_name)
            if stocks_df.empty:
                print(f"  ⚠️ {sector_name} 无成分股数据")
                continue
            
            print(f"  共 {len(stocks_df)} 只个股")
            
            # 分批处理个股，每批5只
            batch_size = 5
            for i in range(0, len(stocks_df), batch_size):
                stock_batch = stocks_df.iloc[i:i+batch_size]
                
                for _, stock_row in stock_batch.iterrows():
                    stock_code = stock_row['代码']
                    stock_name = stock_row['名称']
                    
                    try:
                        # 获取个股历史数据
                        hist_df = self.step8_get_stock_history(stock_code)
                        
                        if hist_df.empty or len(hist_df) < 20:
                            continue
                        
                        # 计算MA5, MA10, MA20
                        hist_df['MA5'] = hist_df['close'].rolling(5).mean()
                        hist_df['MA10'] = hist_df['close'].rolling(10).mean()
                        hist_df['MA20'] = hist_df['close'].rolling(20).mean()
                        
                        # 获取最新一天的数据
                        latest = hist_df.iloc[-1]
                        
                        # 判断条件
                        ma5_gt_ma10 = latest['MA5'] > latest['MA10']
                        ma5_gt_ma20 = latest['MA5'] > latest['MA20']
                        close_gt_ma20 = latest['close'] > latest['MA20']
                        
                        if ma5_gt_ma10 and ma5_gt_ma20 and close_gt_ma20:
                            qualified_stocks.append({
                                '股票代码': stock_code,
                                '股票名称': stock_name,
                                '所属板块': sector_name,
                                '最新收盘': round(latest['close'], 2),
                                'MA5': round(latest['MA5'], 2),
                                'MA10': round(latest['MA10'], 2),
                                'MA20': round(latest['MA20'], 2),
                                '得分': 1
                            })
                            print(f"    ✅ {stock_name}({stock_code})")
                        
                    except Exception as e:
                        # print(f"    处理{stock_name}失败")
                        continue
                
                # 每批个股后休息
                if i + batch_size < len(stocks_df):
                    time.sleep(random.uniform(5, 10))
            
            # 每个板块后休息较长时间
            time.sleep(random.uniform(20, 30))
        
        # 保存结果
        result_df = pd.DataFrame(qualified_stocks)
        output_path = os.path.join(self.output_dir, "short_term_selecor-4.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"\n✅ 步骤9完成：{len(qualified_stocks)} 只个股满足技术条件")
        print(f"   结果保存至: {output_path}")
        
        return result_df
    
    def step10_calc_eps_growth_score(self, stocks_df):
        """
        步骤10：计算每股收益增长率得分
        条件：基本每股收益增长率-3年复合 > 10%，且排名前20
        输出：short_term_selecor-5.xlsx
        """
        print("\n【步骤10】计算每股收益增长率得分")
        
        if stocks_df.empty:
            print("  ❌ 无输入个股数据，跳过")
            return pd.DataFrame()
        
        qualified_stocks = []
        
        for i, row in stocks_df.iterrows():
            stock_code = row['股票代码']
            stock_name = row['股票名称']
            
            try:
                self._wait_before_request()
                
                # 获取成长能力数据
                growth_df = ak.stock_zh_growth_comparison_em(symbol=stock_code)
                
                if growth_df.empty:
                    continue
                
                # 查找"基本每股收益增长率-三年复合"指标
                eps_growth_row = growth_df[growth_df['财务指标'] == '基本每股收益增长率-三年复合']
                
                if not eps_growth_row.empty:
                    eps_growth = eps_growth_row.iloc[0]['数值']
                    eps_rank = eps_growth_row.iloc[0]['排名']
                    
                    # 判断条件
                    if eps_growth > 10 and eps_rank <= 20:
                        row_dict = row.to_dict()
                        row_dict['EPS三年复合增长'] = round(eps_growth, 2)
                        row_dict['EPS排名'] = int(eps_rank)
                        row_dict['得分'] = 1
                        qualified_stocks.append(row_dict)
                        print(f"  ✅ {stock_name}: EPS增长{eps_growth}%，排名{eps_rank}")
                
            except Exception as e:
                # print(f"  处理{stock_name}成长数据失败")
                continue
            
            # 每10只个股休息一下
            if (i + 1) % 10 == 0:
                time.sleep(random.uniform(10, 15))
        
        # 保存结果
        result_df = pd.DataFrame(qualified_stocks)
        output_path = os.path.join(self.output_dir, "short_term_selecor-5.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"\n✅ 步骤10完成：{len(qualified_stocks)} 只个股满足EPS增长条件")
        print(f"   结果保存至: {output_path}")
        
        return result_df
    
    def step11_calc_revenue_rank_score(self, stocks_df):
        """
        步骤11：计算营业收入排名得分
        条件：营业收入排名 <= 10
        输出：short_term_selecor-6.xlsx
        """
        print("\n【步骤11】计算营业收入排名得分")
        
        if stocks_df.empty:
            print("  ❌ 无输入个股数据，跳过")
            return pd.DataFrame()
        
        qualified_stocks = []
        
        for i, row in stocks_df.iterrows():
            stock_code = row['股票代码']
            stock_name = row['股票名称']
            
            try:
                self._wait_before_request()
                
                # 获取规模数据
                scale_df = ak.stock_zh_scale_comparison_em(symbol=stock_code)
                
                if scale_df.empty:
                    continue
                
                # 查找"营业收入"指标
                revenue_row = scale_df[scale_df['财务指标'] == '营业收入']
                
                if not revenue_row.empty:
                    revenue_rank = revenue_row.iloc[0]['排名']
                    
                    # 判断条件（营业收入排名 <= 10，1是第1名）
                    if revenue_rank <= 10:
                        row_dict = row.to_dict()
                        row_dict['营业收入排名'] = int(revenue_rank)
                        row_dict['得分'] = 1
                        qualified_stocks.append(row_dict)
                        print(f"  ✅ {stock_name}: 营业收入排名第{revenue_rank}")
                
            except Exception as e:
                # print(f"  处理{stock_name}规模数据失败")
                continue
            
            # 每10只个股休息一下
            if (i + 1) % 10 == 0:
                time.sleep(random.uniform(10, 15))
        
        # 保存结果
        result_df = pd.DataFrame(qualified_stocks)
        output_path = os.path.join(self.output_dir, "short_term_selecor-6.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"\n✅ 步骤11完成：{len(qualified_stocks)} 只个股满足营业收入排名条件")
        print(f"   结果保存至: {output_path}")
        
        return result_df
    
    def run_full_pipeline(self):
        """
        运行完整五步量化选股流程
        """
        print("\n" + "="*80)
        print("开始执行五步量化选股全流程")
        print("请耐心等待，整个过程可能需要1-2小时")
        print("="*80)
        
        start_time = time.time()
        
        # 步骤1-3：板块技术筛选
        print("\n" + "★"*40)
        print("第一阶段：板块技术筛选")
        print("★"*40)
        sector_tech_df = self.step3_calc_sector_tech_score()
        if sector_tech_df.empty:
            print("❌ 步骤1-3无满足条件的板块，流程终止")
            return
        
        # 步骤4-5：板块相对强度筛选
        print("\n" + "★"*40)
        print("第二阶段：板块相对强度筛选")
        print("★"*40)
        sector_rs_df = self.step5_calc_sector_rs_score(sector_tech_df)
        if sector_rs_df.empty:
            print("❌ 步骤4-5无满足条件的板块，流程终止")
            return
        
        # 步骤6：板块成交量筛选
        print("\n" + "★"*40)
        print("第三阶段：板块成交量筛选")
        print("★"*40)
        sector_volume_df = self.step6_calc_sector_volume_score(sector_rs_df)
        if sector_volume_df.empty:
            print("❌ 步骤6无满足条件的板块，流程终止")
            return
        
        # 步骤7-9：个股技术筛选
        print("\n" + "★"*40)
        print("第四阶段：个股技术筛选")
        print("★"*40)
        stock_tech_df = self.step9_calc_stock_tech_score(sector_volume_df)
        if stock_tech_df.empty:
            print("❌ 步骤7-9无满足条件的个股，流程终止")
            return
        
        # 步骤10：EPS增长筛选
        print("\n" + "★"*40)
        print("第五阶段：EPS增长筛选")
        print("★"*40)
        stock_eps_df = self.step10_calc_eps_growth_score(stock_tech_df)
        if stock_eps_df.empty:
            print("❌ 步骤10无满足条件的个股，流程终止")
            return
        
        # 步骤11：营业收入排名筛选
        print("\n" + "★"*40)
        print("第六阶段：营业收入排名筛选")
        print("★"*40)
        final_stocks_df = self.step11_calc_revenue_rank_score(stock_eps_df)
        
        elapsed_time = (time.time() - start_time) / 60
        
        print("\n" + "="*80)
        print("🎯 【五步量化选股最终结果】")
        print("="*80)
        
        if not final_stocks_df.empty:
            print(f"\n✅ 最终选出 {len(final_stocks_df)} 只符合条件的股票：")
            for i, row in final_stocks_df.iterrows():
                print(f"\n{i+1}. {row['股票名称']}({row['股票代码']})")
                print(f"   所属板块: {row['所属板块']}")
                if 'EPS三年复合增长' in row:
                    print(f"   EPS增长: {row['EPS三年复合增长']}%，排名{row['EPS排名']}")
                if '营业收入排名' in row:
                    print(f"   营收排名: 第{row['营业收入排名']}名")
        else:
            print("\n❌ 未筛选出符合条件的股票")
        
        print(f"\n⏱️ 总耗时: {elapsed_time:.1f} 分钟")
        print(f"📁 所有结果已保存至: {self.output_dir}")
        
        return final_stocks_df


# ==================== 主程序入口 ====================
if __name__ == "__main__":
    selector = QuantStockSelector()
    
    try:
        final_results = selector.run_full_pipeline()
    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断程序执行")
    except Exception as e:
        print(f"\n\n❌ 程序运行出错: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*80)
    print("⚠️ 免责声明：本系统仅为量化分析工具")
    print("   不构成投资建议，据此操作风险自负")
    print("   请结合基本面分析和自身风险承受能力决策")
    print("="*80)
    
    input("\n按回车键退出...")