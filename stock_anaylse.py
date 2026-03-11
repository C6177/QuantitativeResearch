import akshare as ak
import pandas as pd
import time
from functools import wraps

# 带重试机制的API调用装饰器
def retry_with_delay(max_retries=3, delay=2, timeout=30):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    # 每次调用前等待指定时间
                    if attempt > 0:
                        print(f"  等待 {delay} 秒后重试...")
                        time.sleep(delay)
                    else:
                        time.sleep(delay)
                    
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    print(f"  第 {attempt + 1}/{max_retries} 次调用失败: {e}")
                    if attempt == max_retries - 1:
                        print(f"  已达到最大重试次数，放弃调用")
                        raise
            return None
        return wrapper
    return decorator

# 获取行业信息的函数，带2秒间隔和重试机制
@retry_with_delay(max_retries=3, delay=2, timeout=30)
def get_industry_info(code):
    """获取股票行业信息，带2秒间隔和超时重试"""
    industry_data = ak.stock_individual_info_em(symbol=str(code))
    industry = None
    if isinstance(industry_data, pd.DataFrame) and not industry_data.empty:
        industry_row = industry_data[industry_data['item'] == '行业']
        if not industry_row.empty:
            industry = industry_row.iloc[0]['value']
    return industry

# 步骤1：获取所有A股股票代码和名称
def get_all_a_stocks():
    print("正在获取A股列表...")
    # 使用 stock_info_sh_name_code 接口获取沪股列表
    print("使用 stock_info_sh_name_code 接口...")
    stock_list = ak.stock_info_sh_name_code()
    # 提取证券代码和证券简称
    stock_list = stock_list[['证券代码', '证券简称']].rename(columns={'证券代码': 'code', '证券简称': 'name'})
    print(f"获取到 {len(stock_list)} 只沪股股票")
    return stock_list

# 步骤2：查询个股实时信息并筛选
def filter_by_valuation(stock_list):
    print("\n正在根据估值指标筛选股票...")
    filtered_stocks = []
    
    # 处理所有股票
    total_stocks = len(stock_list)
    print(f"正在处理所有 {total_stocks} 只股票...")
    for index, row in stock_list.iterrows():
        code = row['code']
        name = row['name']
        print(f"处理第 {index+1}/{total_stocks} 只股票: {code} {name}")
        
        try:
            # 获取个股实时信息
            print(f"  获取 {code} {name} 的实时信息...")
            stock_data = ak.stock_value_em(symbol=code)
            
            # 提取估值指标
            print(f"  提取 {code} {name} 的估值指标...")
            if isinstance(stock_data, pd.DataFrame) and not stock_data.empty:
                # 按日期排序，获取最新的一行数据
                if '数据日期' in stock_data.columns:
                    try:
                        stock_data['数据日期'] = pd.to_datetime(stock_data['数据日期'])
                        stock_data = stock_data.sort_values('数据日期', ascending=False)
                        print(f"  数据已按日期排序，最新日期: {stock_data.iloc[0]['数据日期']}")
                    except Exception as e:
                        print(f"  日期排序失败: {e}")
                
                # 取最新的一行数据
                latest_data = stock_data.iloc[0]
                
                # 直接从列中提取指标
                pb = latest_data.get('市净率', None)
                pc = latest_data.get('市现率', None)
                ps = latest_data.get('市销率', None)
                
                print(f"  最新数据: 市净率={pb}, 市现率={pc}, 市销率={ps}")
            else:
                print(f"  数据结构不符合预期，跳过")
                continue
            
            # 转换为数值
            try:
                pb = float(pb) if pb else None
                pc = float(pc) if pc else None
                ps = float(ps) if ps else None
            except (ValueError, TypeError):
                continue
            
            # 筛选条件：市净率 < 5，市现率 < 20，市销率 < 5
            if pb and pc and ps and pb < 5 and pc < 20 and ps < 5:
                filtered_stocks.append({'code': code, 'name': name, 'pb': pb, 'pc': pc, 'ps': ps})
                print(f"{code} {name}: 市净率={pb:.2f}, 市现率={pc:.2f}, 市销率={ps:.2f}")
        except Exception as e:
            print(f"处理股票 {code} {name} 时出错: {e}")
            continue
    
    print(f"\n估值筛选完成，符合条件的股票有 {len(filtered_stocks)} 只")
    
    # 创建筛选结果DataFrame
    result_df = pd.DataFrame(filtered_stocks)
    
    # 保存为Excel文件
    if not result_df.empty:
        output_path = "/Users/danawang/coding/QuantitativeResearch/fliter-1.xlsx"
        result_df[['code', 'name']].to_excel(output_path, index=False)
        print(f"已将筛选结果保存到: {output_path}")
    
    return result_df

# 步骤3：查询基本面信息并筛选
def filter_by_fundamentals():
    print("\n正在根据基本面指标筛选股票...")
    final_stocks = []
    
    # 从fliter-1.xlsx读取筛选后的股票
    input_file = "/Users/danawang/coding/QuantitativeResearch/fliter-1.xlsx"
    try:
        filtered_stocks = pd.read_excel(input_file)
        print(f"已从 {input_file} 读取 {len(filtered_stocks)} 只股票")
    except Exception as e:
        print(f"读取文件失败: {e}")
        return pd.DataFrame()
    
    for index, row in filtered_stocks.iterrows():
        code = row['code']
        name = row['name']
        
        try:
            # 获取基本面数据
            print(f"  获取 {code} {name} 的基本面数据...")
            # 拼接symbol参数，添加.SH后缀
            symbol = f"{code}.SH"
            fundamental_data = ak.stock_financial_analysis_indicator_em(symbol=symbol, indicator="按报告期")
            
            if isinstance(fundamental_data, pd.DataFrame) and not fundamental_data.empty:
                # 按报告期排序，获取最新的一行数据
                if 'REPORT_DATE' in fundamental_data.columns:
                    try:
                        fundamental_data['REPORT_DATE'] = pd.to_datetime(fundamental_data['REPORT_DATE'])
                        fundamental_data = fundamental_data.sort_values('REPORT_DATE', ascending=False)
                        print(f"  数据已按报告期排序，最新报告期: {fundamental_data.iloc[0]['REPORT_DATE']}")
                    except Exception as e:
                        print(f"  报告期排序失败: {e}")
                
                # 取最新的一行数据
                latest_data = fundamental_data.iloc[0]
                
                # 提取基本面指标
                eps = latest_data.get('EPSJB', None)  # 基本每股收益(元)
                eps_nq = latest_data.get('EPSKCJB', None)  # 扣非每股收益(元)
                net_profit_rate = latest_data.get('XSJLL', None)  # 净利率(%)
                roa = latest_data.get('ZZCJLL', None)  # 总资产收益率(加权)(%)
                revenue_growth = latest_data.get('TOTALOPERATEREVETZ', None)  # 营业总收入同比增长(%)
                net_profit_growth = latest_data.get('PARENTNETPROFITTZ', None)  # 归属净利润同比增长(%)
                net_profit_nq_growth = latest_data.get('KCFJCXSYJLRTZ', None)  # 扣非净利润同比增长(%)
                operating_cash_flow_per_share = latest_data.get('MGJYXJJE', None)  # 每股经营现金流(元)
                operating_cash_flow_revenue_ratio = latest_data.get('JYXJLYYSR', None)  # 经营净现金流/营业收入
                sales_cash_flow_revenue_ratio = latest_data.get('XSJXLYYSR', None)  # 销售净现金流/营业收入
                asset_liability_ratio = latest_data.get('ZCFZL', None)  # 资产负债率(%)
                current_ratio = latest_data.get('LD', None)  # 流动比率
                quick_ratio = latest_data.get('SD', None)  # 速动比率
                cash_flow_ratio = latest_data.get('XJLLB', None)  # 现金流量比率
                
                print(f"  最新数据: 基本每股收益={eps}, 扣非每股收益={eps_nq}, 净利率={net_profit_rate}%, 总资产收益率={roa}%, 营收增长={revenue_growth}%, 净利润增长={net_profit_growth}%, 扣非净利润增长={net_profit_nq_growth}%, 每股经营现金流={operating_cash_flow_per_share}, 经营净现金流/营业收入={operating_cash_flow_revenue_ratio}, 销售净现金流/营业收入={sales_cash_flow_revenue_ratio}, 资产负债率={asset_liability_ratio}%, 流动比率={current_ratio}, 速动比率={quick_ratio}, 现金流量比率={cash_flow_ratio}")
            else:
                print(f"  数据结构不符合预期，跳过")
                continue
            
            # 转换为数值
            try:
                eps = float(eps) if eps else None
                eps_nq = float(eps_nq) if eps_nq else None
                net_profit_rate = float(net_profit_rate) if net_profit_rate else None
                roa = float(roa) if roa else None
                revenue_growth = float(revenue_growth) if revenue_growth else None
                net_profit_growth = float(net_profit_growth) if net_profit_growth else None
                net_profit_nq_growth = float(net_profit_nq_growth) if net_profit_nq_growth else None
                operating_cash_flow_per_share = float(operating_cash_flow_per_share) if operating_cash_flow_per_share else None
                operating_cash_flow_revenue_ratio = float(operating_cash_flow_revenue_ratio) if operating_cash_flow_revenue_ratio else None
                sales_cash_flow_revenue_ratio = float(sales_cash_flow_revenue_ratio) if sales_cash_flow_revenue_ratio else None
                asset_liability_ratio = float(asset_liability_ratio) if asset_liability_ratio else None
                current_ratio = float(current_ratio) if current_ratio else None
                quick_ratio = float(quick_ratio) if quick_ratio else None
                cash_flow_ratio = float(cash_flow_ratio) if cash_flow_ratio else None
            except (ValueError, TypeError):
                print(f"  数据转换失败，跳过")
                continue
            
            # 筛选条件
            condition1 = eps > 0 if eps else False
            condition2 = (eps_nq / eps >= 0.8) if eps and eps_nq else False
            condition3 = net_profit_rate > 5 if net_profit_rate else False
            condition4 = roa > 5 if roa else False
            condition5 = revenue_growth > 10 if revenue_growth else False
            condition6 = net_profit_growth > 10 if net_profit_growth else False
            condition7 = net_profit_nq_growth > 10 if net_profit_nq_growth else False
            condition8 = operating_cash_flow_per_share > 0 if operating_cash_flow_per_share else False
            condition9 = operating_cash_flow_revenue_ratio >= 0.1 if operating_cash_flow_revenue_ratio else False
            condition10 = sales_cash_flow_revenue_ratio >= 0.9 if sales_cash_flow_revenue_ratio else False
            condition11 = asset_liability_ratio < 60 if asset_liability_ratio else False
            condition12 = current_ratio > 1.5 if current_ratio else False
            condition13 = quick_ratio > 1.0 if quick_ratio else False
            condition14 = cash_flow_ratio > 0.2 if cash_flow_ratio else False
            
            print(f"  条件检查结果: {condition1}, {condition2}, {condition3}, {condition4}, {condition5}, {condition6}, {condition7}, {condition8}, {condition9}, {condition10}, {condition11}, {condition12}, {condition13}, {condition14}")
            
            # 检查满足条件的数量
            conditions = [condition1, condition2, condition3, condition4, condition5, condition6, condition7, 
                        condition8, condition9, condition10, condition11, condition12, condition13, condition14]
            satisfied_conditions = sum(conditions)
            print(f"  满足条件数量: {satisfied_conditions}/14")
            
            if satisfied_conditions >= 10:
                final_stocks.append({'code': code, 'name': name, 'satisfied_conditions': satisfied_conditions})
                print(f"{code} {name} 符合条件（满足 {satisfied_conditions}/14 项）")
        except Exception as e:
            print(f"处理股票 {code} {name} 时出错: {e}")
            continue
    
    print(f"\n基本面筛选完成，符合条件的股票有 {len(final_stocks)} 只")
    
    # 创建结果DataFrame
    result_df = pd.DataFrame(final_stocks)
    
    # 保存为Excel文件
    if not result_df.empty:
        output_file = "/Users/danawang/coding/QuantitativeResearch/fliter-2.xlsx"
        result_df.to_excel(output_file, index=False)
        print(f"已将筛选结果保存到: {output_file}")
    
    return result_df
    
# 步骤4：根据估值比较排名筛选
def filter_by_valuation_ranking():
    print("\n正在根据估值比较排名筛选股票...")
    final_stocks = []
    
    # 从fliter-2.xlsx读取筛选后的股票
    input_file = "/Users/danawang/coding/QuantitativeResearch/fliter-2.xlsx"
    try:
        filtered_stocks = pd.read_excel(input_file)
        print(f"已从 {input_file} 读取 {len(filtered_stocks)} 只股票")
    except Exception as e:
        print(f"读取文件失败: {e}")
        return pd.DataFrame()
    
    for index, row in filtered_stocks.iterrows():
        code = row['code']
        name = row['name']
        
        try:
            # 获取估值比较数据
            print(f"  获取 {code} {name} 的估值比较数据...")
            # 拼接symbol参数，格式为SH+code
            symbol = f"SH{code}"
            valuation_data = ak.stock_zh_valuation_comparison_em(symbol=symbol)
            
            if isinstance(valuation_data, pd.DataFrame) and not valuation_data.empty:
                # 取第一行数据
                latest_data = valuation_data.iloc[0]
                
                # 提取排名数据
                ranking = latest_data.get('排名', None)
                print(f"  排名数据: {ranking}")
                
                # 解析排名
                if ranking:
                    # 处理类似"42.0/120"的格式
                    if isinstance(ranking, str) and '/' in ranking:
                        rank_part = ranking.split('/')[0]
                        try:
                            rank = float(rank_part)
                            print(f"  解析排名: {rank}")
                            
                            # 保留排名在前8名的股票
                            if rank <= 8:
                                # 获取行业信息
                                print(f"  获取 {code} {name} 的行业信息...")
                                try:
                                    # 使用带重试机制的函数获取行业信息
                                    industry = get_industry_info(code)
                                    if industry:
                                        print(f"  行业信息: {industry}")
                                    else:
                                        print(f"  未获取到行业信息")
                                except Exception as e:
                                    print(f"  获取行业信息失败: {e}")
                                    industry = None
                                
                                final_stocks.append({'code': code, 'name': name, 'ranking': ranking, 'industry': industry})
                                print(f"{code} {name} 排名 {ranking}，行业 {industry}，符合条件")
                        except (ValueError, TypeError):
                            print(f"  排名解析失败: {rank_part}")
                    elif isinstance(ranking, (int, float)):
                        print(f"  解析排名: {ranking}")
                        if ranking <= 8:
                            # 获取行业信息
                            print(f"  获取 {code} {name} 的行业信息...")
                            try:
                                # 使用带重试机制的函数获取行业信息
                                industry = get_industry_info(code)
                                if industry:
                                    print(f"  行业信息: {industry}")
                                else:
                                    print(f"  未获取到行业信息")
                            except Exception as e:
                                print(f"  获取行业信息失败: {e}")
                                industry = None
                            
                            final_stocks.append({'code': code, 'name': name, 'ranking': ranking, 'industry': industry})
                            print(f"{code} {name} 排名 {ranking}，行业 {industry}，符合条件")
            else:
                print(f"  数据结构不符合预期，跳过")
                continue
        except Exception as e:
            print(f"处理股票 {code} {name} 时出错: {e}")
            continue
    
    print(f"\n估值比较排名筛选完成，符合条件的股票有 {len(final_stocks)} 只")
    
    # 创建结果DataFrame
    result_df = pd.DataFrame(final_stocks)
    
    # 保存为Excel文件
    if not result_df.empty:
        output_file = "/Users/danawang/coding/QuantitativeResearch/fliter-3.xlsx"
        result_df[['code', 'name', 'ranking', 'industry']].to_excel(output_file, index=False)
        print(f"已将筛选结果保存到: {output_file}")
    
    return result_df

# 主函数
def main():
    print("开始股票筛选...")
    
    # 步骤1：获取所有A股股票
    stock_list = get_all_a_stocks()
    
    # 步骤2：估值筛选
    valuation_filtered = filter_by_valuation(stock_list)
    
    # 步骤3：基本面筛选
    final_result = filter_by_fundamentals()
    
    # 步骤4：根据估值比较排名筛选
    final_result = filter_by_valuation_ranking()
    
    # 输出最终结果
    print("\n最终筛选结果:")
    if not final_result.empty:
        print(final_result)
    else:
        print("没有符合所有条件的股票")


if __name__ == "__main__":
    main()