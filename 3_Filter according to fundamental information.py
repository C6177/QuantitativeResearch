import akshare as ak
import pandas as pd
import numpy as np

# 步骤1：读取evaluate.xlsx文件，获取股票code
def read_evaluate_file():
    print("正在读取evaluate.xlsx文件...")
    input_file = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/evaluate.xlsx"
    try:
        df = pd.read_excel(input_file)
        print(f"已从 {input_file} 读取 {len(df)} 条记录")
        # 确保code列存在
        if 'code' in df.columns:
            return df
        else:
            print("文件中不存在code列")
            return pd.DataFrame()
    except Exception as e:
        print(f"读取文件失败: {e}")
        return pd.DataFrame()

# 步骤2：根据估值比较排名筛选

def filter_by_valuation_ranking():
    print("\n正在根据估值比较排名筛选股票...")
    final_stocks = []
    
    # 读取evaluate.xlsx文件
    evaluate_df = read_evaluate_file()
    if evaluate_df.empty:
        return pd.DataFrame()
    
    for index, row in evaluate_df.iterrows():
        code = row['code']
        name = row.get('name', '')
        
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
                            
                            # 保留排名在前10名的股票（包含第10名）
                            if rank <= 10:
                                final_stocks.append({'code': code, 'name': name, 'ranking': ranking})
                                print(f"{code} {name} 排名 {ranking}，符合条件")
                        except (ValueError, TypeError):
                            print(f"  排名解析失败: {rank_part}")
                    elif isinstance(ranking, (int, float)):
                        print(f"  解析排名: {ranking}")
                        if ranking <= 10:
                            final_stocks.append({'code': code, 'name': name, 'ranking': ranking})
                            print(f"{code} {name} 排名 {ranking}，符合条件")
            else:
                print(f"  数据结构不符合预期，跳过")
                continue
        except Exception as e:
            print(f"处理股票 {code} {name} 时出错: {e}")
            continue
    
    print(f"\n估值排名筛选完成，符合条件的股票有 {len(final_stocks)} 只")
    
    # 创建结果DataFrame
    result_df = pd.DataFrame(final_stocks)
    
    # 保存为Excel文件
    if not result_df.empty:
        output_file = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/evaluate-fliter-1.xlsx"
        result_df.to_excel(output_file, index=False)
        print(f"已将筛选结果保存到: {output_file}")
    
    return result_df

# 步骤3：根据基本面信息筛选
def filter_by_fundamentals():
    print("\n正在根据基本面指标筛选股票...")
    final_stocks = []
    
    # 从evaluate-fliter-1.xlsx读取筛选后的股票
    input_file = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/evaluate-fliter-1.xlsx"
    try:
        filtered_stocks = pd.read_excel(input_file)
        print(f"已从 {input_file} 读取 {len(filtered_stocks)} 只股票")
    except Exception as e:
        print(f"读取文件失败: {e}")
        return pd.DataFrame()
    
    for index, row in filtered_stocks.iterrows():
        code = row['code']
        name = row['name']
        ranking = row['ranking']
        
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
                
                # 只取最新的一行数据
                latest_row = fundamental_data.iloc[0]
                
                # 提取基本面指标
                eps = latest_row.get('EPSJB', None)  # 基本每股收益(元)
                bps = latest_row.get('BPS', None)  # 每股净资产(元)
                parent_net_profit = latest_row.get('PARENTNETPROFIT', None)  # 归属净利润(元)
                parent_net_profit_growth = latest_row.get('PARENTNETPROFITTZ', None)  # 归属净利润同比增长(%)
                asset_liability_ratio = latest_row.get('ZCFZL', None)  # 资产负债率(%)
                
                print(f"  最新数据: 基本每股收益={eps}, 每股净资产={bps}, 归属净利润={parent_net_profit}, 归属净利润同比增长={parent_net_profit_growth}%, 资产负债率={asset_liability_ratio}%")
            else:
                print(f"  数据结构不符合预期，跳过")
                continue
            
            # 转换为数值
            try:
                eps = float(eps) if eps else None
                bps = float(bps) if bps else None
                parent_net_profit = float(parent_net_profit) if parent_net_profit else None
                parent_net_profit_growth = float(parent_net_profit_growth) if parent_net_profit_growth else None
                asset_liability_ratio = float(asset_liability_ratio) if asset_liability_ratio else None
            except (ValueError, TypeError):
                print(f"  数据转换失败，跳过")
                continue
            
            # 检查是否满足所有条件
            condition1 = eps > 0 if eps else False  # 基本每股收益(元) > 0
            condition2 = bps > 0 if bps else False  # 每股净资产(元) > 0
            condition3 = parent_net_profit > 0 if parent_net_profit else False  # 归属净利润(元) > 0
            condition4 = parent_net_profit_growth > 0 if parent_net_profit_growth else False  # 归属净利润同比增长(%) > 0
            condition5 = asset_liability_ratio < 60 if asset_liability_ratio else False  # 资产负债率(%) < 60%
            
            conditions = [condition1, condition2, condition3, condition4, condition5]
            all_conditions_met = all(conditions)
            satisfied_conditions = sum(conditions)
            
            print(f"  条件检查结果: 基本每股收益>0={condition1}, 每股净资产>0={condition2}, 归属净利润>0={condition3}, 归属净利润同比增长>0={condition4}, 资产负债率<60%={condition5}")
            print(f"  满足条件数量: {satisfied_conditions}/5")
            
            if all_conditions_met:
                final_stocks.append({'code': code, 'name': name, 'ranking': ranking, 'satisfied_conditions': satisfied_conditions})
                print(f"{code} {name} 符合条件（满足所有5项条件）")
            else:
                print(f"  未满足所有条件，跳过")
        except Exception as e:
            print(f"处理股票 {code} {name} 时出错: {e}")
            continue
    
    print(f"\n基本面筛选完成，符合条件的股票有 {len(final_stocks)} 只")
    
    # 创建结果DataFrame
    result_df = pd.DataFrame(final_stocks)
    
    # 保存为Excel文件
    if not result_df.empty:
        output_file = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/evaluate-fliter-2.xlsx"
        result_df.to_excel(output_file, index=False)
        print(f"已将筛选结果保存到: {output_file}")
    
    return result_df


# 主函数
def main():
    print("开始执行行业信息获取和筛选流程...")
    
    # 步骤2：根据估值比较排名筛选
    filter_by_valuation_ranking()
    
    # 步骤3：根据基本面信息筛选
    filter_by_fundamentals()
    
    
    print("\n流程执行完成！")

if __name__ == "__main__":
    main()
