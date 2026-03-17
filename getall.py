import akshare as ak
import pandas as pd
import numpy as np

# ==============================================
# 1. 获取所有A股股票代码和名称
# ==============================================
def get_all_a_stocks():
    print("正在获取A股列表...")
    # 使用 stock_info_sh_name_code 接口获取沪股列表
    print("使用 stock_info_sz_name_code 接口...")
    stock_list = ak.stock_info_sz_name_code()
    # 提取证券代码和证券简称
    stock_list = stock_list[['A股代码', 'A股简称']].rename(columns={'A股代码': 'code', 'A股简称': 'name'})
    print(f"获取到 {len(stock_list)} 只深股股票")
    return stock_list

if __name__ == "__main__":
    # 配置参数


    # 1. 获取所有A股股票列表
    stock_list = get_all_a_stocks()
    total_stocks = len(stock_list)
    print(f"开始分析 {total_stocks} 只股票...")
    
    # 2. 处理股票代码格式，添加.SH后缀
    stock_list['code'] = stock_list['code'] + '.SZ'
    
    # 3. 输出到Excel文件
    output_path = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/sz-code.xlsx"
    stock_list.to_excel(output_path, index=False)
    print(f"已将股票代码输出到: {output_path}")
    
    # 4. 打印前几行结果预览
    print("\n输出结果预览:")
    print(stock_list.head())
