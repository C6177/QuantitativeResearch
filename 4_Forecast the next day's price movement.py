import akshare as ak
import pandas as pd
import numpy as np
import talib
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix

# ==============================================
# 1. 获取股票数据
# ==============================================
def fetch_stock_data(symbol, start_date, end_date):
    """使用AKShare获取历史数据"""
    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust="hfq"  # 后复权保证数据连续性
    )
    
    # 重命名列为英文方便处理
    df.columns = ['date', 'code', 'open', 'close', 'high', 'low', 
                  'volume', 'amount', 'amplitude', 'pct_change', 
                  'change', 'turnover']
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)
    return df

# ==============================================
# 2. 特征工程
# ==============================================
def create_features(df):
    """构建技术指标特征集"""
    df = df.copy()
    
    # 价格数据
    open = df['open'].values.astype(float)
    close = df['close'].values.astype(float)
    high = df['high'].values.astype(float)
    low = df['low'].values.astype(float)
    volume = df['volume'].values.astype(float)
    
    # --- 移动平均线 ---
    df['MA5'] = talib.SMA(close, timeperiod=5)
    df['MA10'] = talib.SMA(close, timeperiod=10)
    df['MA20'] = talib.SMA(close, timeperiod=20)
    df['MA60'] = talib.SMA(close, timeperiod=60)
    
    # 价格与均线距离
    df['dist_MA5'] = (close - df['MA5']) / df['MA5']
    df['dist_MA20'] = (close - df['MA20']) / df['MA20']
    
    # --- RSI指标 ---
    df['RSI_6'] = talib.RSI(close, timeperiod=6)
    df['RSI_14'] = talib.RSI(close, timeperiod=14)
    df['RSI_24'] = talib.RSI(close, timeperiod=24)
    
    # --- MACD指标 ---
    df['MACD_DIF'], df['MACD_DEA'], df['MACD_bar'] = talib.MACD(
        close, fastperiod=12, slowperiod=26, signalperiod=9
    )
    
    # --- 布林带 ---
    df['BB_upper'], df['BB_middle'], df['BB_lower'] = talib.BBANDS(
        close, timeperiod=20, nbdevup=2, nbdevdn=2
    )
    df['BB_position'] = (close - df['BB_lower']) / (df['BB_upper'] - df['BB_lower'])
    
    # --- ATR波动率 ---
    df['ATR'] = talib.ATR(high, low, close, timeperiod=14)
    df['ATR_ratio'] = df['ATR'] / close  # 相对波动率
    
    # --- 成交量指标 ---
    df['VOL_MA5'] = talib.SMA(volume, timeperiod=5)
    df['VOL_ratio'] = volume / df['VOL_MA5']  # 量比
    df['OBV'] = talib.OBV(close, volume)  # 能量潮
    
    # --- 价格形态特征 ---
    # 是否创新高/新低
    df['high_20'] = df['high'].rolling(20).max()
    df['low_20'] = df['low'].rolling(20).min()
    df['is_new_high'] = (df['high'] == df['high_20']).astype(int)
    df['is_new_low'] = (df['low'] == df['low_20']).astype(int)
    
    # 收益率特征
    df['return_1d'] = df['close'].pct_change(1)
    df['return_5d'] = df['close'].pct_change(5)
    df['return_20d'] = df['close'].pct_change(20)
    
    # 隔夜收益率 (开盘/前日收盘)
    df['overnight_return'] = df['open'] / df['close'].shift(1) - 1
    
    # 日内波动率
    df['intraday_volatility'] = (df['high'] - df['low']) / df['close']
    
    # --- 目标变量：次日是否上涨 ---
    df['target'] = (df['close'].shift(-1) > df['close']).astype(int)
    
    # 删除NaN值
    df.dropna(inplace=True)
    
    return df

# ==============================================
# 3. 训练预测模型
# ==============================================
def train_prediction_model(df):
    """使用XGBoost训练涨跌预测模型"""
    
    # 选择特征列（排除非特征列）
    feature_cols = [col for col in df.columns if col not in ['code', 'target', 
                   'open', 'high', 'low', 'close', 'volume', 'amount', 
                   'amplitude', 'pct_change', 'change', 'turnover']]
    
    X = df[feature_cols]
    y = df['target']
    
    # 划分训练集和测试集（按时间顺序，避免未来数据泄露）
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    # 训练XGBoost模型
    model = XGBClassifier(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
        use_label_encoder=False,
        eval_metric='logloss'
    )
    
    model.fit(X_train, y_train)
    
    # 评估
    train_acc = accuracy_score(y_train, model.predict(X_train))
    test_acc = accuracy_score(y_test, model.predict(X_test))
    
    print(f"训练集准确率: {train_acc:.4f}")
    print(f"测试集准确率: {test_acc:.4f}")
    
    # 特征重要性
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\n特征重要性前10:")
    print(importance.head(10))
    
    return model, feature_cols

# ==============================================
# 4. 预测次日涨跌
# ==============================================
def predict_next_day(model, feature_cols, df):
    """预测下一个交易日的涨跌"""
    # 获取最新一天的特征数据
    latest_features = df[feature_cols].iloc[-1:].values
    
    # 预测
    prob = model.predict_proba(latest_features)[0]
    pred = model.predict(latest_features)[0]
    
    print(f"\n次日上涨概率: {prob[1]:.2%}")
    print(f"预测结果: {'上涨' if pred == 1 else '下跌'}")
    
    return pred, prob[1]

# ==============================================
# 主程序
# ==============================================
if __name__ == "__main__":
    # 参数设置
    start_date = "20251201"
    end_date = "20260310"
    
    print("开始执行预测程序...")
    
    # 读取股票列表
    input_file = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/evaluate-fliter-1.xlsx"
    print(f"读取股票列表: {input_file}")
    
    try:
        stock_list = pd.read_excel(input_file)
        print(f"成功读取 {len(stock_list)} 只股票")
        print(f"文件列名: {stock_list.columns.tolist()}")
        print(f"前5行数据:")
        print(stock_list.head())
    except Exception as e:
        print(f"读取文件失败: {e}")
        import traceback
        traceback.print_exc()
        exit()
    
    # 存储预测结果
    prediction_results = []
    
    # 逐个分析股票
    total_stocks = len(stock_list)
    for index, row in stock_list.iterrows():
        code = row['code']
        # 将股票代码转换为字符串类型
        code = str(code)
        # 确保股票代码是6位数字
        code = code.zfill(6)
        name = row.get('name', '')
        
        print(f"\n分析第 {index+1}/{total_stocks} 只股票: {code} {name}")
        
        try:
            # 1. 获取数据
            print("获取数据中...")
            raw_data = fetch_stock_data(code, start_date, end_date)
            
            # 2. 特征工程
            print("构建特征...")
            feature_data = create_features(raw_data)
            
            # 3. 训练模型
            print("训练模型...")
            model, features = train_prediction_model(feature_data)
            
            # 4. 预测下一个交易日
            print("预测次日涨跌...")
            pred, prob = predict_next_day(model, features, feature_data)
            
            # 保存结果
            prediction_results.append({
                'code': code,
                'name': name,
                '上涨概率': prob,
                '预测结果': '上涨' if pred == 1 else '下跌'
            })
            
        except Exception as e:
            print(f"分析股票 {code} 时出错: {e}")
            continue
    
    # 输出结果到Excel
    if prediction_results:
        print(f"\n预测完成，共分析 {len(prediction_results)} 只股票")
        
        # 创建结果DataFrame
        result_df = pd.DataFrame(prediction_results)
        
        # 保存为Excel文件
        output_path = "C:/Users/ZJH/Documents/浙江广电-前端开发项目/QuantitativeResearch/prediction.xlsx"
        result_df.to_excel(output_path, index=False)
        print(f"已将预测结果保存到: {output_path}")
        
        # 打印前几行结果
        print("\n预测结果预览:")
        print(result_df.head())
    else:
        print("\n没有成功预测的股票")