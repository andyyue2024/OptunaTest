import time
import numpy as np
import pandas as pd
import optuna
import xgboost as xgb
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# --- 1. 准备数据 ---
# 创建一个合成的分类数据集
X, y = make_classification(
    n_samples=1000,
    n_features=20,
    n_informative=10,
    n_redundant=5,
    n_classes=2,
    random_state=42
)

# 划分训练集和验证集
X_train, X_valid, y_train, y_valid = train_test_split(
    X, y, test_size=0.25, random_state=42
)

print(f"训练集大小: {X_train.shape}, 验证集大小: {X_valid.shape}")


# --- 2. 定义多目标优化函数 ---
def objective(trial):
    """定义要优化的目标函数"""

    # --- 2.1 定义超参数的搜索空间 ---
    # Optuna 会在这些空间中进行采样
    param = {
        "verbosity": 0,
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "booster": "gbtree",
        "n_estimators": trial.suggest_int("n_estimators", 50, 500),
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "random_state": 42,
        "n_jobs": -1,  # 使用所有可用的CPU核心
    }

    # --- 2.2 训练模型并计算目标值 ---
    # 目标1: 错误率 (1 - accuracy)
    # 目标2: 训练时间

    # 记录开始时间
    start_time = time.time()

    # 创建并训练 XGBoost 模型
    model = xgb.XGBClassifier(**param)
    model.fit(X_train, y_train)

    # 计算训练耗时
    elapsed_time = time.time() - start_time

    # 在验证集上评估模型
    y_pred = model.predict(X_valid)
    accuracy = accuracy_score(y_valid, y_pred)
    error_rate = 1.0 - accuracy

    # --- 2.3 返回目标值 ---
    # 返回值的顺序必须与 create_study 中的 directions 列表顺序一致
    # 这里我们返回 [错误率, 训练时间]
    return error_rate, elapsed_time


# --- 3. 创建并执行 Study ---
# 定义优化的方向：两个目标都是最小化
# directions 列表的顺序与 objective 函数返回值的顺序严格对应
study = optuna.create_study(directions=["minimize", "minimize"])

# 开始优化，例如进行 100 次试验
print("\n开始多目标优化...")
study.optimize(objective, n_trials=10, timeout=60)  # timeout 设置为 10 分钟，防止运行过久
print("优化完成！")

# --- 4. 分析结果 ---
print(f"\n研究编号: {study.study_name}")
print(f"试验总次数: {len(study.trials)}")

# 获取所有帕累托最优解
pareto_trials = study.best_trials
print(f"\n找到 {len(pareto_trials)} 个帕累托最优解。")

# 打印所有帕累托最优解的详细信息
print("\n--- 帕累托最优解列表 ---")
for i, trial in enumerate(pareto_trials):
    print(f"\n解 #{i + 1}:")
    print(f"  - 超参数: {trial.params}")
    print(f"  - 目标值 (错误率, 训练时间): {trial.values}")

# 将所有试验结果转换为 DataFrame 方便查看
trials_df = study.trials_dataframe()
print("\n--- 所有试验结果 DataFrame (前5行) ---")
# 列名会自动生成，如 'params_n_estimators', 'values_0', 'values_1'
print(trials_df.head())

# --- 5. 可视化帕累托前沿 ---
print("\n正在生成帕累托前沿图...")
try:
    fig = optuna.visualization.plot_pareto_front(
        study,
        target_names=["错误率", "训练时间"],
    )
    fig.show()
    print("图表已显示。")
except Exception as e:
    print(f"可视化失败，请确保在支持图形界面的环境中运行: {e}")

