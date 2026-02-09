import pandas as pd
import numpy as np

# =========================
# 可調整參數
# =========================
SUMMER_MONTHS = {1, 2, 3, 4, 5, 6}   # 若只要 3-6 改成 {3,4,5,6}
WINTER_MONTHS = {7, 8, 9, 10, 11, 12}

VISIT_ID  = "拜訪紀錄UUID_visit"
VISIT_DT  = "拜訪時間"
POLICY_ID = "保單申請案號" 
POLICY_DT = "投保日"
FYC       = "繳款FYC"


# ====== 日期解析（容錯）======
def parse_dt(x):
    if x is None:
        return pd.NaT
    if isinstance(x, (pd.Timestamp, np.datetime64)):
        return pd.to_datetime(x, errors="coerce")
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return pd.NaT
    s = s.replace("上午", "AM").replace("下午", "PM")
    return pd.to_datetime(s, errors="coerce")

def to_safe_str_datetime(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    out = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    return out.fillna("")

def script(df):
    if isinstance(df, dict):
        df = pd.DataFrame(df)

    if df is None or df.empty:
        return pd.DataFrame(columns=[VISIT_ID, "visit_fyc_in_season"])

    df = df.copy()

    # 必要欄位不存在就補（避免整支爆掉）
    for c in [VISIT_ID, VISIT_DT, POLICY_ID, POLICY_DT, FYC]:
        if c not in df.columns:
            df[c] = None

    # 清理型別
    df[VISIT_ID]  = df[VISIT_ID].fillna("").astype(str).str.strip()
    df[POLICY_ID] = df[POLICY_ID].fillna("").astype(str).str.strip()
    df[VISIT_DT]  = df[VISIT_DT].apply(parse_dt)
    df[POLICY_DT] = df[POLICY_DT].apply(parse_dt)
    df[FYC] = pd.to_numeric(df[FYC], errors="coerce").fillna(0.0)

    # =========================================================
    # ✅ Step 1：先做「保單層級去重」：每張保單只拿一次 FYC
    # ---------------------------------------------------------
    # 用 MAX：適用於「保單FYC被重複貼在每個商品列」的情況（你描述很像這個）
    # 若你的FYC其實是商品拆分，請把 max 改成 sum
    # =========================================================
    policy_level = (
        df[df[POLICY_ID] != ""]
        .groupby([VISIT_ID, POLICY_ID], as_index=False)
        .agg(
            policy_fyc=(FYC, "max"),          # ←若需商品加總改成 "sum"
            policy_dt=(POLICY_DT, "max"),     # 投保日通常同保單一致，取 max/first 都可
            visit_dt=(VISIT_DT, "max")        # 同拜訪一致
        )
    )

    if policy_level.empty:
        # 沒有保單 → 所有拜訪價值=0（仍回原表）
        df["visit_fyc_in_season"] = 0.0
    else:
        # =========================================================
        # ✅ Step 2：算賽期結束日（同年度 6/30 或 12/31）
        # =========================================================
        vm = policy_level["visit_dt"].dt.month
        y  = policy_level["visit_dt"].dt.year

        season_end = pd.Series(pd.NaT, index=policy_level.index, dtype="datetime64[ns]")

        mask_s = vm.isin(list(SUMMER_MONTHS)) & policy_level["visit_dt"].notna()
        season_end.loc[mask_s] = pd.to_datetime(dict(year=y[mask_s], month=6, day=30), errors="coerce")

        mask_w = vm.isin(list(WINTER_MONTHS)) & policy_level["visit_dt"].notna()
        season_end.loc[mask_w] = pd.to_datetime(dict(year=y[mask_w], month=12, day=31), errors="coerce")

        # =========================================================
        # ✅ Step 3：只算「拜訪後～賽期結束」的保單FYC
        # =========================================================
        valid = (
            policy_level["policy_dt"].notna() &
            policy_level["visit_dt"].notna() &
            season_end.notna() &
            (policy_level["policy_dt"] >= policy_level["visit_dt"]) &
            (policy_level["policy_dt"] <= season_end)
        )

        policy_level["_fyc_valid"] = np.where(valid, policy_level["policy_fyc"], 0.0)

        # =========================================================
        # ✅ Step 4：拜訪層級加總（每張保單只算一次）
        # =========================================================
        visit_fyc = (
            policy_level.groupby(VISIT_ID, as_index=False)["_fyc_valid"]
            .sum()
            .rename(columns={"_fyc_valid": "visit_fyc_in_season"})
        )

        df = df.merge(visit_fyc, on=VISIT_ID, how="left")
        df["visit_fyc_in_season"] = df["visit_fyc_in_season"].fillna(0.0)

    # =========================================================
    # ✅ 避免 NaT JSON 序列化：把 datetime 欄位轉字串
    # =========================================================
    if VISIT_DT in df.columns:
        df[VISIT_DT] = to_safe_str_datetime(df[VISIT_DT])
    if POLICY_DT in df.columns:
        df[POLICY_DT] = to_safe_str_datetime(df[POLICY_DT])

    # 若還有其他 datetime 欄，保險起見全部轉字串
    dt_cols = df.select_dtypes(include=["datetime64[ns]"]).columns.tolist()
    for c in dt_cols:
        df[c] = to_safe_str_datetime(df[c])

    return df


def get_output_schema():
    # 只宣告你新增/改型別的欄位最重要
    return pd.DataFrame({
        VISIT_ID: prep_string(),
        "visit_fyc_in_season": prep_decimal(),
        VISIT_DT: prep_string(),
        POLICY_DT: prep_string(),
    })
