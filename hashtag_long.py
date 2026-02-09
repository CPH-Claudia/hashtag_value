import pandas as pd
import re

TAG_COL   = "#計畫"
VISIT_COL = "拜訪紀錄UUID"
UNIT_COL  = "營業單位"
AGENT_COL = "業代"

# 先保留 True，避免又遇到「空表就被 Prep 當成沒結果」的狀況
KEEP_NO_TAG_ROWS = True

# 同一拜訪同一 hashtag 去重（文字雲通常建議 True）
DEDUP_TAG_PER_VISIT = True

# 英文一致化：#100P -> #100p
NORMALIZE_LOWER = True


def _to_df(x):
    if isinstance(x, dict):
        return pd.DataFrame(x)
    return x


def split_plan_to_tags(plan_text):
    """
    ✅ 核心抽取邏輯：抓每一行裡的 '#......' 到行尾
    - 同時支援：真的換行、字面 \\n、字面 /n
    - 不要求 # 在行首（前面有空白/引號也能抓）
    """
    if pd.isna(plan_text):
        return []

    s = str(plan_text)
    s = s.replace("＃", "#").replace("_x000D_", "").strip()

    # 去掉整格被引號包住的狀況（你範例有）
    s = s.strip('"').strip("'").strip()

    if s == "":
        return []

    # 容錯：字面 \n 或 /n 轉成真正換行
    s = s.replace("\\n", "\n").replace("/n", "\n")

    # ✅ 抓出每行中的 hashtag 片段：從 # 到行尾
    tags = re.findall(r"#[^\r\n]+", s)

    cleaned = []
    for t in tags:
        t = t.strip()
        if t == "#" or t == "":
            continue
        # 收斂連續 ##
        t = re.sub(r"#{2,}", "#", t)
        # 去掉頭尾引號
        t = t.strip('"').strip("'").strip()
        if NORMALIZE_LOWER:
            t = t.lower()
        cleaned.append(t)

    # 去重但保留順序
    if DEDUP_TAG_PER_VISIT:
        seen = set()
        uniq = []
        for t in cleaned:
            if t not in seen:
                uniq.append(t)
                seen.add(t)
        return uniq

    return cleaned


def script(df):
    df = _to_df(df)

    # 保底：避免回傳 None 造成「didn't return any results」
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame(columns=[VISIT_COL, "hashtag", UNIT_COL, AGENT_COL, "tag_cnt"])

    df = df.copy()

    # 缺欄補空，避免 KeyError
    for c in [VISIT_COL, TAG_COL, UNIT_COL, AGENT_COL]:
        if c not in df.columns:
            df[c] = ""

    df["_tags_list"] = df[TAG_COL].apply(split_plan_to_tags)

    if KEEP_NO_TAG_ROWS:
        df["_tags_list"] = df["_tags_list"].apply(lambda xs: xs if len(xs) > 0 else [""])
    else:
        df = df[df["_tags_list"].map(len) > 0].copy()

    out = df.explode("_tags_list", ignore_index=True).rename(columns={"_tags_list": "hashtag"})
    out["hashtag"] = out["hashtag"].fillna("").astype(str).str.strip()

    if not KEEP_NO_TAG_ROWS:
        out = out[out["hashtag"] != ""].copy()

    if DEDUP_TAG_PER_VISIT:
        out = out.drop_duplicates(subset=[VISIT_COL, "hashtag"])

    out["tag_cnt"] = 1

    # 只留你要的 5 欄
    keep_cols = [VISIT_COL, "hashtag", UNIT_COL, AGENT_COL, "tag_cnt"]
    out = out[keep_cols].copy()

    return out


def get_output_schema():
    return pd.DataFrame({
        "拜訪紀錄UUID": prep_string(),
        "hashtag": prep_string(),
        "營業單位": prep_string(),
        "業代": prep_string(),
        "tag_cnt": prep_int(),
    })



