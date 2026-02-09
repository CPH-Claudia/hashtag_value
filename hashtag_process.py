import pandas as pd
import re

# -------------------------------
# 1) 拆分「純文字」與「#計畫」
# -------------------------------
_TAG_RE = re.compile(r"#[^\s#]+")

def split_note_and_tags(note):
    if pd.isna(note):
        return [], ""

    text = str(note).replace("＃", "#").replace("_x000D_", "")
    text = text.strip()
    if text == "":
        return [], ""

    lines = text.split("\n")

    tags = []
    plain_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith("#"):
            found = _TAG_RE.findall(line)  # 同行多 tag 也抓
            if found:
                tags.extend([t.strip() for t in found])
        else:
            plain_lines.append(line)

    # 去重但保留順序
    seen = set()
    tags_unique = []
    for t in tags:
        if t and t not in seen:
            tags_unique.append(t)
            seen.add(t)

    plain_text = "\n".join(plain_lines).strip()
    return tags_unique, plain_text


# -------------------------------
# 2) 主處理流程（精簡 + 只留備註非空）
# -------------------------------
def process_visit(df):
    df = df.copy()

    # 你截圖中一定有的欄位（作為 join key 與必要欄位）
    # 若你的欄名有不同（例如「拜訪時間_str」），在這裡改掉即可
    need_cols = [
        "客戶UUID",
        "業代",
        "營業單位",
        "拜訪紀錄UUID",
        # "拜訪時間",
        "拜訪備註",
    ]

    # 缺欄補空（避免 schema 對不上）
    for c in need_cols:
        if c not in df.columns:
            df[c] = ""

    # 先砍欄位（降載）
    df = df[need_cols].copy()

    # 拜訪紀錄 UUID 清理（避免空白）
    df["拜訪紀錄UUID"] = df["拜訪紀錄UUID"].fillna("").astype(str).str.strip()

    # 備註清理
    note_raw = df["拜訪備註"].fillna("").astype(str).str.replace("_x000D_", "", regex=False)
    note_str = note_raw.str.strip()

    # 只保留「拜訪備註非空」
    df = df[note_str != ""].copy()
    note_str = note_str.loc[df.index]  # 對齊 index

    # Row-level features（只針對非空備註）
    df["_has_note"] = 1
    df["_note_len"] = note_str.str.len().astype(int)

    parsed = df["拜訪備註"].apply(split_note_and_tags)
    tags_list = parsed.apply(lambda x: x[0])
    plain_text = parsed.apply(lambda x: x[1])

    df["#計畫"] = tags_list.apply(lambda tags: "\n".join(tags) if tags else "")
    df["純文字"] = plain_text

    df["_tag_cnt"] = tags_list.apply(lambda tags: len(tags) if tags else 0).astype(int)
    df["_uniq_tag_cnt"] = tags_list.apply(lambda tags: len(set(tags)) if tags else 0).astype(int)
    df["_has_tag"] = (df["_tag_cnt"] > 0).astype(int)

    # 是否有拜訪紀錄（仍保留，方便下游檢核/ join）
    df["_has_record"] = (df["拜訪紀錄UUID"] != "").astype(int)

    # # 統一處理拜訪時間（轉字串避免 TabPy JSON error）
    # dt = pd.to_datetime(df["拜訪時間"], errors="coerce")
    # df["拜訪時間"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

    # 最終輸出（精簡欄位 + 特徵欄位）
    out_cols = [
        "拜訪紀錄UUID",
        "客戶UUID",
        "業代",
        "營業單位",
        # "拜訪時間",
        # "拜訪備註",

        "_has_record",
        "_has_note",
        "_note_len",
        "_has_tag",
        "_tag_cnt",
        "_uniq_tag_cnt",

        "#計畫",
        "純文字",
    ]
    return df[out_cols]


# -------------------------------
# 3) Output schema（Prep 用）
# -------------------------------
def get_output_schema():
    return pd.DataFrame({
        "拜訪紀錄UUID": prep_string(),
        "客戶UUID": prep_string(),
        "業代": prep_string(),
        "營業單位": prep_string(),
        # "拜訪時間": prep_string(),
        # "拜訪備註": prep_string(),

        "_has_record": prep_int(),
        "_has_note": prep_int(),
        "_note_len": prep_int(),
        "_has_tag": prep_int(),
        "_tag_cnt": prep_int(),
        "_uniq_tag_cnt": prep_int(),

        "#計畫": prep_string(),
        "純文字": prep_string(),
    })
