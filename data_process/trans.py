import pandas as pd
from deep_translator import GoogleTranslator
import re
import time
from collections import defaultdict
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ========== 配置项 ==========
TERM_DB_PATH = "data_process/term_base.csv"  # 术语库路径
MAX_RETRIES = 3  # 最大重试次数
REQUEST_TIMEOUT = 10  # 请求超时时间
BATCH_SIZE = 30  # 减小批量大小
USE_PROXY = False  # 是否启用代理
PROXY = {"https": "http://127.0.0.1:7890"}  # 代理地址


# ========== 中文检测函数 ==========
def contains_chinese(text):
    """检测字符串是否包含中文字符"""
    pattern = re.compile(r'[\u4e00-\u9fff]')
    return bool(pattern.search(text))


# ========== 术语处理模块 ==========
class TermProcessor:
    def __init__(self):
        self.term_base = self.load_term_base()
        self.pattern = self.build_regex_pattern()

    def load_term_base(self):
        try:
            if TERM_DB_PATH.endswith('.csv'):
                df = pd.read_csv(TERM_DB_PATH)
            else:
                df = pd.read_excel(TERM_DB_PATH)
            return dict(zip(df['source'].str.lower(), df['target']))
        except Exception as e:
            print(f"术语库加载失败: {str(e)}")
            return {}

    def build_regex_pattern(self):
        terms = sorted(self.term_base.keys(), key=len, reverse=True)
        escaped_terms = [re.escape(term) for term in terms]
        return re.compile(r'\b(' + '|'.join(escaped_terms) + r')\b', flags=re.IGNORECASE)

    def replace_terms(self, text):
        def replace_match(match):
            matched_term = match.group(0).lower()
            return self.term_base.get(matched_term, match.group(0))

        return self.pattern.sub(replace_match, text)


# ========== 网络会话配置 ==========
def create_http_session():
    """创建带重试机制的会话"""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=['GET', 'POST']
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session


# ========== 核心翻译函数 ==========
def translate_excel_columns(input_path, output_path):
    # 初始化组件
    term_processor = TermProcessor()
    session = create_http_session()

    # 配置翻译器
    translator = GoogleTranslator(
        source='en',
        target='zh-CN',
        session=session,
        proxies=PROXY if USE_PROXY else None,
        timeout=REQUEST_TIMEOUT
    )

    # 读取数据
    df = pd.read_excel(input_path, header=0)
    target_columns = [1, 3, 6, 8]  # 需要翻译的列索引
    translation_cache = defaultdict(str)

    # 预处理：收集需要翻译的文本
    text_mapping = defaultdict(list)  # {processed_text: [(col, row)]}
    for col_idx in target_columns:
        for row_idx in range(1, len(df)):  # 跳过标题行
            cell = df.iloc[row_idx, col_idx]

            # 跳过非文本内容
            if pd.isna(cell) or not isinstance(cell, str):
                continue

            stripped = cell.strip()
            if not stripped:
                continue

            # 跳过已含中文的内容
            if contains_chinese(stripped):
                continue

            # 执行术语替换
            processed = term_processor.replace_terms(stripped)
            text_mapping[processed].append((col_idx, row_idx))

    # 去重处理
    unique_texts = list(text_mapping.keys())
    print(f"需要翻译的文本数量：{len(unique_texts)}")

    # 批量翻译
    for i in range(0, len(unique_texts), BATCH_SIZE):
        batch = unique_texts[i:i + BATCH_SIZE]
        for attempt in range(MAX_RETRIES + 1):
            try:
                translated = translator.translate_batch(batch)
                # 更新缓存
                for src, tgt in zip(batch, translated):
                    translation_cache[src] = tgt
                print(f"成功翻译：{min(i + BATCH_SIZE, len(unique_texts))}/{len(unique_texts)}")
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"翻译失败: {str(e)}")
                    break
                wait_time = (attempt + 1) * 5
                print(f"第{attempt + 1}次重试，等待{wait_time}秒...")
                time.sleep(wait_time)

    # 应用翻译结果
    for processed_text, positions in text_mapping.items():
        translated = translation_cache.get(processed_text, None)
        if translated:
            for (col_idx, row_idx) in positions:
                df.iloc[row_idx, col_idx] = translated

    # 保存结果
    df.to_excel(output_path, index=False)
    print(f"文件已保存：{output_path}")


# 使用示例
translate_excel_columns("data_process/triples_data_updated.xlsx", "data_process/output_plus1.xlsx")