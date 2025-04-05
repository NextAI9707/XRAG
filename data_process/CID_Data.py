import pandas as pd

# 1. 读取txt文件并创建CID到STY的映射字典
cid_to_sty = {}
with open('SemanticTypes.txt', 'r') as f:
    for line in f:
        # 分割每一行
        parts = line.strip().split('|')
        if len(parts) == 3:
            cid, styid, sty = parts
            cid_to_sty[cid] = sty

# 2. 读取Excel文件
df = pd.read_excel('triples_data.xlsx')

# 3. 替换B列的值
# 假设B列是第1列（索引从0开始）
df.iloc[:, 1] = df.iloc[:, 1].map(lambda x: cid_to_sty.get(str(x), str(x)))

# 4. 替换D列的值
# 假设D列是第3列（索引从0开始）
df.iloc[:, 6] = df.iloc[:, 6].map(lambda x: cid_to_sty.get(str(x), str(x)))

# 5. 保存修改后的Excel文件
df.to_excel('triples_data_updated.xlsx', index=False)

print("处理完成，已生成新文件：triples_data_updated.xlsx")