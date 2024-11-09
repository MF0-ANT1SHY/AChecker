import pandas as pd
import os

path1 = "/home/shuo/repo/AChecker_normal/"  # 第一个 CSV 文件所在目录
filename1 = "VACC_duration.csv"  # 第一个 CSV 文件名

path2 = "/home/shuo/repo/AChecker/"  # 第二个 CSV 文件所在目录
filename2 = "VACC_duration.csv"  # 第二个 CSV 文件名

output_path = "/home/shuo/repo/AChecker_normal"  # 输出文件的目录
output_filename = "merged_output.csv"  # 输出文件名


def merge_csv_files(path1, filename1, path2, filename2, output_path, output_filename):
    """
    合并两个 CSV 文件，根据 'contract' 字段匹配。

    :param path1: 第一个 CSV 文件的目录路径
    :param filename1: 第一个 CSV 文件的文件名
    :param path2: 第二个 CSV 文件的目录路径
    :param filename2: 第二个 CSV 文件的文件名
    :param output_path: 输出文件的目录路径
    :param output_filename: 输出文件的文件名
    """
    # 构建完整的文件路径
    file1 = os.path.join(path1, filename1)
    file2 = os.path.join(path2, filename2)
    output_file = os.path.join(output_path, output_filename)

    # 读取 CSV 文件
    try:
        df1 = pd.read_csv(file1)
        print(f"成功读取文件: {file1}")
    except FileNotFoundError:
        print(f"错误: 找不到文件 {file1}")
        return
    except pd.errors.EmptyDataError:
        print(f"错误: 文件 {file1} 是空的")
        return
    except Exception as e:
        print(f"读取文件 {file1} 时发生错误: {e}")
        return

    try:
        df2 = pd.read_csv(file2)
        print(f"成功读取文件: {file2}")
    except FileNotFoundError:
        print(f"错误: 找不到文件 {file2}")
        return
    except pd.errors.EmptyDataError:
        print(f"错误: 文件 {file2} 是空的")
        return
    except Exception as e:
        print(f"读取文件 {file2} 时发生错误: {e}")
        return

    # 检查必要的列是否存在
    required_columns = ["contract", "duration"]
    for df, name in [(df1, filename1), (df2, filename2)]:
        for col in required_columns:
            if col not in df.columns:
                print(f"错误: 文件 {name} 缺少必要的列 '{col}'")
                return

    # 合并 DataFrame，根据 'contract' 字段进行匹配
    merged_df = pd.merge(
        df1, df2, on="contract", how="inner", suffixes=("_file1", "_file2")
    )

    # 检查是否有匹配结果
    if merged_df.empty:
        print("警告: 没有找到匹配的 'contract' 项。合并结果为空。")
    else:
        print(f"合并完成，匹配到 {len(merged_df)} 条记录。")

    # 创建输出目录（如果不存在）
    os.makedirs(output_path, exist_ok=True)

    # 保存合并后的 DataFrame 到新的 CSV 文件
    try:
        merged_df.to_csv(output_file, index=False)
        print(f"合并后的文件已保存到: {output_file}")
    except Exception as e:
        print(f"保存文件 {output_file} 时发生错误: {e}")


if __name__ == "__main__":
    merge_csv_files(path1, filename1, path2, filename2, output_path, output_filename)
