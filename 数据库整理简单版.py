import os
import argparse
import pandas as pd
from sqlalchemy import create_engine, inspect


def backup_and_truncate_all_tables(db_url: str, backup_dir: str, keep_rows: int = 100, verbose: bool = True):
    os.makedirs(backup_dir, exist_ok=True)

    engine = create_engine(db_url)
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    if verbose:
        print(f"检测到 {len(table_names)} 个表: {table_names}")

    result_info = []

    with engine.begin() as conn:
        for table_name in table_names:
            if verbose:
                print(f"读取表：{table_name}")

            try:
                df = pd.read_sql_table(table_name, conn)
            except Exception as e:
                print(f"读取表 {table_name} 失败: {e}")
                continue

            # 备份单表数据到本地文件（parquet / csv）
            backup_path = os.path.join(backup_dir, f"{table_name}.parquet")
            try:
                df.to_parquet(backup_path, index=False)
                if verbose:
                    print(f"已备份: {backup_path} (共 {len(df)} 条)")
            except Exception as ex:
                csv_path = os.path.join(backup_dir, f"{table_name}.csv")
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                if verbose:
                    print(f"parquet 写入失败，已改为 csv 备份: {csv_path}")

            # 新表只保留指定行数
            df_small = df.head(keep_rows)
            if verbose:
                print(f"{table_name} 仅保留前 {len(df_small)} 条")

            # 重新写回表（替换现有表）
            try:
                df_small.to_sql(table_name, conn, if_exists='replace', index=False)
                if verbose:
                    print(f"已重建并写入：{table_name} (仅 {len(df_small)} 条)")
                result_info.append((table_name, len(df), len(df_small)))
            except Exception as err:
                print(f"写回表 {table_name} 失败: {err}")

    return result_info


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='备份并截断数据库所有表，默认保留100条数据')
    parser.add_argument('--db-url', required=False, default='sqlite:///data.db',
                        help='数据库连接 URL，例如 sqlite:///data.db 或 mysql+pymysql://user:pass@host/db')
    parser.add_argument('--backup-dir', required=False, default='backup',
                        help='备份目录（如果不存在则创建）')
    parser.add_argument('--keep-rows', type=int, default=100,
                        help='每个表保留的行数，默认为100')
    args = parser.parse_args()

    print('如果你不懂参数，可以直接输入，回车使用默认值。')

    db_url = input(f"数据库 URL （默认 {args.db_url}）：") .strip()
    if not db_url:
        db_url = args.db_url

    backup_dir = input(f"备份目录 （默认 {args.backup_dir}）：").strip()
    if not backup_dir:
        backup_dir = args.backup_dir

    keep_rows_text = input(f"每表保留行数 （默认 {args.keep_rows}）：").strip()
    if keep_rows_text:
        try:
            keep_rows = int(keep_rows_text)
        except ValueError:
            print('无效数字，使用默认保留行数。')
            keep_rows = args.keep_rows
    else:
        keep_rows = args.keep_rows

    print('开始备份并截断数据库表...')
    info = backup_and_truncate_all_tables(db_url, backup_dir, keep_rows)
    print('操作完成！')
    for table_name, old_count, new_count in info:
        print(f"{table_name}: 原始 {old_count} -> 保留 {new_count}")

    print('\n示例运行：')
    print('python 数据库整理.py --db-url "mysql+pymysql://root:pwd@localhost/dbname" --backup-dir ".\\backup" --keep-rows 100')

