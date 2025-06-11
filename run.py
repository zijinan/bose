#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双波数据采集系统 - 主运行脚本（优化版）
"""

import os
import sys
import subprocess
from pathlib import Path


def print_banner():
    """打印程序横幅"""
    print("\n" + "="*50)
    print("        双波数据采集系统")
    print("="*50)
    print("功能：采集双波关键词和波色数据")
    print("特性：高效解析、质量过滤、性能优化")
    print("="*50)


def print_menu():
    """打印菜单"""
    print("\n选择操作:")
    print("1. 预处理链接文件（去重优化）")
    print("2. 运行数据采集爬虫")
    print("3. 运行数据分析程序")
    print("4. 查看数据文件状态")
    print("5. 安装依赖包")
    print("0. 退出程序")
    print("-" * 30)


def check_dependencies():
    """检查关键依赖项"""
    deps = ['requests', 'pandas', 'beautifulsoup4', 'lxml', 'tqdm']
    missing = []
    
    for dep in deps:
        try:
            if dep == 'beautifulsoup4':
                __import__('bs4')
            else:
                __import__(dep)
            print(f"✓ {dep}")
        except ImportError:
            missing.append(dep)
            print(f"✗ {dep}")
    
    if missing:
        print(f"\n缺少依赖: {', '.join(missing)}")
        try:
            subprocess.run([sys.executable, '-m', 'pip', 'install'] + missing, check=True)
            print("✓ 依赖安装完成")
            return True
        except Exception as e:
            print(f"✗ 安装失败: {e}")
            return False
    
    return True


def install_dependencies():
    """安装依赖包"""
    print("正在安装依赖包...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✓ 依赖安装完成")
        return True
    except Exception as e:
        print(f"✗ 安装失败: {e}")
        return False


def check_data_files():
    """检查数据文件状态"""
    print("\n" + "="*30)
    print("数据文件状态")
    print("="*30)
    
    files = {
        "data/links.csv": "原始链接文件",
        "data/links_processed.csv": "预处理链接文件",
        "data/results.csv": "采集结果文件"
    }
    
    for file_path, desc in files.items():
        path = Path(file_path)
        if path.exists():
            size = path.stat().st_size
            if size > 1024*1024:
                size_str = f"{size/1024/1024:.1f} MB"
            else:
                size_str = f"{size/1024:.1f} KB"
            print(f"✓ {desc}: {size_str}")
        else:
            print(f"✗ {desc}: 不存在")


def run_preprocess():
    """运行链接预处理"""
    print("\n启动链接预处理...")
    
    if not Path("data/links.csv").exists():
        print("✗ 输入文件 data/links.csv 不存在")
        return
    
    if not check_dependencies():
        print("✗ 依赖检查失败")
        return
    
    try:
        os.chdir("scripts")
        subprocess.run([sys.executable, "preprocess_links.py"], check=True)
        os.chdir("..")
        print("✓ 链接预处理完成")
    except Exception as e:
        print(f"✗ 预处理失败: {e}")
        os.chdir("..")


def run_crawler():
    """运行数据采集爬虫"""
    print("\n启动数据采集爬虫...")
    
    # 检查输入文件
    if Path("data/links_processed.csv").exists():
        print("使用预处理链接文件")
    elif Path("data/links.csv").exists():
        print("使用原始链接文件（建议先预处理）")
    else:
        print("✗ 链接文件不存在")
        return
    
    if not check_dependencies():
        print("✗ 依赖检查失败")
        return
    
    # 获取期数
    while True:
        try:
            period = input("输入采集期数（如：160）: ").strip()
            if period:
                target_period = int(period)
                break
            print("期数不能为空")
        except ValueError:
            print("期数格式错误")
        except KeyboardInterrupt:
            print("\n操作取消")
            return
    
    print(f"目标期数: {target_period}期")
    
    try:
        os.chdir("scripts")
        subprocess.run([sys.executable, "crawler.py", str(target_period)], check=True)
        os.chdir("..")
        print(f"✓ 采集完成，结果保存到: data/results_{target_period}.csv")
    except Exception as e:
        print(f"✗ 采集失败: {e}")
        os.chdir("..")


def run_analysis():
    """运行数据分析"""
    print("\n启动数据分析...")
    
    # 查找data目录下的results文件
    data_dir = Path("data")
    result_files = list(data_dir.glob("results*.csv"))
    
    if not result_files:
        print("✗ 结果文件不存在，请先采集数据")
        return
    
    # 使用最新的结果文件
    latest_file = max(result_files, key=lambda f: f.stat().st_mtime)
    print(f"使用数据文件: {latest_file.name}")
    
    if not check_dependencies():
        print("✗ 依赖检查失败")
        return
    
    try:
        os.chdir("scripts")
        subprocess.run([sys.executable, "analysis.py", str(latest_file)], check=True)
        os.chdir("..")
        print("✓ 分析完成")
    except Exception as e:
        print(f"✗ 分析失败: {e}")
        os.chdir("..")


def main():
    """主函数"""
    print_banner()
    
    while True:
        print_menu()
        
        try:
            choice = input("请选择 (0-5): ").strip()
            
            if choice == '0':
                print("退出程序")
                break
            elif choice == '1':
                run_preprocess()
            elif choice == '2':
                run_crawler()
            elif choice == '3':
                run_analysis()
            elif choice == '4':
                check_data_files()
            elif choice == '5':
                install_dependencies()
            else:
                print("无效选择，请重新输入")
                
        except KeyboardInterrupt:
            print("\n程序被中断")
            break
        except Exception as e:
            print(f"执行错误: {e}")
    
    print("程序结束")


if __name__ == "__main__":
    main()