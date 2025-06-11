#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据分析模块
功能：对采集到的波色数据进行统计分析
作者：AI Assistant
日期：2025年6月
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from collections import Counter
import json
from datetime import datetime


class DataAnalyzer:
    """数据分析器"""
    
    def __init__(self, input_file='../data/results.csv', output_dir='../data/'):
        """
        初始化数据分析器
        
        Args:
            input_file (str): 输入数据文件路径
            output_dir (str): 输出目录路径
        """
        self.input_file = input_file
        self.output_dir = output_dir
        self.df = None
        
        # 配置中文字体（解决matplotlib中文显示问题）
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
        plt.rcParams['axes.unicode_minus'] = False
    
    def load_data(self):
        """加载采集到的数据"""
        try:
            if not os.path.exists(self.input_file):
                print(f"数据文件不存在: {self.input_file}")
                return False
            
            self.df = pd.read_csv(self.input_file, encoding='utf-8-sig')
            print(f"成功加载数据，共 {len(self.df)} 条记录")
            print(f"数据列: {list(self.df.columns)}")
            return True
            
        except Exception as e:
            print(f"加载数据时出错: {e}")
            return False
    
    def data_overview(self):
        """数据概览"""
        if self.df is None:
            print("请先加载数据")
            return
        
        print("\n" + "="*50)
        print("数据概览")
        print("="*50)
        
        print(f"总数据条数: {len(self.df)}")
        print(f"数据列数: {len(self.df.columns)}")
        print(f"数据时间范围: {self.get_date_range()}")
        
        # 显示数据基本信息
        print("\n数据基本信息:")
        print(self.df.info())
        
        # 显示前几行数据
        print("\n前5行数据:")
        print(self.df.head())
        
        # 检查缺失值
        print("\n缺失值统计:")
        missing_count = self.df.isnull().sum()
        print(missing_count[missing_count > 0])
    
    def get_date_range(self):
        """获取数据的时间范围"""
        try:
            if '日期期数' in self.df.columns:
                dates = self.df['日期期数'].dropna()
                if len(dates) > 0:
                    return f"{dates.min()} ~ {dates.max()}"
            return "无法确定时间范围"
        except:
            return "无法确定时间范围"
    
    def analyze_colors(self):
        """分析波色数据"""
        if self.df is None or '波色' not in self.df.columns:
            print("没有波色数据可供分析")
            return None
        
        print("\n" + "="*50)
        print("波色统计分析")
        print("="*50)
        
        # 统计波色频次
        color_counts = self.df['波色'].value_counts()
        color_percentage = self.df['波色'].value_counts(normalize=True) * 100
        
        # 创建统计结果
        color_stats = pd.DataFrame({
            '出现次数': color_counts,
            '出现频率(%)': color_percentage.round(2)
        })
        
        print("波色组合出现统计:")
        print(color_stats)
        
        # 分析单个波色出现次数
        individual_color_stats = self.analyze_individual_colors()
        
        # 保存统计结果
        self.save_color_statistics(color_stats)
        
        return color_stats
    
    def analyze_individual_colors(self):
        """分析每个单独波色的总出现次数"""
        if self.df is None or '波色' not in self.df.columns:
            return None
        
        print("\n" + "="*50)
        print("单个波色总出现次数统计")
        print("="*50)
        
        # 初始化计数器
        individual_counts = {'红波': 0, '绿波': 0, '蓝波': 0}
        
        # 遍历所有波色数据
        for color_combination in self.df['波色'].dropna():
            # 检查每个单独波色是否在组合中出现
            if '红波' in str(color_combination):
                individual_counts['红波'] += 1
            if '绿波' in str(color_combination):
                individual_counts['绿波'] += 1
            if '蓝波' in str(color_combination):
                individual_counts['蓝波'] += 1
        
        # 计算总数和百分比
        total_occurrences = sum(individual_counts.values())
        individual_stats = pd.DataFrame({
            '总出现次数': list(individual_counts.values()),
            '占比(%)': [round(count/total_occurrences*100, 2) for count in individual_counts.values()]
        }, index=list(individual_counts.keys()))
        
        # 按出现次数排序
        individual_stats = individual_stats.sort_values('总出现次数', ascending=False)
        
        print("各波色总出现次数:")
        print(individual_stats)
        
        # 保存单个波色统计结果
        self.save_individual_color_statistics(individual_stats)
        
        return individual_stats
    
    def analyze_keywords(self):
        """分析关键词数据"""
        if self.df is None or '关键词' not in self.df.columns:
            print("没有关键词数据可供分析")
            return None
        
        print("\n" + "="*50)
        print("关键词统计分析")
        print("="*50)
        
        # 统计关键词频次
        keyword_counts = self.df['关键词'].value_counts()
        keyword_percentage = self.df['关键词'].value_counts(normalize=True) * 100
        
        # 创建统计结果
        keyword_stats = pd.DataFrame({
            '出现次数': keyword_counts,
            '出现频率(%)': keyword_percentage.round(2)
        })
        
        print("关键词出现统计:")
        print(keyword_stats)
        
        return keyword_stats
    
    def generate_color_chart(self, color_stats):
        """生成波色统计图表"""
        try:
            # 创建图表
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # 柱状图
            colors = ['red', 'green', 'blue']
            color_map = {'红波': 'red', '绿波': 'green', '蓝波': 'blue'}
            bar_colors = [color_map.get(name, 'gray') for name in color_stats.index]
            
            ax1.bar(color_stats.index, color_stats['出现次数'], color=bar_colors, alpha=0.7)
            ax1.set_title('波色出现次数统计', fontsize=14, fontweight='bold')
            ax1.set_xlabel('波色类型')
            ax1.set_ylabel('出现次数')
            ax1.grid(axis='y', alpha=0.3)
            
            # 添加数值标签
            for i, v in enumerate(color_stats['出现次数']):
                ax1.text(i, v + 0.5, str(v), ha='center', va='bottom')
            
            # 饼图
            ax2.pie(color_stats['出现次数'], labels=color_stats.index, autopct='%1.1f%%', 
                   colors=[color_map.get(name, 'gray') for name in color_stats.index],
                   startangle=90)
            ax2.set_title('波色分布比例', fontsize=14, fontweight='bold')
            
            plt.tight_layout()
            
            # 保存图表
            chart_path = os.path.join(self.output_dir, 'color_statistics.png')
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            print(f"波色统计图表已保存到: {chart_path}")
            
            plt.show()
            
        except Exception as e:
            print(f"生成图表时出错: {e}")
    
    def save_color_statistics(self, color_stats):
        """保存波色统计结果"""
        try:
            # 保存为CSV
            stats_path = os.path.join(self.output_dir, 'color_statistics.csv')
            color_stats.to_csv(stats_path, encoding='utf-8-sig')
            print(f"波色统计结果已保存到: {stats_path}")
            
            # 保存为JSON格式
            json_path = os.path.join(self.output_dir, 'color_statistics.json')
            stats_dict = {
                'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_records': len(self.df),
                'color_statistics': color_stats.to_dict('index')
            }
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(stats_dict, f, ensure_ascii=False, indent=2)
            print(f"波色统计结果已保存到: {json_path}")
            
        except Exception as e:
            print(f"保存统计结果时出错: {e}")
    
    def save_individual_color_statistics(self, individual_stats):
        """保存单个波色统计结果"""
        try:
            # 保存为CSV
            stats_path = os.path.join(self.output_dir, 'individual_color_statistics.csv')
            individual_stats.to_csv(stats_path, encoding='utf-8-sig')
            print(f"单个波色统计结果已保存到: {stats_path}")
            
            # 保存为JSON格式
            json_path = os.path.join(self.output_dir, 'individual_color_statistics.json')
            stats_dict = {
                'generation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_records': len(self.df),
                'individual_color_statistics': individual_stats.to_dict('index')
            }
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(stats_dict, f, ensure_ascii=False, indent=2)
            print(f"单个波色统计结果已保存到: {json_path}")
            
        except Exception as e:
            print(f"保存单个波色统计结果时出错: {e}")
    
    def analyze_trends(self):
        """分析趋势数据"""
        if self.df is None or '日期期数' not in self.df.columns:
            print("没有日期数据可供趋势分析")
            return
        
        print("\n" + "="*50)
        print("趋势分析")
        print("="*50)
        
        # 按日期分组统计
        try:
            # 尝试解析日期
            self.df['日期'] = pd.to_datetime(self.df['日期期数'], errors='coerce')
            
            if self.df['日期'].notna().sum() > 0:
                # 按日期统计波色
                daily_stats = self.df.groupby([self.df['日期'].dt.date, '波色']).size().unstack(fill_value=0)
                print("每日波色统计:")
                print(daily_stats.tail(10))  # 显示最近10天的数据
                
                # 生成趋势图
                self.generate_trend_chart(daily_stats)
            else:
                print("无法解析日期数据，跳过趋势分析")
                
        except Exception as e:
            print(f"趋势分析出错: {e}")
    
    def generate_trend_chart(self, daily_stats):
        """生成趋势图表"""
        try:
            plt.figure(figsize=(12, 6))
            
            # 绘制趋势线
            for color in daily_stats.columns:
                plt.plot(daily_stats.index, daily_stats[color], marker='o', label=color, linewidth=2)
            
            plt.title('波色出现趋势', fontsize=14, fontweight='bold')
            plt.xlabel('日期')
            plt.ylabel('出现次数')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            
            # 保存图表
            trend_path = os.path.join(self.output_dir, 'color_trends.png')
            plt.savefig(trend_path, dpi=300, bbox_inches='tight')
            print(f"趋势图表已保存到: {trend_path}")
            
            plt.show()
            
        except Exception as e:
            print(f"生成趋势图表时出错: {e}")
    
    def generate_report(self):
        """生成完整的分析报告"""
        if self.df is None:
            print("请先加载数据")
            return
        
        print("\n" + "="*60)
        print("生成完整分析报告")
        print("="*60)
        
        # 基础统计
        self.data_overview()
        
        # 波色分析
        color_stats = self.analyze_colors()
        if color_stats is not None:
            self.generate_color_chart(color_stats)
        
        # 关键词分析
        self.analyze_keywords()
        
        # 趋势分析
        self.analyze_trends()
        
        # 生成综合报告文本
        self.save_comprehensive_report()
        
        print("\n分析报告生成完成！")
    
    def save_comprehensive_report(self):
        """保存综合分析报告"""
        try:
            report_path = os.path.join(self.output_dir, 'analysis_report.txt')
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("数据采集与分析报告\n")
                f.write("="*50 + "\n")
                f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                if self.df is not None:
                    f.write(f"数据总量: {len(self.df)} 条\n")
                    f.write(f"数据列: {', '.join(self.df.columns)}\n")
                    f.write(f"时间范围: {self.get_date_range()}\n\n")
                    
                    # 波色统计
                    if '波色' in self.df.columns:
                        f.write("波色统计:\n")
                        color_counts = self.df['波色'].value_counts()
                        for color, count in color_counts.items():
                            percentage = (count / len(self.df)) * 100
                            f.write(f"  {color}: {count} 次 ({percentage:.1f}%)\n")
                        f.write("\n")
                    
                    # 关键词统计
                    if '关键词' in self.df.columns:
                        f.write("关键词统计:\n")
                        keyword_counts = self.df['关键词'].value_counts()
                        for keyword, count in keyword_counts.items():
                            f.write(f"  {keyword}: {count} 次\n")
                        f.write("\n")
            
            print(f"综合分析报告已保存到: {report_path}")
            
        except Exception as e:
            print(f"保存综合报告时出错: {e}")


def main():
    """主函数"""
    import sys
    
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 设置输入输出文件路径
    if len(sys.argv) > 1:
        # 从命令行参数获取文件路径
        input_file = sys.argv[1]
        if not os.path.isabs(input_file):
            input_file = os.path.join(current_dir, '..', input_file)
    else:
        # 默认文件路径
        input_file = os.path.join(current_dir, '..', 'data', 'results.csv')
    
    output_dir = os.path.join(current_dir, '..', 'data')
    
    print(f"分析文件: {input_file}")
    
    # 创建分析器实例并运行
    analyzer = DataAnalyzer(input_file, output_dir)
    
    if analyzer.load_data():
        analyzer.generate_report()
    else:
        print("无法加载数据，请先运行爬虫程序采集数据")


if __name__ == "__main__":
    main()