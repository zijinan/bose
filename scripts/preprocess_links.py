#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
链接预处理脚本
功能：对links.csv进行去重、清理和优化
作者：AI Assistant
日期：2025年6月
"""

import pandas as pd
import re
from urllib.parse import urlparse, urlunparse
import os
from collections import defaultdict


class LinkPreprocessor:
    """链接预处理器"""
    
    def __init__(self, input_file='../data/links.csv', output_file='../data/links_processed.csv'):
        """
        初始化预处理器
        
        Args:
            input_file (str): 输入文件路径
            output_file (str): 输出文件路径
        """
        self.input_file = input_file
        self.output_file = output_file
        self.raw_links = []
        self.processed_links = []
    
    def load_links(self):
        """加载原始链接"""
        try:
            print("正在加载原始链接...")
            df = pd.read_csv(self.input_file)
            
            # 获取链接列
            if 'link' in df.columns:
                self.raw_links = df['link'].tolist()
            elif 'url' in df.columns:
                self.raw_links = df['url'].tolist()
            else:
                self.raw_links = df.iloc[:, 0].tolist()
            
            print(f"加载了 {len(self.raw_links)} 个原始链接")
            return True
            
        except Exception as e:
            print(f"加载链接失败: {e}")
            return False
    
    def normalize_url(self, url):
        """标准化URL"""
        try:
            # 移除空白字符
            url = url.strip()
            
            # 确保有协议
            if not url.startswith(('http://', 'https://')):
                url = 'http://' + url
            
            # 解析URL
            parsed = urlparse(url)
            
            # 标准化域名（转小写）
            domain = parsed.netloc.lower()
            
            # 移除默认端口
            if domain.endswith(':80') and parsed.scheme == 'http':
                domain = domain[:-3]
            elif domain.endswith(':443') and parsed.scheme == 'https':
                domain = domain[:-4]
            
            # 标准化路径
            path = parsed.path
            if not path:
                path = '/'
            
            # 重构URL
            normalized = urlunparse((
                parsed.scheme,
                domain,
                path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            
            return normalized
            
        except Exception:
            return None
    
    def deduplicate_links(self):
        """智能去重链接"""
        print("正在进行智能去重...")
        
        # 用于存储域名对应的链接
        domain_links = defaultdict(list)
        valid_links = []
        
        # 第一步：清理和验证链接
        for url in self.raw_links:
            if pd.isna(url) or not url:
                continue
                
            normalized = self.normalize_url(str(url))
            if normalized:
                try:
                    parsed = urlparse(normalized)
                    domain_key = parsed.netloc.lower()
                    
                    # 移除端口号作为域名key（用于去重）
                    if ':' in domain_key:
                        domain_key = domain_key.split(':')[0]
                    
                    domain_links[domain_key].append({
                        'url': normalized,
                        'scheme': parsed.scheme,
                        'original': url
                    })
                except Exception:
                    continue
        
        print(f"找到 {len(domain_links)} 个唯一域名")
        
        # 第二步：每个域名选择最佳链接
        for domain, urls in domain_links.items():
            if len(urls) == 1:
                valid_links.append(urls[0]['url'])
            else:
                # 多个链接时的选择策略
                best_url = self.select_best_url(urls)
                if best_url:
                    valid_links.append(best_url)
        
        self.processed_links = valid_links
        print(f"去重后保留 {len(self.processed_links)} 个链接")
        
        # 第三步：显示去重统计
        removed_count = len(self.raw_links) - len(self.processed_links)
        print(f"共移除 {removed_count} 个重复或无效链接")
    
    def select_best_url(self, urls):
        """选择最佳URL"""
        # 优先级：https > http
        # 其次：更短的路径 > 更长的路径
        # 最后：保持原有顺序
        
        https_urls = [u for u in urls if u['scheme'] == 'https']
        http_urls = [u for u in urls if u['scheme'] == 'http']
        
        # 优先选择https
        if https_urls:
            candidates = https_urls
        else:
            candidates = http_urls
        
        if not candidates:
            return None
        
        # 选择路径最短的（通常是首页或主要页面）
        best = min(candidates, key=lambda x: len(urlparse(x['url']).path))
        return best['url']
    
    def validate_links(self):
        """验证链接格式"""
        print("正在验证链接格式...")
        
        valid_links = []
        invalid_count = 0
        
        for url in self.processed_links:
            try:
                parsed = urlparse(url)
                
                # 基本验证
                if (parsed.scheme in ['http', 'https'] and 
                    parsed.netloc and 
                    '.' in parsed.netloc):
                    valid_links.append(url)
                else:
                    invalid_count += 1
                    
            except Exception:
                invalid_count += 1
        
        self.processed_links = valid_links
        print(f"验证完成，移除 {invalid_count} 个无效链接")
        print(f"最终保留 {len(self.processed_links)} 个有效链接")
    
    def save_processed_links(self):
        """保存处理后的链接"""
        try:
            # 创建输出目录
            output_dir = os.path.dirname(self.output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # 保存为CSV
            df = pd.DataFrame({'link': self.processed_links})
            df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
            
            print(f"处理后的链接已保存到: {self.output_file}")
            
            # 生成统计报告
            self.generate_report()
            
        except Exception as e:
            print(f"保存链接失败: {e}")
    
    def generate_report(self):
        """生成处理报告"""
        report_file = self.output_file.replace('.csv', '_report.txt')
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("链接预处理报告\n")
                f.write("="*50 + "\n")
                f.write(f"原始链接数量: {len(self.raw_links)}\n")
                f.write(f"处理后链接数量: {len(self.processed_links)}\n")
                f.write(f"去重数量: {len(self.raw_links) - len(self.processed_links)}\n")
                f.write(f"去重率: {((len(self.raw_links) - len(self.processed_links)) / len(self.raw_links) * 100):.1f}%\n")
                f.write("\n")
                
                # 协议统计
                https_count = sum(1 for url in self.processed_links if url.startswith('https://'))
                http_count = len(self.processed_links) - https_count
                
                f.write("协议分布:\n")
                f.write(f"HTTPS: {https_count} ({https_count/len(self.processed_links)*100:.1f}%)\n")
                f.write(f"HTTP: {http_count} ({http_count/len(self.processed_links)*100:.1f}%)\n")
                f.write("\n")
                
                # 域名统计
                domains = set()
                for url in self.processed_links:
                    try:
                        domain = urlparse(url).netloc.lower()
                        if ':' in domain:
                            domain = domain.split(':')[0]
                        domains.add(domain)
                    except:
                        pass
                
                f.write(f"唯一域名数量: {len(domains)}\n")
            
            print(f"处理报告已保存到: {report_file}")
            
        except Exception as e:
            print(f"生成报告失败: {e}")
    
    def run(self):
        """执行完整的预处理流程"""
        print("开始链接预处理...")
        
        if not self.load_links():
            return False
        
        if not self.raw_links:
            print("没有找到有效的链接")
            return False
        
        self.deduplicate_links()
        self.validate_links()
        self.save_processed_links()
        
        print("链接预处理完成！")
        return True


def main():
    """主函数"""
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 设置输入输出文件路径
    input_file = os.path.join(current_dir, '..', 'data', 'links.csv')
    output_file = os.path.join(current_dir, '..', 'data', 'links_processed.csv')
    
    # 创建预处理器并运行
    preprocessor = LinkPreprocessor(input_file, output_file)
    preprocessor.run()


if __name__ == "__main__":
    main() 