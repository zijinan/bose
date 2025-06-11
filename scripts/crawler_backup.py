#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高性能双波数据采集爬虫 - 精度优化版
功能：采集指定期数的双波关键词和波色数据
特性：保持高精度、性能优化、逻辑清晰
"""

import requests
import pandas as pd
import time
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import threading
import chardet
from tqdm import tqdm
from bs4 import BeautifulSoup
from functools import lru_cache


class OptimizedWaveScraper:
    """精度优化的双波数据采集器"""
    
    def __init__(self, target_period, input_file, output_file, batch_size=50, max_workers=8):
        self.input_file = input_file
        self.output_file = output_file
        self.target_period = target_period
        self.batch_size = batch_size
        self.max_workers = max_workers
        
        # 数据存储
        self.results = []
        self.stats = {'processed': 0, 'success': 0, 'failed': 0}
        self.results_lock = threading.Lock()
        
        # 会话池
        self.session_local = threading.local()
        
        # 预编译正则模式
        self._init_patterns()
    
    def _init_patterns(self):
        """初始化正则模式"""
        # 期数匹配 - 更精确的模式
        self.period_pattern = re.compile(r'(\d{1,3})期')
        
        # 扩展关键词列表以保持精度
        keywords = [
            '两波中特', '双波中特', '精准双波', '必中双波', '双波', '二波',
            '两波必中', '双波推荐', '两波推荐', '双波精选', '两波料',
            '双波料', '两波爆料', '双波爆料', '两波内幕', '双波内幕'
        ]
        keyword_regex = '|'.join(keywords)
        
        # 关键词提取 - 多种模式确保精度
        self.keyword_patterns = [
            re.compile(rf'([^\n]*?(?:{keyword_regex})[^\n]*?)', re.IGNORECASE),
            re.compile(rf'<[^>]*>([^<]*?(?:{keyword_regex})[^<]*?)<', re.IGNORECASE),
            re.compile(rf'(?:^|\s)([^\s]*?(?:{keyword_regex})[^\s]*?)(?:\s|$)', re.IGNORECASE)
        ]
        
        # 波色提取 - 更全面的模式
        self.color_patterns = [
            re.compile(r'([红绿蓝]波)', re.IGNORECASE),
            re.compile(r'(红|绿|蓝)(?=波)', re.IGNORECASE),
            re.compile(r'(?:推荐|精选|必中).*?([红绿蓝]波)', re.IGNORECASE)
        ]
        
        # 开奖结果 - 多种表达方式
        self.result_patterns = [
            re.compile(r'开[：:]?([^\n]*?[中错准])', re.IGNORECASE),
            re.compile(r'结果[：:]?([^\n]*?[中错准])', re.IGNORECASE),
            re.compile(r'([^\n]*?[中错准][^\n]*)', re.IGNORECASE)
        ]
        
        # 其他期数检测 - 防止串期
        self.other_period_pattern = re.compile(r'(\d{1,3})期')
    
    def get_session(self):
        """获取线程本地session - 优化网络性能"""
        if not hasattr(self.session_local, 'session'):
            session = requests.Session()
            # 优化session配置
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            # 连接池优化
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=20,
                pool_maxsize=20,
                max_retries=2
            )
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.session_local.session = session
        return self.session_local.session
    
    def load_links(self):
        """加载链接文件"""
        try:
            df = pd.read_csv(self.input_file)
            # 支持多种列名
            link_columns = ['link', 'url', 'links', 'urls']
            link_column = None
            
            for col in link_columns:
                if col in df.columns:
                    link_column = col
                    break
            
            if link_column is None:
                # 使用第一列
                links = df.iloc[:, 0].dropna().tolist()
            else:
                links = df[link_column].dropna().tolist()
            
            valid_links = [link for link in links if isinstance(link, str) and link.startswith(('http://', 'https://'))]
            print(f"加载了 {len(valid_links)} 个有效链接")
            return valid_links
        except Exception as e:
            print(f"加载链接文件错误: {e}")
            return []
    
    def get_page_content(self, url):
        """获取网页内容 - 优化版"""
        try:
            session = self.get_session()
            response = session.get(url, timeout=(5, 10))  # 连接超时5秒，读取超时10秒
            response.raise_for_status()
            
            # 检查响应大小
            if len(response.content) > 5 * 1024 * 1024:  # 5MB限制
                return None
            
            # 编码检测优化
            if response.encoding in ['ISO-8859-1', None]:
                detected = chardet.detect(response.content[:2048])  # 增加检测样本
                if detected.get('confidence', 0) > 0.7:
                    response.encoding = detected['encoding']
                else:
                    response.encoding = 'utf-8'  # 默认编码
            
            return response.text
        except requests.exceptions.Timeout:
            return None
        except requests.exceptions.RequestException:
            return None
        except Exception:
            return None
    
    @lru_cache(maxsize=1000)
    def extract_period_context(self, html_text, target_period):
        """提取期数上下文 - 缓存优化"""
        period_matches = list(self.period_pattern.finditer(html_text))
        target_matches = [match for match in period_matches if int(match.group(1)) == target_period]
        
        if not target_matches:
            return None
        
        contexts = []
        for match in target_matches:
            position = match.start()
            # 动态调整上下文范围
            start = max(0, position - 300)
            end = min(len(html_text), position + 300)
            context = html_text[start:end]
            
            # 检查串期风险
            other_periods = [int(p) for p in self.other_period_pattern.findall(context) if int(p) != target_period]
            if len(other_periods) <= 1:  # 允许少量其他期数
                contexts.append(context)
        
        return contexts if contexts else None
    
    def extract_data_with_beautifulsoup(self, html_text):
        """使用BeautifulSoup提取数据 - 保持精度"""
        try:
            # 使用lxml解析器提高性能
            soup = BeautifulSoup(html_text, 'lxml')
            
            # 查找包含目标期数的元素
            period_elements = soup.find_all(text=re.compile(f'{self.target_period}期'))
            
            results = []
            for element in period_elements:
                # 获取父元素的文本内容
                parent = element.parent
                if parent:
                    # 尝试获取表格行或段落
                    row = parent.find_parent(['tr', 'p', 'div', 'td'])
                    if row:
                        context = row.get_text(separator=' ', strip=True)
                        
                        # 检查串期
                        other_periods = [int(p) for p in self.other_period_pattern.findall(context) if int(p) != self.target_period]
                        if len(other_periods) <= 1:
                            results.append(context)
            
            return results
        except Exception:
            return []
    
    def extract_keywords(self, context):
        """提取关键词 - 多模式匹配"""
        keywords = set()
        
        for pattern in self.keyword_patterns:
            matches = pattern.findall(context)
            for match in matches:
                clean_match = re.sub(r'<[^>]*>', '', match).strip()
                clean_match = re.sub(r'[\s\u3000]+', ' ', clean_match)  # 清理空白字符
                if clean_match and len(clean_match) > 2:
                    keywords.add(clean_match)
        
        return list(keywords)
    
    def extract_colors(self, context):
        """提取波色 - 增强版多模式匹配"""
        colors = set()
        
        # 原有的波色模式匹配
        for pattern in self.color_patterns:
            matches = pattern.findall(context)
            for match in matches:
                if match.endswith('波'):
                    colors.add(match)
                else:
                    colors.add(match + '波')
        
        # 新增：单字符波色识别（处理红绿、♠绿红♠等格式）
        # 移除特殊符号后查找波色字符
        cleaned_text = re.sub(r'[♠【】\[\]()（）<>《》""''
    
    def extract_results(self, context):
        """提取开奖结果 - 多模式匹配"""
        results = set()
        
        for pattern in self.result_patterns:
            matches = pattern.findall(context)
            for match in matches:
                clean_result = re.sub(r'<[^>]*>', '', match).strip()
                if clean_result and len(clean_result) > 1:
                    results.add(clean_result)
        
        return list(results)
    
    def parse_single_period(self, html_text):
        """解析单个期数的数据 - 混合方法"""
        all_data = []
        
        # 方法1：BeautifulSoup解析（高精度）
        bs_contexts = self.extract_data_with_beautifulsoup(html_text)
        
        # 方法2：正则表达式解析（备用）
        regex_contexts = self.extract_period_context(html_text, self.target_period)
        
        # 合并上下文
        contexts = bs_contexts if bs_contexts else (regex_contexts or [])
        
        for context in contexts:
            keywords = self.extract_keywords(context)
            colors = self.extract_colors(context)
            results = self.extract_results(context)
            
            # 质量过滤
            if keywords:  # 必须有关键词
                data = {
                    '期数': f"{self.target_period}期",
                    '关键词': ', '.join(keywords),
                    '波色': ', '.join(colors) if colors else '',
                    '开奖结果': ', '.join(results) if results else ''
                }
                all_data.append(data)
        
        return all_data
    
    def parse_page_data(self, html_text, url):
        """解析页面数据"""
        try:
            data = self.parse_single_period(html_text)
            if data:
                print(f"✓ {url} - 找到 {len(data)} 条数据")
            return data
        except Exception as e:
            print(f"⚠️ 解析错误 {url}: {str(e)[:50]}...")
            return []
    
    def process_single_url(self, url):
        """处理单个URL"""
        try:
            html_text = self.get_page_content(url)
            if html_text:
                data = self.parse_page_data(html_text, url)
                return True, data
            return False, []
        except Exception:
            return False, []
    
    def save_results(self, incremental=False):
        """保存结果"""
        if self.results:
            try:
                df = pd.DataFrame(self.results)
                if incremental:
                    # 增量保存：追加到现有文件
                    if os.path.exists(self.output_file):
                        df.to_csv(self.output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
                    else:
                        df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
                    print(f"💾 增量保存 {len(self.results)} 条数据到: {self.output_file}")
                    # 清空已保存的数据
                    self.results.clear()
                else:
                    # 完整保存
                    df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
                    print(f"✅ 数据已保存到: {self.output_file}")
            except Exception as e:
                print(f"❌ 保存失败: {e}")
        else:
            if not incremental:
                print("⚠️ 没有数据需要保存")
    
    def run(self):
        """运行爬虫"""
        print(f"🚀 开始采集第 {self.target_period} 期数据")
        print(f"批处理大小: {self.batch_size}, 线程数: {self.max_workers}")
        
        links = self.load_links()
        if not links:
            print("❌ 没有可用的链接")
            return
        
        start_time = time.time()
        
        try:
            # 分批处理
            for i in range(0, len(links), self.batch_size):
                batch_links = links[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                total_batches = (len(links) - 1) // self.batch_size + 1
                
                print(f"\n📦 处理批次 {batch_num}/{total_batches}")
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_url = {executor.submit(self.process_single_url, url): url for url in batch_links}
                    
                    with tqdm(total=len(batch_links), desc="当前批次") as pbar:
                        for future in as_completed(future_to_url):
                            self.stats['processed'] += 1
                            
                            try:
                                success, data = future.result(timeout=15)
                                if success and data:
                                    with self.results_lock:
                                        self.results.extend(data)
                                    self.stats['success'] += 1
                                else:
                                    self.stats['failed'] += 1
                            except TimeoutError:
                                self.stats['failed'] += 1
                            except Exception:
                                self.stats['failed'] += 1
                            
                            pbar.update(1)
                            pbar.set_postfix({
                                '成功': self.stats['success'],
                                '数据': len(self.results)
                            })
                
                # 检查是否需要增量保存（每50条数据保存一次）
                if len(self.results) >= 50:
                    with self.results_lock:
                        self.save_results(incremental=True)
        
        except KeyboardInterrupt:
            print("\n⚠️ 程序被中断")
        finally:
            # 保存剩余数据（如果有）
            if self.results:
                self.save_results()
            
            # 显示统计
            elapsed = time.time() - start_time
            success_rate = self.stats['success'] / self.stats['processed'] * 100 if self.stats['processed'] > 0 else 0
            
            print(f"\n📊 采集完成:")
            print(f"处理链接: {self.stats['processed']} | 成功率: {success_rate:.1f}%")
            print(f"有效数据: {len(self.results)} 条 | 耗时: {elapsed:.1f} 秒")


def main():
    """主函数"""
    try:
        # 检查命令行参数
        if len(sys.argv) > 1:
            # 从命令行参数获取期数
            try:
                target_period = int(sys.argv[1])
            except ValueError:
                print("❌ 期数格式错误")
                return
        else:
            # 交互式输入期数
            target_period = input("请输入目标期数 (如: 160): ").strip()
            if not target_period:
                print("❌ 期数不能为空")
                return
            
            # 转换为整数
            try:
                target_period = int(target_period)
            except ValueError:
                print("❌ 期数格式错误")
                return
        
        # 使用通用的links.csv文件
        input_file = "d:/6zai/bose/data/links.csv"
        output_file = f"d:/6zai/bose/data/results_{target_period}.csv"
        
        scraper = OptimizedWaveScraper(
            target_period=target_period,
            input_file=input_file,
            output_file=output_file,
            batch_size=50,
            max_workers=8
        )
        
        scraper.run()
        
    except KeyboardInterrupt:
        print("\n⚠️ 程序被用户中断")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {e}")

if __name__ == "__main__":
    main()