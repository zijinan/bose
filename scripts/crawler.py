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
    
    @lru_cache(maxsize=1000)
    def detect_encoding(self, raw_data):
        """检测编码 - 缓存优化"""
        try:
            result = chardet.detect(raw_data[:10000])  # 只检测前10KB
            return result.get('encoding', 'utf-8') or 'utf-8'
        except:
            return 'utf-8'
    
    def fetch_url_content(self, url, timeout=15):
        """获取URL内容 - 优化版"""
        session = self.get_session()
        
        try:
            response = session.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # 获取原始内容
            raw_content = response.content
            
            # 智能编码检测
            encoding = self.detect_encoding(raw_content)
            
            try:
                content = raw_content.decode(encoding)
            except UnicodeDecodeError:
                # 备用编码
                for backup_encoding in ['utf-8', 'gbk', 'gb2312', 'latin1']:
                    try:
                        content = raw_content.decode(backup_encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    content = raw_content.decode('utf-8', errors='ignore')
            
            return content
            
        except Exception as e:
            print(f"获取URL失败 {url}: {str(e)}")
            return None
    
    def extract_data_with_beautifulsoup(self, html_text):
        """使用BeautifulSoup提取数据 - 高精度方法"""
        try:
            soup = BeautifulSoup(html_text, 'html.parser')
            contexts = []
            
            # 移除脚本和样式标签
            for script in soup(["script", "style"]):
                script.decompose()
            
            # 获取所有文本内容
            text_content = soup.get_text()
            
            # 按行分割并过滤
            lines = [line.strip() for line in text_content.split('\n') if line.strip()]
            
            # 查找包含目标期数的行及其上下文
            target_lines = []
            for i, line in enumerate(lines):
                if f"{self.target_period}期" in line:
                    # 获取上下文（前后各3行）
                    start_idx = max(0, i - 3)
                    end_idx = min(len(lines), i + 4)
                    context = '\n'.join(lines[start_idx:end_idx])
                    target_lines.append(context)
            
            return target_lines
            
        except Exception as e:
            print(f"BeautifulSoup解析失败: {str(e)}")
            return []
    
    def extract_period_context(self, text, target_period):
        """提取指定期数的上下文 - 备用方法"""
        contexts = []
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            if f"{target_period}期" in line:
                # 获取上下文
                start_idx = max(0, i - 5)
                end_idx = min(len(lines), i + 6)
                context = '\n'.join(lines[start_idx:end_idx])
                contexts.append(context)
        
        return contexts
    
    def extract_keywords(self, context):
        """提取关键词 - 多模式匹配"""
        keywords = set()
        
        for pattern in self.keyword_patterns:
            matches = pattern.findall(context)
            for match in matches:
                clean_keyword = re.sub(r'<[^>]*>', '', match).strip()
                if clean_keyword and len(clean_keyword) > 2:
                    keywords.add(clean_keyword)
        
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
        
        # 新增：单字符波色识别（处理红绿、♠绿红♠、【蓝红波】、[ 蓝红 ]等格式）
        # 移除特殊符号后查找波色字符
        cleaned_text = re.sub(r'[♠【】\[\]()（）<>《》🐬波]', '', context)
        
        # 查找所有波色字符
        color_chars = re.findall(r'[红绿蓝]', cleaned_text)
        
        # 转换为完整波色名称并去重
        color_map = {'红': '红波', '绿': '绿波', '蓝': '蓝波'}
        seen_chars = set()
        
        for char in color_chars:
            if char in color_map and char not in seen_chars:
                colors.add(color_map[char])
                seen_chars.add(char)
        
        # 限制最多返回3个波色
        return list(colors)[:3]
    
    def extract_results(self, context):
        """提取开奖结果"""
        results = set()
        
        for pattern in self.result_patterns:
            matches = pattern.findall(context)
            for match in matches:
                clean_result = re.sub(r'<[^>]*>', '', match).strip()
                if clean_result and len(clean_result) <= 10:
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
            
            # 质量过滤 - 避免采集广告和无关内容
            if self.is_valid_content(context, keywords, colors):
                data = {
                    '期数': f"{self.target_period}期",
                    '关键词': ', '.join(keywords),
                    '波色': ', '.join(colors) if colors else '',
                    '开奖结果': ', '.join(results) if results else ''
                }
                all_data.append(data)
        
        return all_data
    
    def is_valid_content(self, context, keywords, colors):
        """判断内容是否有效，过滤广告和无关内容"""
        # 必须有关键词才算有效
        if not keywords:
            return False
        
        # 过滤明显的广告内容
        ad_keywords = [
            '广告', '推广', '联系', '微信', 'QQ', '电话', '手机', '网址', 'http',
            '加群', '客服', '咨询', '代理', '招商', '合作', '投资', '理财',
            '贷款', '借钱', '赚钱', '兼职', '招聘', '工作', '职位',
            '免费', '优惠', '折扣', '特价', '促销', '活动', '礼品',
            '注册', '开户', '充值', '提现', '转账', '支付', '银行',
            '点击', '下载', '安装', '扫码', '关注', '订阅'
        ]
        
        context_lower = context.lower()
        for ad_word in ad_keywords:
            if ad_word in context_lower:
                return False
        
        # 过滤纯标题内容（通常很短且没有实质内容）
        if len(context.strip()) < 20:
            return False
        
        # 过滤重复字符过多的内容
        if self.has_excessive_repetition(context):
            return False
        
        return True
    
    def has_excessive_repetition(self, text):
        """检查是否有过多重复字符"""
        if len(text) < 10:
            return False
        
        # 检查连续重复字符
        for i in range(len(text) - 5):
            char = text[i]
            if char != ' ' and text[i:i+6] == char * 6:
                return True
        
        return False
    
    def parse_page_data(self, html_text, url):
        """解析页面数据"""
        try:
            data = self.parse_single_period(html_text)
            if data:
                print(f"✓ {url} - 找到 {len(data)} 条有效数据")
            return data
        except Exception as e:
            print(f"⚠️ 解析错误 {url}: {str(e)[:50]}...")
            return []
    
    def process_single_url(self, url):
        """处理单个URL"""
        try:
            html_text = self.fetch_url_content(url)
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
    
    def process_urls_batch(self, urls):
        """批量处理URLs"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.process_single_url, url): url for url in urls}
            
            for future in as_completed(future_to_url, timeout=300):
                url = future_to_url[future]
                try:
                    success, data = future.result(timeout=30)
                    
                    with self.results_lock:
                        self.stats['processed'] += 1
                        if success:
                            self.stats['success'] += 1
                            if data:
                                self.results.extend(data)
                        else:
                            self.stats['failed'] += 1
                            
                except TimeoutError:
                    print(f"⏰ 超时: {url}")
                    with self.results_lock:
                        self.stats['failed'] += 1
                except Exception as e:
                    print(f"❌ 处理失败 {url}: {str(e)[:50]}...")
                    with self.results_lock:
                        self.stats['failed'] += 1
    
    def run(self):
        """运行爬虫"""
        try:
            # 读取链接
            df = pd.read_csv(self.input_file)
            urls = df['link'].tolist()
            total_urls = len(urls)
            
            print(f"📊 开始采集 {self.target_period}期 数据")
            print(f"📋 总链接数: {total_urls}")
            print(f"🔧 批次大小: {self.batch_size}")
            print(f"🧵 并发数: {self.max_workers}")
            
            # 分批处理
            for i in range(0, total_urls, self.batch_size):
                batch_urls = urls[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                total_batches = (total_urls + self.batch_size - 1) // self.batch_size
                
                print(f"\n🔄 处理批次 {batch_num}/{total_batches} ({len(batch_urls)} 个链接)")
                
                self.process_urls_batch(batch_urls)
                
                # 增量保存
                if self.results:
                    self.save_results(incremental=True)
                
                # 显示进度
                print(f"📈 进度: {self.stats['processed']}/{total_urls} | "
                      f"成功: {self.stats['success']} | 失败: {self.stats['failed']}")
                
                # 批次间休息
                if i + self.batch_size < total_urls:
                    time.sleep(2)
            
            # 最终保存
            self.save_results()
            
            print(f"\n🎉 采集完成!")
            print(f"📊 总处理: {self.stats['processed']}")
            print(f"✅ 成功: {self.stats['success']}")
            print(f"❌ 失败: {self.stats['failed']}")
            print(f"📁 结果文件: {self.output_file}")
            
        except Exception as e:
            print(f"❌ 运行失败: {e}")
            # 保存已采集的数据
            if self.results:
                self.save_results()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python crawler.py <期数>")
        print("示例: python crawler.py 160")
        sys.exit(1)
    
    try:
        target_period = int(sys.argv[1])
    except ValueError:
        print("❌ 期数必须是数字")
        sys.exit(1)
    
    # 文件路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(current_dir, '..', 'data', 'links_processed.csv')
    
    # 如果预处理文件不存在，使用原始文件
    if not os.path.exists(input_file):
        input_file = os.path.join(current_dir, '..', 'data', 'links.csv')
    
    output_file = os.path.join(current_dir, '..', 'data', f'results_{target_period}.csv')
    
    # 创建爬虫实例并运行
    scraper = OptimizedWaveScraper(
        target_period=target_period,
        input_file=input_file,
        output_file=output_file,
        batch_size=50,
        max_workers=8
    )
    
    scraper.run()