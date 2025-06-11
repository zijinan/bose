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
        cleaned_text = re.sub(r'[♠【】\[\]()（）<>《》""''