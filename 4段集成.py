# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 01:44:05 2025

@author: 吴志远
"""
#导入必备运行库
import requests
from bs4 import BeautifulSoup
import json
from threading import Thread
from queue import Queue
import time
from pymongo import MongoClient
import jieba.analyse
from urllib.parse import urljoin
import re
import random
import datetime
import logging
from openai import OpenAI 
from datetime import datetime
from py2neo import Graph, Node, Relationship
from typing import List, Dict
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kg_builder.log'),
        logging.StreamHandler()
    ]
)
#================= 第一阶段 =================
# ================= 配置部分 =================
MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "governance_data"
MAX_THREADS = 5  # 每个爬虫类型的线程数
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"

# ================= 存储初始化 =================
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
policy_col = db["policies"]
hot_col = db["hotspots"]

# ================= 爬虫核心类 =================
class GovPolicySpider(Thread):
    """政府政策采集器（多线程版）"""
    def __init__(self, task_queue):
        super().__init__()
        self.task_queue = task_queue
        self.base_url = "https://sousuo.www.gov.cn/search-gov/data?t=zhengcelibrary&q=&timetype=&mintime=&maxtime=&sort=score&sortType=1&searchfield=title&pcodeJiguan=&childtype=&subchildtype=&tsbq=&pubtimeyear=&puborg=&pcodeYear=&pcodeNum=&filetype=&p={}&n=5&inpro=&bmfl=&dup=&orpro=&type=gwyzcwjk"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.client = MongoClient(MONGO_URI)  # 从配置读取URI
        self.db = self.client[DATABASE_NAME]  # 从配置读取数据库名
    #政府机构数据存储，可能分类不准确，可能需要更改    
    def _normalize_org_name(self, name):
        """机构名称标准化（解决别名问题）"""
        alias_map = {
            "国家药监局": "国家药品监督管理局",
            "国家知识产权局": "国家知识产权局",
            "国家统计局": "国家统计局"
        }
        return alias_map.get(name, name.strip())

    def _determine_level(self, org_name):
        """核心逻辑：精准判定机构层级"""
        # 国家级机构匹配规则
        national_patterns = [
            r'^国务院', 
            r'总局$', 
            r'监管局$', 
            r'^国家(药品|知识产权|统计|市场监督管理)局',
            r'^中国[^省市区县]+(协会|联合会)$'
        ]
        for pattern in national_patterns:
            if re.search(pattern, org_name):
                return "国家级"
        
        # 省市级机构匹配规则
        if re.search(r'省|自治区|直辖市|特别行政区', org_name):
            return "省市级"
        
        return "其他"

    def _parse_gov_org(self, pub_org):
        """完整机构解析流程"""
        # 1. 名称标准化
        clean_name = self._normalize_org_name(pub_org)
        
        # 2. 判定层级
        org_level = self._determine_level(clean_name)
        
        # 3. 提取属地
        region = "全国"
        if org_level == "省市级":
            # 示例：从"广东省药品监督管理局"提取"广东"
            region_match = re.search(r'^(.*?(省|市|自治区))', clean_name)
            region = region_match.group(1) if region_match else clean_name[:2] + "省"
        
        # 4. 写入数据库
        org_data = {
            'name': clean_name,
            'level': org_level,
            'region': region
        }
        self.db.gov_orgs.update_one(
            {'name': org_data['name']},
            {'$setOnInsert': org_data},
            upsert=True
        )
        return org_data
    #法条细节结点构建

    def _extract_articles(self, policy_content):
        """改进后的法条提取方法"""
        articles = []
        article_pattern = re.compile(
            r'(第[一二三四五六七八九十百]+条)\s*' 
            r'([\s\S]+?)'
            r'(?=第[一二三四五六七八九十百]+条|第[一二三四五六七八九十百]+章|$)',
            re.DOTALL
        )
        
        for match in article_pattern.finditer(policy_content):
            number, content = match.groups()
            content = self.clean_article_content(content)
            
            if len(content) < 5:
                continue
                
            articles.append({
                'number': number,
                'content': content,
                'effect_date': self._parse_effect_date(policy_content),
                'type': '基础条款'
            })
        return articles

    def clean_article_content(self, text):
        text = re.sub(r'[\u3000\x00-\x1F\x7F]', ' ', text)
        text = re.sub(r'\n+', '\n', text)
        return text.strip()


    def _parse_effect_date(self, text):
        """解析法条生效日期（示例：自2023年5月1日起施行）"""
        date_patterns = [
            r'自(\d{4}年\d{1,2}月\d{1,2}日)起施行',
            r'施行日期：(\d{4}-\d{2}-\d{2})',
            r'effective from (\d{4}/\d{2}/\d{2})'  # 英文日期格式
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                raw_date = match.group(1)
                # 统一转换为ISO格式
                try:
                    date_obj = datetime.strptime(raw_date, '%Y年%m月%d日')
                    return date_obj.isoformat()
                except:
                    pass
        return None

   
    def run(self):
        while True:
            page = self.task_queue.get()
            if page is None:  # 终止信号
                break
            
            try:
                url = self.base_url.format(page)
                resp = self.session.get(url, timeout=10)
                data = resp.json()['searchVO']['catMap']
                for k, v in data.items():
                    for i in v['listVO']:
                        policy = {
                            'type': 'policy',
                            'title': i['title'],
                            'pub_date': i['pubtimeStr'],
                            'url': i['url'],
                            'source': '中国政府网',
                            'publisher': i.get('puborg','国务院'),
                            'childtype': i.get('childtype',''),
                            'pcode': i['pcode']
                        }
                        time.sleep(1)
                        self.process_detail(policy)
                        # print(policy)
                        # if not policy_col.find_one({'title': policy['title']}):
                        #     policy_col.insert_one(policy)

                # 间隔防止反爬
                time.sleep(10)
            except Exception as e:
                print(f"政策采集出错: {str(e)}")
            finally:
                self.task_queue.task_done()

    def process_detail(self, policy):
        """处理政策详情页"""
        # 【新增】政策标题标准化
        policy['title'] = self.normalize_title(policy['title'])

# 【新增】生成唯一政策编码（假设原有pcode可能缺失）
        if 'pcode' not in policy:
            policy['pcode'] = self.generate_pcode(policy['title'])
        try:
            resp = self.session.get(policy['url'], timeout=15)
            resp.encoding='utf-8'
            soup = BeautifulSoup(resp.text, 'html.parser')
            text=soup.find(name='div',id='UCAP-CONTENT').get_text()
            # 提取核心内容
            policy['content'] = text[:5000]  # 限制长度
            policy['keywords']=jieba.analyse.extract_tags(text, topK=5)
            pub_org = policy.get('publisher', '')
            if pub_org:
               self._parse_gov_org(pub_org)  # 触发机构解析
            if 'content' in policy:
                 articles = self._extract_articles(policy['content'])
            if articles:
                # 写入法条集合，关联政策pcode
                for art in articles:
                    art['policy_pcode'] = policy['pcode']  # 关键关联字段
                    art['policy_title'] = policy['title']  # 添加关联字段
                    self.db.articles.update_one(
                        {
                            'policy_pcode': art['policy_pcode'],
                            'number': art['number']
                        },
                        {'$set': art},
                        upsert=True
                    )
            # print(policy)
            # 保存到MongoDB
            if not policy_col.find_one({'title': policy['title']}):
                policy_col.insert_one(policy)
        except Exception as e:
            print(f"详情页处理失败: {policy['url']} - {str(e)}")
    def normalize_title(self, title):
        """统一政策标题格式（解决全角括号等问题）"""
        return (
              title.replace('〔', '(')
                   .replace('〕', ')')
                   .strip()
         )
    def generate_pcode(self, title):
        """生成政策编码（示例逻辑）"""
        year = re.search(r'\d{4}', title).group()
        return f"GOV-{year}-{hash(title[:10]) % 1000:03d}"

class WeiboHotSpider(Thread):
    """微博热点采集器（增强版）"""
    def __init__(self):
        super().__init__()
        self.api_url = "https://weibo.com/ajax/statuses/hot_band"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': USER_AGENT,
            'Referer': 'https://weibo.com/'
        })

    def run(self):
        while True:
            try:
                resp = self.session.get(self.api_url, timeout=10)
                data = resp.json()
                
                for item in data['data']['band_list']:
                    hotspot = {
                        'type': 'hotspot',
                        'rank': item['rank'],
                        'title': item['note'],
                        'hot_value': item.get('num', 0),
                        'link': f"https://s.weibo.com/weibo?q={item['word']}",
                        'timestamp': int(time.time())
                    }
                    # 去重后存储
                    if not hot_col.find_one({'title': hotspot['title'], 'timestamp': {'$gt': time.time()-86400}}):
                        hot_col.insert_one(hotspot)
                
                # 每5分钟更新一次
                time.sleep(300)
            except Exception as e:
                print(f"微博热点采集失败: {str(e)}")
                time.sleep(60)

# ================= 调度执行 =================
def start_policy_spiders():
    """启动政策采集集群"""
    task_queue = Queue()
    
    # 初始化任务队列（爬取前20页）
    for page in range(1, 21):
        task_queue.put(page)
    
    # 添加终止信号
    for _ in range(MAX_THREADS):
        task_queue.put(None)
    
    # 启动线程池
    threads = []
    for _ in range(MAX_THREADS):
        spider = GovPolicySpider(task_queue)
        spider.start()
        threads.append(spider)
    
    return threads

def start_hotspot_spiders():
    """启动热点采集"""
    spider = WeiboHotSpider()
    spider.start()
    return [spider]

if __name__ == "__main__":
    print("启动政府政策采集器...")
    policy_threads = start_policy_spiders()
    
    print("启动微博热点监测...")
    hotspot_threads = start_hotspot_spiders()
    
    # 等待政策采集完成
    for t in policy_threads:
        t.join()
    
    print("政策数据采集完成，热点监测持续运行中...")
#================= 第二阶段 =================
# ----------------------- 配置类 -----------------------
class GraphConfig:
    """知识图谱配置"""
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_AUTH = ("neo4j", "salt5619")
    MONGO_URI = "mongodb://localhost:27017/governance_data"
    
    # 节点类型定义
    NODE_TYPES = {
        "Policy": ["title", "pub_date", "publisher"],
        "Article": ["number", "content", "effect_date"],
        "GovOrg": ["name", "level", "region"]
    }

# ----------------------- 数据加载器 -----------------------
class PolicyLoader:
    """多源数据加载器"""
    def __init__(self):
        self.db = MongoClient(GraphConfig.MONGO_URI).get_database()  # 修正属性名
        
    def load_policies(self, batch_size=100):
        """从MongoDB加载原始政策数据"""
        return self.db.policies.find().batch_size(batch_size)
    
    def load_orgs(self):
        """加载政府机构数据"""
        return self.db.gov_orgs.find()
    
    def load_articles(self):
        """加载法条数据（添加字段过滤）"""
        # 使用投影排除 _id 字段
        raw_articles = self.db.articles.find({}, {'_id': 0})
        
        for art in raw_articles:
            # 转换可能的 ObjectId 字段为字符串
            if 'policy_id' in art:  # 假设存在关联字段
                art['policy_id'] = str(art['policy_id'])
            
            # 其他校验逻辑保持不变
            if not art.get('number') or not art.get('content'):
                logging.error(f"非法法条格式，已跳过: {art}")
                continue
            
            art['policy_pcode'] = art.get('policy_pcode') or self._guess_pcode(art)
            yield art

    def _guess_pcode(self, article):
        """根据法条内容推测政策编码（新增方法）"""
        # 示例：从法条内容中提取政策标题关键词
        match = re.search(r'《(.*?)》', article['content'])
        if match:
            title_keyword = match.group(1)
            policy = self.db.policies.find_one({"title": {"$regex": title_keyword}})
            return policy['pcode'] if policy else None
        return None
# ----------------------- 知识图谱构建器 -----------------------
class KnowledgeGraphBuilder:
    """Neo4j知识图谱构建引擎"""
    def build_full_graph(self):
        try:
            self._clear_existing_data()
            self._build_org_nodes()
            self._build_policy_nodes()
            self._build_article_nodes()  # 关键监控点
            self._build_relationships()
            self.show_graph_stats()
        except Exception as e:
            logging.critical(f"构建过程崩溃: {str(e)}", exc_info=True)
            self._send_alert_to_sentry(e)  # 集成错误监控平台
    def __init__(self):
        self.graph = Graph(
           GraphConfig.NEO4J_URI,
           auth=GraphConfig.NEO4J_AUTH,
           secure=False,  # 如果使用非加密连接
           name='neo4j'    # 指定数据库名称（Neo4j 4.0+）
        )
        self.loader = PolicyLoader()
        self._create_indexes()
      
        # 初始化索引
        self._create_indexes()
    
    def _create_indexes(self):
        """创建图数据库索引"""
        for label in GraphConfig.NODE_TYPES:
            self.graph.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.name)")
    def _clear_existing_data(self):
        """精准清除本系统管理的节点"""
        for label in GraphConfig.NODE_TYPES.keys():  # 使用实际定义的标签（如GovOrg）
            query = f"MATCH (n:{label}) DETACH DELETE n"
            self.graph.run(query)
            logging.warning(f"已清空 {label} 类型数据")
        
    
    def build_full_graph(self):
        """全量图谱构建"""
        self._clear_existing_data()
        # 核心构建流程
        self._build_org_nodes()      # 政府机构节点
        self._build_policy_nodes()    # 政策文件节点
        self._build_article_nodes()   # 法条节点
        self._build_relationships()  # 关系构建
        self.show_graph_stats()       # 新增：构建完成后显示统计 <-- 关键位置
        
    def _build_org_nodes(self):
        """构建政府机构节点"""
        orgs = self.loader.load_orgs()
        for org in orgs:
            node = Node("GovOrg",
                        name=org['name'],
                        level=org['level'],
                        region=org.get('region', '全国'))
            self.graph.create(node)
    
    def _build_policy_nodes(self):
        """构建政策文件节点"""
        for policy in self.loader.load_policies():
            node = Node("Policy",
                        title=policy['title'],
                        pub_date=policy['pub_date'], 
                        publisher=policy['publisher'])
            self.graph.create(node)
    def _create_article_node(self, article_data, policy_node):
        """创建法条节点并关联政策节点（核心逻辑）"""
        try:
            # 创建法条节点
            article_node = Node("Article", **article_data)
            self.graph.create(article_node)
            
            # 建立政策-法条关系
            rel = Relationship(policy_node, "CONTAINS", article_node)
            self.graph.create(rel)
            
            logging.info(f"法条节点创建成功: {article_data['number']}")
        except Exception as e:
            logging.error(f"法条节点创建失败: {str(e)}")
            raise
    def _build_article_nodes(self):
        """构建法条节点（增强版关联逻辑）"""
        for art in self.loader.load_articles():
            # 【修复】处理缺失policy_pcode的情况
            if 'policy_pcode' not in art:
                art['policy_pcode'] = self._retrospect_pcode(art.get('policy_title'))
            
            # 多级关联策略
            policy_node = (
                self.graph.nodes.match("Policy", pcode=art['policy_pcode']).first()
                or self.graph.nodes.match("Policy", title=art.get('policy_title')).first()
                or self._find_similar_policy(art.get('policy_title'))
            )
            
            if policy_node:
                self._create_article_node(art, policy_node)
            else:
                logging.warning(f"法条 {art['number']} 无法关联政策，已归档至待处理区")
                self._save_orphan_article(art)
                
    def _retrospect_pcode(self, title):
        """基于标题追溯政策编码（新增方法）"""
        policy = self.loader.db.policies.find_one(
            {"title": self.normalize_title(title)}, 
            {"pcode": 1}
        )
        return policy['pcode'] if policy else None
    def _find_similar_policy(self, title):
       """模糊匹配政策标题（新增方法）"""
       from difflib import get_close_matches
       all_titles = [p['title'] for p in self.loader.load_policies()]
       matches = get_close_matches(title, all_titles, n=1, cutoff=0.6)
       return self.graph.nodes.match("Policy", title=matches[0]).first() if matches else None

            

    
    def _build_relationships(self):
        """构建核心关系链"""
        tx = self.graph.begin()
        
        # 政策-机构关系
        policies = self.graph.nodes.match("Policy")
        for policy in policies:
            org_node = self.graph.nodes.match("GovOrg", name=policy['publisher']).first()
            if org_node:
                tx.create(Relationship(policy, "PUBLISHED_BY", org_node))
        
        # 法条引用关系
        articles = self.graph.nodes.match("Article")
        for article in articles:
            if '依据' in article['content']:
                ref_numbers = self._extract_references(article['content'])
                for ref in ref_numbers:
                    target = self.graph.nodes.match("Article", number=ref).first()
                    if target:
                        tx.create(Relationship(article, "REFERENCES", target))
        
        self.graph.commit(tx)
    
    def _extract_references(self, text: str) -> list:
        """提取法条引用（第X条）"""
        return re.findall(r'第([零一二三四五六七八九十百]+)条', text)
    def show_graph_stats(self):
       """显示精准节点统计（按目标类型过滤）"""
       stats = {}
       for label in GraphConfig.NODE_TYPES.keys():  # 从配置读取目标标签
           query = f"MATCH (n:{label}) RETURN count(n) as count"
           result = self.graph.run(query).data()
           stats[label] = result[0]['count'] if result else 0
       
       print("\n知识图谱构建成功，节点统计:")
       print("{:<10} | {:<10}".format('类型', '数量'))
       print("-" * 25)
       for label, count in stats.items():
           print("{:<10} | {:<10}".format(label, count))

# ----------------------- 辅助工具 -----------------------
class DataValidator:
    """数据质量验证器"""
    @staticmethod
    def validate_policy(policy):
        required_fields = ['title', 'pub_date', 'publisher']
        return all(field in policy for field in required_fields)
    # 在关联逻辑中添加多层校验


# ----------------------- 单元测试 -----------------------
if __name__ == "__main__":
    # 初始化构建器
    builder = KnowledgeGraphBuilder()
    
    # 执行全量构建
    try:
        builder.build_full_graph()
        print("知识图谱构建成功，节点统计:")
        print(builder.graph.run("MATCH (n) RETURN labels(n)[0] as type, count(*)"))
    except Exception as e:
        logging.error(f"构建失败: {str(e)}")
        
        
#================= 第三阶段 =================
class GovernanceAnalyzer: 
    def __init__(self): 
        # 初始化连接 
        self.mongo = MongoClient("mongodb://localhost:27017/governance_data ")# MongoDB地址
        self.neo4j = Graph("bolt://localhost:7687", auth=("neo4j", "salt5619"))  # Neo4j地址和认证
        # DeepSeek-R1配置（示例值，需替换为实际值）
        self.client = OpenAI(
            api_key="bce-v3/ALTAK-xlQTKLXlpIVatzq2E1Jf2/389d8bbbab545b42a792e9b3c87d07f171b7c0e2",  # 替换为真实API密钥
            base_url="https://qianfan.baidubce.com/v2"  # 确认正确的API地址
        )
    def full_analysis_pipeline(self, event_id):
        """全流程分析入口"""
        event = self._get_event_data(event_id)

        # 深度分析
        analysis_result = self._deepseek_analysis(event)

        # 生成治理建议（带动态温度）
        suggestion = self._generate_governance_suggestion(event, analysis_result)

        # 存储结果
        self._save_results(event_id, analysis_result, suggestion)
        return suggestion

    def _deepseek_analysis(self, event):
        """调用DeepSeek-R1进行深度分析（兼容新版SDK）"""
        messages = [
            {"role": "system", "content": "你是一个舆情分析专家"},
            {"role": "user", "content": f"""
            请分析以下舆情事件：
            标题：{event['title']}
            内容：{event['content']}
            要求：
            1. 识别事实性错误
            2. 检测逻辑矛盾
            3. 评估政策敏感性
            输出格式：JSON"""}
        ]

        try:
            completion = self.client.chat.completions.create(
                model="deepseek-r1",
                messages=messages,
                temperature=0.4,
                max_tokens=800
            )
            return completion.choices[0].message.content
        except Exception as e:
            self._log_error(event['_id'], str(e))
            return {"error": str(e)}

    def _generate_governance_suggestion(self, event, analysis):
        """生成治理建议（带动态温度）"""
        policy_level = self._get_policy_level(event['_id'])
        temperature = max(0.1, 0.7 - policy_level * 0.1)  # 动态温度计算

        messages = [
            {"role": "system", "content": "你是一个政府舆情治理专家"},
            {"role": "user", "content": f"""
            根据以下分析结果生成治理建议：
            事件可信度：{event['cred_score']}
            政策敏感度：{policy_level}
            分析结果：{analysis}
            要求：
            - 包含技术处置和舆情引导措施
            - 使用专业术语
            - 符合政策规范"""}
        ]

        try:
            completion = self.client.chat.completions.create(
                model="deepseek-r1",
                messages=messages,
                temperature=temperature,
                top_p=0.85,
                max_tokens=600
            )
            return completion.choices[0].message.content
        except Exception as e:
            self._log_error(event['_id'], str(e))
            return "生成失败，请检查API连接"
        
#================= 第四阶段 =================
# ----------------------- 配置类 -----------------------
class Config:
    # 数据库配置
    MONGO_URI = "mongodb://localhost:27017/governance_data"
    NEO4J_URI = "bolt://localhost:7687"
    NEO4J_AUTH = ("neo4j", "salt5619")
    
    # 舆情采集配置
    CRAWL_INTERVAL = 300  # 5分钟
    HOTWORD_THRESHOLD = 1000  # 热搜词阈值
    
    # 模型参数
    MAX_RETRIES = 3
    PRS_THRESHOLD = 0.8

# ----------------------- 数据层 -----------------------
class PolicyLoader:
    """政策数据加载器"""
    def __init__(self):
        self.client = MongoClient(Config.MONGO_URI)
        self.db = self.client.get_database()
        
    def get_policies_by_keywords(self, keywords: List[str]) -> List[Dict]:
        """根据关键词检索相关政策"""
        query = {"$or": [{"title": {"$regex": kw}} for kw in keywords]}
        return list(self.db.policies.find(query))
    
    def save_correction_log(self, log_data):  # 参数名修正为 log_data
        """存储修正日志"""
        self.db.correction_logs.insert_one(log_data)  # 变量名统一


class Neo4jConnector:
    """知识图谱连接器"""
    def __init__(self):
        self.graph = Graph(Config.NEO4J_URI, auth=Config.NEO4J_AUTH)
        
    def get_related_policies(self, entities: List[str]) -> List[Dict]:
        """获取实体关联政策"""
        query = (
            "MATCH (p:Policy)-[r]->(a:Article) "
            "WHERE any(entity IN $entities WHERE a.content CONTAINS entity) "
            "RETURN p.title as title, a.number as article, a.content as content"
        )
        return self.graph.run(query, entities=entities).data()

# ----------------------- 核心逻辑 -----------------------
class MisinformationDetector:
    """误导信息检测器"""
    def __init__(self):
        self.neo4j = Neo4jConnector()
        self.loader = PolicyLoader()
        
    def detect_misinformation(self, text: str) -> Dict:
        """执行误导信息检测"""
        entities = self._extract_entities(text)
        policies = self.neo4j.get_related_policies(entities)
        
        value_violations = self._check_value_violations(text, policies)
        time_violations = self._check_time_violations(policies)
        
        return {
            "entities": entities,
            "related_policies": [p["title"] for p in policies],
            "value_violations": value_violations,
            "time_violations": time_violations,
            "violations": value_violations + time_violations  # 明确合并结果
        }
    
    def _extract_entities(self, text: str) -> List[str]:
        return re.findall(r'《([^》]+)》|第[零一二三四五六七八九十]+条', text)
    
    def _check_value_violations(self, text: str, policies: List[Dict]) -> List[Dict]:
        violations = []
        for policy in policies:
            numbers = re.findall(r'\d+\.?\d*%?', policy['content'])
            for num in numbers:
                if num in text:
                    violations.append({
                        "type": "数值冲突",
                        "policy": policy["title"],
                        "conflict_value": num
                    })
        return violations
    
    def _check_time_violations(self, policies: List[Dict]) -> List[Dict]:
        violations = []
        for policy in policies:
            if "expiry_date" in policy:
                expiry = datetime.strptime(policy["expiry_date"], "%Y-%m-%d")
                if datetime.now() > expiry:
                    violations.append({
                        "type": "时效性冲突",
                        "policy": policy["title"],
                        "expired_date": policy["expiry_date"]
                    })
        return violations

      
    def _check_value_violations(self, text: str, policies: List[Dict]) -> List[Dict]:
        """数值矛盾检测"""
        violations = []
        for policy in policies:
            # 提取政策中的数值约束
            numbers = re.findall(r'\d+\.?\d*%?', policy['content'])
            for num in numbers:
                if num in text:
                    # 验证数值一致性
                    policy_value = float(num.replace('%', ''))
                    text_value = self._extract_number_from_text(text, num)
                    if text_value and abs(policy_value - text_value) > policy_value * 0.1:  # 允许10%误差
                        violations.append({
                            "type": "value_conflict",
                            "policy": policy['title'],
                            "article": policy['article'],
                            "expected": num,
                            "actual": text_value
                        })
        return violations
    
    def _extract_number_from_text(self, text: str, pattern: str) -> float:
        """从文本中提取特定格式的数值"""
        match = re.search(rf'(\d+\.?\d*){pattern[-1]}', text)  # 匹配相同单位
        return float(match.group(1)) if match else None

class HallucinationCorrector:
    """幻觉修正引擎"""
    def __init__(self):
        self.neo4j = Neo4jConnector()
        self.loader = PolicyLoader()
        
    def generate_correction(self, text: str, violations: List[Dict]) -> str:
        """生成修正内容"""
        corrections = []
        for violation in violations:
            if violation['type'] == 'value_conflict':
                correction = self._correct_value_violation(violation)
                corrections.append(correction)
        
        # 构建修正后文本
        corrected_text = text
        for corr in corrections:
            corrected_text += f"\n【修正】{corr['reason']} 正确信息：{corr['correct_info']}"
            
        return corrected_text
    
    def _correct_value_violation(self, violation: Dict) -> Dict:
        """数值矛盾修正"""
        policy = self.neo4j.get_policy_details(
            violation['policy'], 
            violation['article']
        )
        return {
            "type": "value_correction",
            "original": violation['actual'],
            "corrected": violation['expected'],
            "reason": f"根据《{violation['policy']}》第{violation['article']}条",
            "correct_info": policy['content']
        }

# ----------------------- 工作流控制器 -----------------------
class GovernanceWorkflow:
    """治理工作流控制器"""
    def __init__(self):
        self.detector = MisinformationDetector()
        self.corrector = HallucinationCorrector()
        self.loader = PolicyLoader()
        
    def process_event(self, event: Dict):
        """处理舆情事件全流程"""
        # 检测阶段
        detection_result = self.detector.detect_misinformation(event['content'])
        
        # 修正阶段
        if detection_result['violations']:
            corrected_text = self.corrector.generate_correction(
                event['content'], 
                detection_result['violations']
            )
            # 记录日志
            log_entry = {
                "event_id": event['id'],
                "original_text": event['content'],
                "corrected_text": corrected_text,
                "detection_result": detection_result,
                "timestamp": datetime.now()
            }
            self.loader.save_correction_log(log_entry)
            return corrected_text
        return event['content']

# ----------------------- 示例用法 -----------------------
if __name__ == "__main__":
    # 初始化工作流
    workflow = GovernanceWorkflow()
    
    # 模拟舆情事件
    test_event = {
        "id": "20240315_001",
        "content": "根据《过期网络安全法》(2020年废止)第38条要求，企业数据留存应不少于180天，但当前要求是60天",
        "source": "微博",
        "timestamp": "2024-03-15T14:30:00"
    }
    
    # 执行治理流程
    result = workflow.process_event(test_event)
    
    print("原始内容:", test_event['content'])
    print("修正结果:", result)        