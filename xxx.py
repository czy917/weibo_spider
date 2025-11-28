import requests
import json
import time
import random
from typing import List, Dict, Optional, Tuple
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from requests.exceptions import ProxyError, ConnectionError, Timeout, RequestException
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from queue import Queue
import threading

# 禁用SSL警告
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


class ProxyPool:
    """IP代理池管理类"""

    def __init__(self, proxy_list: List[str] = None):
        self.proxies = proxy_list or []
        self.lock = threading.Lock()
        self.current_index = 0
        self.err_proxy = []

        # 过滤无效代理（移除None和空字符串）
        self.proxies = [p for p in self.proxies if p and isinstance(p, str) and p.strip()]

        # 如果没有提供代理列表，尝试从文件加载
        if not self.proxies:
            self.load_proxies_from_file("proxies.txt")

    def load_proxies_from_file(self, filename: str = "proxies.txt"):
        """从文件加载代理列表（每行一个代理）"""
        try:
            with open(filename, "r", encoding="utf-8") as f:
                self.proxies = [line.strip() for line in f if line.strip() and line.strip().lower() != "none"]
            logging.info(f"从文件加载了 {len(self.proxies)} 个代理")
        except FileNotFoundError:
            logging.warning(f"代理文件 {filename} 不存在，使用空代理池")

    def get_proxy(self) -> Optional[str]:
        """获取一个代理（轮询）"""
        if not self.proxies:
            return None

        with self.lock:
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)

        return proxy

    def get_random_proxy(self) -> Optional[str]:
        """随机获取一个代理"""
        if not self.proxies:
            return None

        with self.lock:
            return random.choice(self.proxies)

    def mark_proxy_bad(self, proxy: str):
        """标记坏代理并暂时移除"""
        if proxy and proxy in self.proxies:
            with self.lock:
                self.proxies.remove(proxy)
                # 将坏代理添加到错误队列
                self.err_proxy.append(proxy)
            logging.warning(f"标记代理 {proxy} 为坏代理，剩余代理数：{len(self.proxies)}")


class WeiboSpider:
    def __init__(self, uid: str, cookie_str: str, proxy_pool: ProxyPool = None,
                 mongo_uri: str = "mongodb://localhost:27017/", mongo_db: str = "weibo_db",
                 max_pages: int = 5, retry_times: int = 3, max_workers: int = 5):
        """
        初始化微博爬虫（多线程版本）
        :param uid: 目标用户的 UID
        :param cookie_str: 浏览器复制的 Cookie 字符串
        :param proxy_pool: 代理池对象
        :param mongo_uri: MongoDB 连接 URI
        :param mongo_db: MongoDB 数据库名称
        :param max_pages: 初始最大爬取页数（会根据接口返回的total动态调整）
        :param retry_times: 请求重试次数
        :param max_workers: 最大线程数
        """
        self.uid = uid
        self.proxy_pool = proxy_pool or ProxyPool()
        self.initial_max_pages = max_pages
        self.max_pages = max_pages  # 实际最大页数，会动态更新
        self.retry_times = retry_times
        self.max_workers = max_workers

        # 添加线程锁用于保护max_pages的更新
        self.page_lock = threading.Lock()

        # MongoDB 配置
        self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        try:
            self.mongo_client.admin.command('ping')
            logging.info("MongoDB连接成功")
        except PyMongoError as e:
            logging.error(f"MongoDB连接失败: {e}")
            raise

        self.db = self.mongo_client[mongo_db]
        self.collection = self.db[f"weibo_{uid}"]
        self.collection.create_index("mblogid", unique=True)

        # 请求头模板（精简不必要的字段，避免头信息过长）
        self.headers_template = {
            "User-Agent": self._get_random_user_agent(),
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": f"https://weibo.com/u/{uid}",
            "X-Requested-With": "XMLHttpRequest",
            "X-XSRF-TOKEN": "",  # 后续动态填充
            "Connection": "keep-alive"
        }

        # 解析Cookie（清理无效字符）
        self.cookies = self._parse_cookies(cookie_str)
        self.xsrf_token = self.cookies.get("XSRF-TOKEN", "").strip()
        if self.xsrf_token:
            self.headers_template["X-XSRF-TOKEN"] = self.xsrf_token
        else:
            logging.warning("未找到XSRF-TOKEN，可能影响请求")

        # 线程安全的计数器
        self.lock = threading.Lock()
        self.success_count = 0
        self.failed_count = 0

        # 记录已处理的页面
        self.processed_pages = set()
        self.page_set_lock = threading.Lock()

    def _parse_cookies(self, cookie_str: str) -> Dict:
        """解析Cookie字符串（清理特殊字符）"""
        cookies = {}
        if not cookie_str:
            return cookies

        # 清理Cookie字符串中的换行符和多余空格
        cookie_str = cookie_str.replace("\n", "").replace("\r", "").strip()

        for item in cookie_str.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                # 清理键值对中的特殊字符
                key = key.strip().replace("\x00", "").replace("\t", "")
                value = value.strip().replace("\x00", "").replace("\t", "")
                if key:
                    cookies[key] = value
        return cookies

    def _get_random_user_agent(self) -> str:
        """获取随机User-Agent"""
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/142.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
        ]
        return random.choice(user_agents)

    def _create_session(self) -> requests.Session:
        """创建新的Session（每个线程一个）"""
        session = requests.Session()
        session.cookies.update(self.cookies)

        # 随机User-Agent
        headers = self.headers_template.copy()
        headers["User-Agent"] = self._get_random_user_agent()
        session.headers.update(headers)

        # 禁用URL自动编码（避免特殊字符导致URL过长）
        session.params = {}

        return session

    def _request_with_retry(self, url: str, params: dict = None) -> Optional[dict]:
        """带重试和代理切换的请求方法（解决URL过长问题）"""
        session = self._create_session()

        for retry in range(self.retry_times):
            proxy = self.proxy_pool.get_proxy()
            proxies = None

            if proxy:
                # 验证代理格式
                if not proxy.startswith(("http://", "https://")):
                    proxy = f"http://{proxy}"
                proxies = {
                    "http": proxy,
                    "https": proxy
                }

            try:
                # 精简参数（只保留必要参数）
                clean_params = {}
                if params:
                    for k, v in params.items():
                        if v is not None and str(v).strip():
                            clean_params[k] = str(v).strip()

                # 发送请求（使用params参数，避免手动拼接URL）
                response = session.get(
                    url=url,
                    params=clean_params,  # 让requests自动处理参数编码
                    proxies=proxies,
                    timeout=15,
                    verify=False,
                    allow_redirects=True
                )

                # 处理重定向（如果URL过长，服务器可能返回重定向）
                if response.status_code in [301, 302, 307]:
                    redirect_url = response.headers.get("Location")
                    if redirect_url:
                        logging.info(f"重定向到: {redirect_url[:100]}...")
                        response = session.get(
                            url=redirect_url,
                            proxies=proxies,
                            timeout=15,
                            verify=False
                        )

                response.raise_for_status()

                # 随机延迟，模拟人类行为
                time.sleep(random.uniform(0.5, 1.0))

                return response.json()

            except (ProxyError, ConnectionError):
                logging.error(f"代理 {proxy} 连接失败（重试{retry + 1}/{self.retry_times}）")
                if proxy:
                    self.proxy_pool.mark_proxy_bad(proxy)
                time.sleep(random.uniform(1.5 ** retry, 1.5 ** retry + 1))

            except Timeout:
                logging.error(f"请求超时（重试{retry + 1}/{self.retry_times}）")
                time.sleep(random.uniform(1.5 ** retry, 1.5 ** retry + 1))

            except RequestException as e:
                status_code = getattr(e.response, 'status_code', 'unknown')
                logging.error(f"请求异常 {status_code}: {str(e)}（重试{retry + 1}/{self.retry_times}）")

                # 针对414错误的特殊处理
                if status_code == 414:
                    logging.error(f"URL过长错误！当前URL: {url}")
                    if params:
                        logging.error(f"请求参数: {json.dumps(clean_params, ensure_ascii=False)[:200]}...")

                # 403/429表示被反爬，切换代理
                if status_code in [403, 429, 414] and proxy:
                    self.proxy_pool.mark_proxy_bad(proxy)

                time.sleep(random.uniform(1.5 ** retry, 1.5 ** retry + 1))

            except Exception as e:
                logging.error(f"解析响应失败: {str(e)}")
                return None

        logging.error(f"请求失败，已重试{self.retry_times}次")
        return None

    def get_page_data(self, page: int) -> Tuple[List[Dict], bool, int]:
        """
        获取单页微博数据
        :return: (数据列表, 是否有下一页, 总页数)
        """
        logging.info(f"线程 {threading.current_thread().name} 正在爬取第 {page} 页")

        url = f"https://weibo.com/ajax/statuses/mymblog"
        params = {
            "uid": self.uid,
            "page": page,
            "feature": 0,
            "count": 20  # 明确指定每页20条，避免默认值导致的参数问题
        }

        data = self._request_with_retry(url, params)
        time.sleep(1)
        total_pages = self.max_pages  # 默认使用当前max_pages

        if not data:
            return [], False, total_pages

        try:
            # 从返回数据中获取总条数并计算总页数
            total_count = data.get("data", {}).get("total", 0)
            if total_count > 0:
                total_pages = (total_count + 19) // 20  # 每页20条，向上取整
                with self.page_lock:
                    if total_pages != self.max_pages:
                        old_max = self.max_pages
                        self.max_pages = total_pages
                        logging.info(f"根据接口返回的total={total_count}，更新总页数：{old_max} → {total_pages}")

            mblog_data_list = []
            for item in data.get("data", {}).get("list", []):
                mblog_data = {
                    "mblogid": item.get("mblogid"),
                    "text": item.get("text", ""),
                    "text_raw": item.get("text_raw", ""),
                    "created_at": item.get("created_at", ""),
                    "title": item.get("title", ""),
                    "has_expand": ">展开<" in item.get("text", ""),
                    "page": page
                }
                if mblog_data["mblogid"]:
                    mblog_data_list.append(mblog_data)

            has_next = len(data.get("data", {}).get("list", [])) >= 20  # 每页20条
            logging.info(f"线程 {threading.current_thread().name} 第 {page} 页获取到 {len(mblog_data_list)} 条数据")

            # 标记页面已处理
            with self.page_set_lock:
                self.processed_pages.add(page)

            return mblog_data_list, has_next, total_pages

        except Exception as e:
            logging.error(f"处理第 {page} 页数据失败：{str(e)}")
            return [], False, total_pages

    def process_single_weibo(self, mblog_data: Dict) -> Dict:
        """处理单条微博（线程安全）"""
        result = {
            "mblogid": mblog_data["mblogid"],
            "uid": self.uid,
            "original_text": mblog_data["text"],
            "text_raw": mblog_data["text_raw"],
            "created_at": mblog_data["created_at"],
            "title": mblog_data["title"],
            "has_expand": mblog_data["has_expand"],
            "page": mblog_data["page"],
            "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "thread_name": threading.current_thread().name,
            "is_success": False
        }

        try:
            if mblog_data["has_expand"]:
                # 获取长文本
                url = f"https://weibo.com/ajax/statuses/longtext"
                params = {"id": mblog_data["mblogid"]}
                longtext_data = self._request_with_retry(url, params)

                if longtext_data:
                    result["full_text"] = longtext_data.get("data", {}).get("longTextContent", "")
                    result["is_success"] = True
                    result["is_longtext"] = True
                else:
                    result["full_text"] = mblog_data["text_raw"]
                    result["error_msg"] = "长文本获取失败，使用备用文本"
            else:
                result["full_text"] = mblog_data["text_raw"]
                result["is_success"] = True
                result["is_longtext"] = False

            with self.lock:
                if result["is_success"]:
                    self.success_count += 1
                else:
                    self.failed_count += 1

            return result

        except Exception as e:
            result["error_msg"] = f"处理失败: {str(e)}"
            logging.error(f"处理微博 {mblog_data['mblogid']} 失败: {str(e)}")

            with self.lock:
                self.failed_count += 1

            return result

    def save_to_mongodb_batch(self, results: List[Dict]):
        """批量保存到MongoDB"""
        if not results:
            return

        try:
            # 使用bulk_write提高效率
            from pymongo import UpdateOne

            operations = []
            for result in results:
                if result.get("mblogid"):
                    operations.append(
                        UpdateOne(
                            {"mblogid": result["mblogid"]},
                            {"$set": result},
                            upsert=True
                        )
                    )

            if operations:
                result = self.collection.bulk_write(operations)
                logging.info(f"批量保存：插入 {result.upserted_count} 条，更新 {result.modified_count} 条")

        except PyMongoError as e:
            logging.error(f"批量保存MongoDB失败：{str(e)}")
            # 降级为逐条保存
            for result in results:
                self.save_to_mongodb_single(result)

    def save_to_mongodb_single(self, result: Dict):
        """单条保存到MongoDB"""
        try:
            self.collection.update_one(
                {"mblogid": result["mblogid"]},
                {"$set": result},
                upsert=True
            )
        except PyMongoError as e:
            logging.error(f"保存 {result['mblogid']} 失败：{str(e)}")

    def run(self):
        """启动多线程爬虫"""
        start_time = time.time()
        logging.info(f"开始多线程爬取用户 {self.uid}，初始页数：{self.initial_max_pages}，线程数：{self.max_workers}")

        all_weibo_data = []

        # 第一步：动态获取所有页面的微博ID
        page_executor = ThreadPoolExecutor(max_workers=min(3, self.max_workers), thread_name_prefix="Page")
        page_futures = {}

        # 先提交初始页数的任务（限制初始页数，避免一次性提交过多）
        initial_pages = min(self.initial_max_pages, 10)  # 初始最多提交10页
        for page in range(1, initial_pages + 1):
            future = page_executor.submit(self.get_page_data, page)
            page_futures[page] = future

        # 处理结果并动态添加新任务
        while page_futures:
            # 等待任一任务完成
            done = list(as_completed(page_futures.values()))

            for future in done:
                # 找到对应的页码
                page = None
                for p, f in page_futures.items():
                    if f == future:
                        page = p
                        break

                if page is None:
                    continue

                try:
                    mblog_data_list, has_next, total_pages = future.result()
                    all_weibo_data.extend(mblog_data_list)

                    # 如果总页数大于当前已提交的页数，继续添加任务
                    with self.page_lock:
                        current_max = self.max_pages

                    # 只添加未处理且未提交的页面
                    for p in range(len(page_futures) + 1, min(current_max + 1, 1000)):  # 限制最大页数为1000，避免无限循环
                        if p not in page_futures and p not in self.processed_pages:
                            new_future = page_executor.submit(self.get_page_data, p)
                            page_futures[p] = new_future
                            logging.info(f"动态添加第 {p} / {current_max} 页爬取任务")
                            # 控制并发数，避免一次性添加过多任务
                            if len(page_futures) >= self.max_workers * 2:
                                break

                except Exception as e:
                    logging.error(f"处理第 {page} 页失败：{str(e)}")

                # 移除已完成的任务
                del page_futures[page]

        page_executor.shutdown()
        logging.info(f"共获取到 {len(all_weibo_data)} 条微博数据待处理")

        # 第二步：多线程处理每条微博
        processed_results = []
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="Weibo") as weibo_executor:
            # 提交微博处理任务
            futures = [
                weibo_executor.submit(self.process_single_weibo, mblog_data)
                for mblog_data in all_weibo_data
            ]

            # 收集结果
            for future in as_completed(futures):
                try:
                    result = future.result()
                    processed_results.append(result)

                    # 每20条批量保存一次
                    if len(processed_results) >= 20:
                        self.save_to_mongodb_batch(processed_results)
                        processed_results = []

                except Exception as e:
                    logging.error(f"处理微博任务失败：{str(e)}")

        # 保存剩余结果
        if processed_results:
            self.save_to_mongodb_batch(processed_results)

        # 保存JSON备份
        self.save_to_json(all_weibo_data)

        # 统计信息
        elapsed_time = time.time() - start_time
        logging.info(f"爬取完成！总计耗时：{elapsed_time:.2f}秒")
        logging.info(f"成功：{self.success_count} 条，失败：{self.failed_count} 条")
        logging.info(f"坏代理列表：{self.proxy_pool.err_proxy}")

        try:
            mongo_count = self.collection.count_documents({})
            logging.info(f"MongoDB 中总记录数：{mongo_count}")
        except PyMongoError as e:
            logging.error(f"获取MongoDB统计失败：{str(e)}")

        self.mongo_client.close()

        return {
            "success": self.success_count,
            "failed": self.failed_count,
            "total": self.success_count + self.failed_count,
            "total_pages": self.max_pages,
            "bad_proxies": self.proxy_pool.err_proxy,
            "elapsed_time": elapsed_time
        }

    def save_to_json(self, results: List[Dict], filename: str = None):
        """保存到JSON文件"""
        if not filename:
            filename = f"weibo_{self.uid}_results_{int(time.time())}.json"

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logging.info(f"结果已备份到 JSON 文件：{filename}")
        except Exception as e:
            logging.error(f"保存JSON文件失败：{str(e)}")


if __name__ == "__main__":
    # 配置参数
    UID = "6361439950"
    COOKIE_STR = """
XSRF-TOKEN=g_fUhERnpslVHrKYOmBRIT_I; WBPSESS=pCRaIcAtN6DhisyErIW0eYJci_b370fT494DDTI2VGK6CAhrahtxvV4Ce416tTW-NO-HEfXr_JOzpTydSdPA7r0aOJ2jChjbUROzPZuxMLjWsWr5okvvSREdjCcHquAg_VN21jMooO4OZe-qD_ArNg==; SCF=AvLmcMctnxRb_ASpcgx5OGne0akjwW36Aj24FxQdUiuem0UFkJQYy9--xWrZI6oGhSWkX2g742wGEEmx4_C09Ik.; SUB=_2A25ELRx3DeRhGe5K61AQ8i3IzT-IHXVnQxG_rDV8PUNbmtAbLWHfkW9NdMOTlw1PinqHbWL77yVy23jgtfvMU8t8; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9Whhw.90B-ioQDwrWKK.EAm95JpX5KzhUgL.FonXehzpeoeXSoe2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMRSh5EeKz0Shq0; ALF=02_1766914343
    """

    # 代理池配置（过滤掉None，只保留有效代理）
    proxy_list = [
        # 只保留有效的代理，示例：
        # "199.217.99.123:2525",
        # "95.173.218.67:8082",
    ]

    proxy_pool = ProxyPool(proxy_list)

    # MongoDB 配置
    MONGO_URI = "mongodb://localhost:27017/"
    MONGO_DB = "weibo_spider_db"

    try:
        # 初始化爬虫
        spider = WeiboSpider(
            uid=UID,
            cookie_str=COOKIE_STR.strip(),
            proxy_pool=proxy_pool,
            mongo_uri=MONGO_URI,
            mongo_db=MONGO_DB,
            max_pages=5,  # 初始页数，会根据接口返回的total动态调整
            retry_times=3,
            max_workers=3  # 减少线程数，避免并发过高导致的URL累积
        )

        # 运行爬虫
        stats = spider.run()
        logging.info(f"爬虫统计：{stats}")

    except Exception as e:
        logging.error(f"爬虫运行失败：{str(e)}")