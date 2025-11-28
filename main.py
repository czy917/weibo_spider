import requests
import json
import time
from typing import List, Dict, Optional, Tuple
import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from requests.exceptions import ProxyError, ConnectionError, Timeout, RequestException
# 禁用SSL警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)


class WeiboSpider:
    def __init__(self, uid: str, cookie_str: str, proxy: Optional[str] = None,
                 mongo_uri: str = "mongodb://localhost:27017/", mongo_db: str = "weibo_db",
                 max_pages: int = 5, retry_times: int = 3):
        """
        初始化微博爬虫（集成 MongoDB）
        :param uid: 目标用户的 UID
        :param cookie_str: 浏览器复制的 Cookie 字符串
        :param proxy: 代理地址
        :param mongo_uri: MongoDB 连接 URI
        :param mongo_db: MongoDB 数据库名称
        :param max_pages: 最大爬取页数
        :param retry_times: 请求重试次数
        """
        self.uid = uid
        self.session = requests.Session()
        self.proxy = {"http": proxy, "https": proxy} if proxy else None
        self.max_pages = max_pages
        self.retry_times = retry_times

        # 配置请求头
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Referer": f"https://weibo.com/u/{uid}",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Ch-Ua": '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        }

        # 解析 Cookie
        self._set_cookies(cookie_str)

        # MongoDB 配置
        self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # 测试MongoDB连接
        try:
            self.mongo_client.admin.command('ping')
            logging.info("MongoDB连接成功")
        except PyMongoError as e:
            logging.error(f"MongoDB连接失败: {e}")
            raise

        self.db = self.mongo_client[mongo_db]
        self.collection = self.db[f"weibo_{uid}"]
        # 创建索引防止重复
        self.collection.create_index("mblogid", unique=True)

        # 提取 XSRF-TOKEN
        xsrf_token = self.session.cookies.get("XSRF-TOKEN")
        if xsrf_token:
            self.headers["X-XSRF-TOKEN"] = xsrf_token
        else:
            logging.warning("未找到XSRF-TOKEN，可能影响请求")

    def _set_cookies(self, cookie_str: str):
        """解析并设置 Cookie"""
        if not cookie_str:
            logging.warning("Cookie字符串为空")
            return

        cookies = {}
        for item in cookie_str.split(";"):
            item = item.strip()
            if "=" in item:
                key, value = item.split("=", 1)
                cookies[key] = value
        self.session.cookies.update(cookies)

    def _request_with_retry(self, url: str, params: dict = None) -> Optional[dict]:
        """带重试机制的请求方法"""
        for retry in range(self.retry_times):
            try:
                response = self.session.get(
                    url=url,
                    params=params,
                    headers=self.headers,
                    proxies=self.proxy,
                    timeout=15,
                    verify=False  # 忽略SSL证书验证（根据需要调整）
                )
                response.raise_for_status()
                return response.json()

            except (ProxyError, ConnectionError):
                logging.error(f"代理连接失败（重试{retry + 1}/{self.retry_times}）")
                if self.proxy:
                    logging.warning("尝试关闭代理重试...")
                    self.proxy = None  # 临时关闭代理
                time.sleep(2 ** retry)  # 指数退避

            except Timeout:
                logging.error(f"请求超时（重试{retry + 1}/{self.retry_times}）")
                time.sleep(2 ** retry)

            except RequestException as e:
                logging.error(f"请求异常: {str(e)}（重试{retry + 1}/{self.retry_times}）")
                time.sleep(2 ** retry)

            except Exception as e:
                logging.error(f"解析响应失败: {str(e)}")
                return None

        logging.error(f"请求失败，已重试{self.retry_times}次")
        return None

    def get_mblog_data(self, page: int = 1):
        """获取某一页的微博数据（包含完整的初始信息）"""
        url = f"https://weibo.com/ajax/statuses/mymblog"
        params = {
            "uid": self.uid,
            "page": page,
            "feature": 0,
        }

        data = self._request_with_retry(url, params)
        if not data:
            return [], False, 0

        try:
            mblog_data_list = []
            for item in data.get("data", {}).get("list", []):
                mblog_data = {
                    "mblogid": item.get("mblogid"),
                    "text": item.get("text", ""),
                    "text_raw": item.get("text_raw", ""),
                    "created_at": item.get("created_at", ""),
                    "title": item.get("title", ""),
                    "has_expand": ">展开<" in item.get("text", "")
                }
                if mblog_data["mblogid"]:
                    mblog_data_list.append(mblog_data)

            has_next = len(data.get("data", {}).get("list", [])) >= 10  # 每页10条
            total_pages = data.get("data", {}).get("total", 0) // 10 + 1
            logging.info(
                f"第 {page} 页获取到 {len(mblog_data_list)} 条微博数据（{sum(1 for d in mblog_data_list if d['has_expand'])} 条需要展开）")
            return mblog_data_list, has_next, total_pages//10

        except Exception as e:
            logging.error(f"处理微博数据失败：{str(e)}")
            return [], False, 0

    def get_longtext(self, mblogid: str):
        """根据 mblogid 获取长文本"""
        url = f"https://weibo.com/ajax/statuses/longtext"
        params = {"id": mblogid}

        data = self._request_with_retry(url, params)
        if not data:
            return None

        try:
            result = {
                "full_text": data.get("data", {}).get("longTextContent", ""),
                "is_longtext": True
            }
            logging.debug(f"成功获取 mblogid={mblogid} 的长文本")
            return result
        except Exception as e:
            logging.error(f"解析长文本失败：{str(e)}")
            return None

    def process_mblog(self, mblog_data: Dict):
        """处理单条微博数据（判断是否需要获取长文本）"""
        result = {
            "mblogid": mblog_data["mblogid"],
            "uid": self.uid,
            "original_text": mblog_data["text"],
            "text_raw": mblog_data["text_raw"],
            "created_at": mblog_data["created_at"],
            "title": mblog_data["title"],
            "has_expand": mblog_data["has_expand"],
            "crawl_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            "is_success": False
        }

        try:
            # 判断是否需要获取长文本
            if mblog_data["has_expand"]:
                # 需要展开，调用长文本接口
                longtext_data = self.get_longtext(mblog_data["mblogid"])
                if longtext_data:
                    result["full_text"] = longtext_data["full_text"]
                    result["is_success"] = True
                    result["is_longtext"] = True
                else:
                    # 长文本获取失败，使用 text_raw 作为备用
                    result["full_text"] = mblog_data["text_raw"]
                    result["error_msg"] = "长文本获取失败，使用备用文本"
            else:
                # 不需要展开，直接使用 text_raw
                result["full_text"] = mblog_data["text_raw"]
                result["is_success"] = True
                result["is_longtext"] = False
                logging.debug(f"mblogid={mblog_data['mblogid']} 无需展开，直接使用 text_raw")

        except Exception as e:
            result["error_msg"] = f"处理失败: {str(e)}"
            logging.error(f"处理微博 {mblog_data['mblogid']} 失败: {str(e)}")

        return result

    def save_to_mongodb(self, data: Dict):
        """保存数据到 MongoDB"""
        try:
            # 使用 mblogid 作为唯一索引，避免重复存储
            self.collection.update_one(
                {"mblogid": data["mblogid"]},
                {"$set": data},
                upsert=True  # 如果不存在则插入，存在则更新
            )
            logging.debug(
                f"保存 mblogid={data['mblogid']} 到 MongoDB（{'长文本' if data.get('is_longtext') else '普通文本'}）")
        except PyMongoError as e:
            logging.error(f"保存到 MongoDB 失败：{str(e)}")

    def save_to_json(self, all_results: List[Dict], filename: str = None):
        """保存到 JSON 文件（备份）"""
        if not filename:
            filename = f"weibo_{self.uid}_results_{int(time.time())}.json"

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)
            logging.info(f"结果已备份到 JSON 文件：{filename}")
        except Exception as e:
            logging.error(f"保存JSON文件失败：{str(e)}")

    def run(self):
        """启动爬虫"""
        all_results = []
        page = 1

        logging.info(f"开始爬取用户 {self.uid} 的微博数据，最大页数：{self.max_pages}")

        while page <= self.max_pages:
            logging.info(f"正在爬取第 {page}/{self.max_pages} 页...")

            # 获取当前页的微博数据
            mblog_data_list, has_next, self.max_pages = self.get_mblog_data(page)

            if not mblog_data_list:
                logging.warning(f"第 {page} 页未获取到任何微博数据")
                # 判断是否继续重试
                if page == 1:
                    logging.error("第一页就获取失败，可能是Cookie过期或账号问题")
                    break
                else:
                    logging.warning("停止爬取")
                    break


            # 处理每条微博数据
            for mblog_data in mblog_data_list:
                result = self.process_mblog(mblog_data)
                all_results.append(result)
                self.save_to_mongodb(result)

                # 控制请求频率
                if mblog_data["has_expand"]:
                    time.sleep(1.5)
                else:
                    time.sleep(0.3)

            # 如果没有下一页，停止爬取
            if not has_next or page >= self.max_pages:
                break

            page += 1
            time.sleep(3)  # 分页间隔

        # 备份到 JSON
        if all_results:
            self.save_to_json(all_results)
        else:
            logging.warning("未获取到任何数据，跳过JSON备份")

        # 统计结果
        total = len(all_results)
        success = sum(1 for r in all_results if r["is_success"])
        longtext_count = sum(1 for r in all_results if r.get("is_longtext"))

        logging.info(f"爬取完成！总计 {total} 条微博（成功 {success} 条，长文本 {longtext_count} 条）")

        # 获取MongoDB统计
        try:
            mongo_count = self.collection.count_documents({})
            logging.info(f"MongoDB 中总记录数：{mongo_count}")
        except PyMongoError as e:
            logging.error(f"获取MongoDB统计失败：{str(e)}")

        # 关闭MongoDB连接
        self.mongo_client.close()

        return all_results


if __name__ == "__main__":
    # 配置参数
    UID = "5703799536"
    COOKIE_STR = """
    XSRF-TOKEN=yW1uVF4cDliSBL8rsFZtTg0M; SCF=Ah15xFnVdYhGco46RKFCVK4BIepO_Mj6Mw_E7u9eelw1IrAH20lz_Ha-QyBdaCsKizPtPkk1E0vvrzNc6g_IZMM.; SUB=_2A25ELIeJDeRhGe5K61AQ8i3IzT-IHXVnQ4VBrDV8PUNbmtAbLVTTkW9NdMOTl0M_GNQDnfhVLFnohRairV3cxG52; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9Whhw.90B-ioQDwrWKK.EAm95NHD95QRSh5EeKz0Shq0Ws4DqcjMi--NiK.Xi-2Ri--ciKnRi-zN1hB7eo2Ee0Bce7tt; _s_tentry=passport.weibo.com; Apache=9453706722489.812.1764292574142; SINAGLOBAL=9453706722489.812.1764292574142; ULV=1764292574165:1:1:1:9453706722489.812.1764292574142:; WBPSESS=pCRaIcAtN6DhisyErIW0eYJci_b370fT494DDTI2VGK6CAhrahtxvV4Ce416tTW-NO-HEfXr_JOzpTydSdPA7r_vGNPWjadPSKsRCwIljCWVZbuXEkhEgro78oJ8hH9WpQaUiEUxZjBAB69GhnFKVw==
    """

    # 代理配置（如果代理不可用，设为None）
    PROXY = None  # "http://127.0.0.1:7890"

    # MongoDB 配置
    MONGO_URI = "mongodb://localhost:27017/"
    MONGO_DB = "weibo_spider_db"

    try:
        # 初始化爬虫并运行
        spider = WeiboSpider(
            uid=UID,
            cookie_str=COOKIE_STR.strip(),
            proxy=PROXY,
            mongo_uri=MONGO_URI,
            mongo_db=MONGO_DB,
            max_pages=5,
            retry_times=3
        )
        results = spider.run()
    except Exception as e:
        logging.error(f"爬虫运行失败：{str(e)}")
