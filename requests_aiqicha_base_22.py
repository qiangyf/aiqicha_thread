
import requests
import pymysql
import re
import json
import random
from threading import Thread


def get_proxy():
    host1 = ""
    port1 = ""
    user1 = ''
    password1 = ''
    proxy_meta1 = "http://%(user)s:%(password)s@%(host)s:%(port)s" % {
        "host": host1,
        "port": port1,
        "user": user1,
        "password": password1,
    }
    
    host2 = ""
    port2 = ""
    user2 = ''
    password2 = ''

    proxy_meta2 = "http://%(user)s:%(password)s@%(host)s:%(port)s" % {
                "host": host2,
                "port": port2,
                "user": user2,
                "password": password2,
            }
    proxy_meta = random.choice([proxy_meta1,proxy_meta2])
    proxies = {
        "http": proxy_meta,
        "https": proxy_meta,
    }
    return proxies

def requests_proxy(url):
    #加上代理后，重写了request,加入循环，加上try,防止因为代理问题，程序终止
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36',
    }
    for i in range(20):
        proxy = get_proxy()
        try:
            r = requests.get(url,headers=headers,proxies=proxy)
            html = r.text
            break
        except:
            html = ''
    if html:
        return html
    else:
        print('爬取代理遇到问题')
        return html

def get_pid(company_name):
    
    url = 'https://aiqicha.baidu.com/s?q={}'.format(company_name)
    html = requests_proxy(url)
    html = re.findall('window.pageData = ({.*})', html)[0]
    dict_data = json.loads(html)
    pid = dict_data['result']['resultList'][0]['pid']
    return pid

def detail_header_info(pid):
    url = 'https://aiqicha.baidu.com/company_detail_{}'.format(pid)
    html = requests_proxy(url)
    json_data = re.findall('window.pageData = ({.*})',html)[0]

    dict_data = json.loads(json_data)

    source_phone = dict_data['result']['telephone']
    source_web = dict_data['result']['website']
    source_email = dict_data['result']['email']
    source_address = dict_data['result']['addr']
    source_introduce = dict_data['result']['describe']
    return (source_phone,source_web,source_email,source_address,source_introduce)

def base_info(pid,company_name):
    
    url = 'https://aiqicha.baidu.com/detail/basicAllDataAjax?pid={}'.format(pid)
    html = requests_proxy(url)
    dict_data = json.loads(html)

    basicData = dict_data.get('data').get('basicData')
    product_info = ''
    investment_ins = ''
    representative = basicData.get('legalPerson')
    registered_capital = basicData.get('regCapital')
    paid_in_capital = basicData.get('paidinCapital')
    company_status = basicData.get('openStatus')
    establishment_date = basicData.get('startDate')
    social_credit_code = basicData.get('unifiedCode')
    taxpayer_identification_number = basicData.get('taxNo')
    registration_number = basicData.get('licenseNumber')
    registration_number_organization_code = basicData.get('orgNo')
    enterprise_type = basicData.get('entType')
    industry = basicData.get('industry')
    approval_date = basicData.get('annualDate')
    registration_authority = basicData.get('authority')
    area = basicData.get('district')
    english_name = ''
    history_name = basicData.get('prevEntName')
    num_insured = ''
    staff_size = ''
    business_term = basicData.get('openTime')
    business_scope = basicData.get('scope')
    import_export_enterprise_code = ' '
    source_phone, \
    source_web, \
    source_email, \
    source_address, \
    source_introduce = detail_header_info(pid)
    data = (company_name,
            source_phone,
            source_web,
            source_email,
            source_address,
            source_introduce,
            product_info,
            investment_ins,
            representative,
            registered_capital,
            paid_in_capital,
            company_status,
            establishment_date,
            social_credit_code,
            taxpayer_identification_number,
            registration_number,
            registration_number_organization_code,
            enterprise_type,
            industry,
            approval_date,
            registration_authority,
            area,
            english_name,
            history_name,
            num_insured,
            staff_size,
            business_term,
            business_scope,
            import_export_enterprise_code)
    
    save_data(data)
    return data

def save_data(data):
    conn,cur = connect_mysql()
    #插入数据
    sql = 'insert into tb_aiqicha_base_mingluji values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    rows = cur.execute(sql,data)
    conn.commit()
    cur.close()
    conn.close()

def connect_mysql():
    conn = pymysql.connect(
        port=3306,
        user='rhino',
        password='rhino',
        db='OceanSpider_Base',
        charset='utf8mb4'
    )
    # conn = pymysql.connect(
    #     port=3306,
    #     user='root',
    #     password='',
    #     db='test',
    #     charset='utf8mb4'
    # )
    cur = conn.cursor()
    return conn,cur

def get_all_company_names():
	db = pymysql.connect("localhost", "rhino", 'rhino', "OceanSpider_Base")
	
	cursor = db.cursor()
	cursor.execute("SELECT * FROM tb_mingluji WHERE city='南京'")
	data = cursor.fetchall()
	#打印获取到的数据
	data = [d[1] for d in data]
	#关闭游标和数据库的连接
	cursor.close()
	db.close()
	return set(data)


def get_crawl_company_names():
	db = pymysql.connect("localhost", "rhino", 'rhino', "OceanSpider_Base")
	
	cursor = db.cursor()
	cursor.execute("SELECT company_name FROM tb_aiqicha_base_mingluji")
	data = cursor.fetchall()
	data = [d[0] for d in data]
	cursor.close()
	db.close()
	return set(data)

def get_second_crawl_company():
	all_company_names = get_all_company_names()
	crawl_company_names = get_crawl_company_names()
	second_crawl_company_names = all_company_names-crawl_company_names
	return second_crawl_company_names

class MyThread(Thread):
    def __init__(self, spider, args):
        '''
        :param func: 可调用的对象
        :param args: 可调用对象的参数
        '''
        Thread.__init__(self)   # 不要忘记调用Thread的初始化方法
        self.func = spider
        self.args = args

    def run(self):
        self.func(*self.args)

def spider(a,b):
        
    for company in company_datas[a:b]:
        try:
            pid = get_pid(company)
            print('当前开始爬取公司名：',company)
            data = base_info(pid,company_name=company)
        except:
            pass
            
def main():
    # 创建 Thread 实例
    #主函数里手动开了22个线程，后续还需要改进，改进成自动分配的，暂时不需要加锁。
    t1 = MyThread(spider, (0, 10000))
    t2 = MyThread(spider, (10000, 20000))
    t3 = MyThread(spider, (20000, 30000))
    t4 = MyThread(spider, (30000, 40000))
    t5 = MyThread(spider, (40000, 50000))
    t6 = MyThread(spider, (50000, 60000))
    t7 = MyThread(spider, (60000, 70000))
    t8 = MyThread(spider, (70000, 80000))
    t9 = MyThread(spider, (80000, 90000))
    t10 = MyThread(spider, (90000, 100000))
    t11 = MyThread(spider, (100000, 110000))
    t12 = MyThread(spider, (110000, 120000))
    t13 = MyThread(spider, (120000, 130000))
    t14 = MyThread(spider, (130000, 140000))
    t15 = MyThread(spider, (140000, 150000))
    t16 = MyThread(spider, (150000, 160000))
    t17 = MyThread(spider, (160000, 170000))
    t18 = MyThread(spider, (170000, 180000))
    t19 = MyThread(spider, (180000, 190000))
    t20 = MyThread(spider, (190000, 200000))
    t21 = MyThread(spider, (200000, 210000))
    t22 = MyThread(spider, (210000, 220000))
    # 启动线程运行
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()
    t10.start()
    t11.start()
    t12.start()
    t13.start()
    t14.start()
    t15.start()
    t16.start()
    t17.start()
    t18.start()
    t19.start()
    t20.start()
    t21.start()
    t22.start()
    # 等待所有线程执行完毕
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    t9.join()
    t10.join()
    t11.join()
    t12.join()
    t13.join()
    t14.join()
    t15.join()
    t16.join()
    t17.join()
    t18.join()
    t19.join()
    t20.join()
    t21.join()
    t22.join()


#获取公司名信息
company_datas = list(get_second_crawl_company())
main()




