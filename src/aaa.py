import csv
import time
import requests
from bs4 import BeautifulSoup

default_fields = [
    {'Mark': 'Mark:'},
    {'US Serial Number': 'US Serial Number:'},
    {'US Registration Number': 'US Registration Number:'},
    {'Status Date': 'Status Date:'},
    {'Publication Date': 'Publication Date:'},
    {'International Class(es)': 'International Class(es):'},
    {'Owner Name': 'Owner Name:'},
    {'Owner Address': 'Owner Address:'},
    {'Legal Entity Type': 'Legal Entity Type:'},
    {'Citizenship': 'Citizenship:'},
    {'Correspondent Name/Address': 'Correspondent Name/Address:'},
    {'Correspondent e-mail': 'Correspondent e-mail:'},
    {'Correspondent e-mail Authorized': 'Correspondent e-mail Authorized:'}
]

def getText(html, text=''):
    tag = html.find('div', string=text)
    if bool(tag):
        tag = tag.find_next('div')
    res = ''
    if bool(tag):
        res = tag.text.replace(' ', '').replace('\n', '').replace('\t', '')
    return res

def saveCsv(start=0, end=100, fields=None, chunk_size=3000):
    # 默认字段映射配置（表头名称: 页面匹配的文本）
    if fields is None:
        fields = default_fields
    # 按最多 chunk_size（3000）条进行分组
    for group_start in range(start, end, chunk_size):
        group_end = min(group_start + chunk_size, end)
        filename = f"{group_start}_{group_end - 1}.csv"

        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # 动态生成表头：CaseNumber 加上所有配置项的 key
            headers = ['CaseNumber'] + [list(item.keys())[0] for item in fields]
            writer.writerow(headers)

            for i in range(group_start, group_end):
                try:
                    url = f'https://tsdr.uspto.gov/statusview/sn{i}'
                    page = requests.get(url, timeout=10)
                    print(f"[{i}] Status: {page.status_code}")

                    if page.status_code == 200:
                        html = BeautifulSoup(page.text, 'html.parser')
                        # 动态提取各字段的值
                        row = [str(i)]
                        for item in fields:
                            label = list(item.values())[0]
                            row.append(getText(html, label))
                    else:
                        # 请求失败时填充空值
                        row = [str(i)] + [''] * len(fields)

                    writer.writerow(row)
                    f.flush()  # 确保每爬一条写入一次磁盘，防止中断丢失数据
                except Exception as e:
                    print(f"[{i}] 抓取失败: {e}")
                    writer.writerow([str(i)] + [''] * len(fields))

                time.sleep(3)
def updateCsv(fileName,rows):
    f = open(fileName,'r')
    csv_reader =csv.reader(f)
    data = list(csv_reader)
    for row in data:
        if rows.count(row[0]) > 0:
            page = requests.get('https://tsdr.uspto.gov/statusview/sn' + row[0])
            print(page.status_code)
            html = BeautifulSoup(page.text,'html.parser')
            html.prettify()
            row[1] = getText(html,'Mark:')
            row[2] = getText(html,'US Serial Number:')
            row[3] = getText(html,'US Registration Number:')
            row[4] = getText(html,'Status Date:')
            row[5] = getText(html,'Publication Date:')
            row[6] = getText(html,'International Class(es):')
            row[7] = getText(html,'Owner Name:')
            row[8] = getText(html,'Owner Address:')
            row[9] = getText(html,'Legal Entity Type:')
            row[10] = getText(html,'Citizenship:')
            row[11] = getText(html,'Correspondent Name/Address:')
            row[12] = getText(html,'Correspondent e-mail:')
            row[13] = getText(html,'Correspondent e-mail Authorized:')
            print(row)
            time.sleep(1)
        else:
            print('')
    print(data)
    fr = open(fileName,'w', newline='',encoding='UTF8')
    csv_writer = csv.writer(fr)
    csv_writer.writerows(data)
type = input('请输入查询方法：1、根据输入的最大最小值来获取数据  2、修改更新原来的csv数据：\n')
if type == '1':
    start = input('请输入最小值：\n')
    end = input('请输入最大值：\n')
    saveCsv(int(start),int(end))
if type == '2':
    fileName = input('请输入文件名称：\n')
    data = input('请输入需要更新的数据，每个数据之间用英文的,隔开：\n')
    rows = data.split(',')
    updateCsv(fileName,rows)





# saveCsv(90593676,90593676+1000)
# arr = [0]
# for i in range(1):
#     t = Thread(target=saveCsv,args=(90593676 + i*1000, 90593676+ (i+1)*1000))
#     time.sleep(i*10)
#     t.start()


# f = open('test.html','w')
# f.write(page.text)
# f.close()
# f = open('data.csv','w')
# f.write('作者 \t 姓名 \t')
# f.close()