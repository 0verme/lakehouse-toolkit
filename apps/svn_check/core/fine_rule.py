# !/bin/python
import xml.etree.ElementTree as ET
from pathlib import Path

from services.re_service import (
    extract_tables,
    find_dot_strings,
    find_hardcoded_dates,
    read_data_from_file,
)

from core.public_data import all_fine, all_role


def _parse_xml(file_path: str | Path) -> ET.Element:
    """解析本地导出的 XML，并拒绝 DTD/entity 声明以避免 XML 注入。"""
    payload = Path(file_path).read_bytes()
    upper_payload = payload.upper()
    if b"<!DOCTYPE" in upper_payload or b"<!ENTITY" in upper_payload:
        raise ValueError("XML DTD/entity declarations are not allowed")
    return ET.fromstring(payload)  # noqa: S314 - DTD/entity input is rejected above.


gjz_lists = ["DATETIME", "DUAL", "AGE", "LAST_DAY"]

SENSITIVE_FIELD_RULES = {
    "证件类型": ["证件类型", "CERT_TYPE", "ID_TYPE"],
    "身份证": [
        "身份证",
        "身份证号",
        "证件号码",
        "ID_CARD",
        "IDCARD",
        "ID_NO",
        "CERT_NO",
        "SFZ",
        "ZJH",
        "CERT_ID",
    ],
    "地址": ["地址", "开户地址", "家庭地址", "住址", "ADDRESS", "ADDR"],
    "手机号": [
        "手机号",
        "手机号码",
        "联系电话",
        "移动电话",
        "MOBILE",
        "PHONE_NO",
        "TEL_NO",
        "PHONE",
    ],
    "卡号": [
        "卡号",
        "银行卡号",
        "借记卡号",
        "贷记卡号",
        "信用卡号",
        "卡片号码",
        "卡号码",
        "CARD_NO",
        "CARDNO",
        "CARD_NUM",
        "CARD_NUMBER",
        "BANK_CARD_NO",
        "BANKCARD_NO",
        "CARD_ID",
        "PAN",
    ],
    "账号": [
        "账号",
        "帐号",
        "账户",
        "帐户",
        "账户号",
        "帐户号",
        "银行账号",
        "银行帐号",
        "客户账号",
        "客户帐号",
        "结算账号",
        "结算帐号",
        "ACCT_NO",
        "ACCTNO",
        "ACCOUNT_NO",
        "ACCOUNTNO",
        "ACC_NO",
        "ACCNO",
        "ACCOUNT",
        "ACCT",
        "BANK_ACCT_NO",
        "BANK_ACCOUNT_NO",
        "CUST_ACCT_NO",
    ],
    "邮箱": [
        "邮箱",
        "电子邮箱",
        "电子邮件",
        "邮件地址",
        "邮箱地址",
        "E_MAIL",
        "EMAIL",
        "MAIL",
        "EMAIL_ADDR",
        "EMAIL_ADDRESS",
        "MAIL_ADDR",
        "MAIL_ADDRESS",
    ],
    "座机": [
        "座机",
        "座机号",
        "座机号码",
        "固定电话",
        "固话",
        "办公电话",
        "公司电话",
        "家庭电话",
        "住宅电话",
        "LANDLINE",
        "FIXED_PHONE",
        "FIXED_TEL",
        "OFFICE_TEL",
        "OFFICE_PHONE",
        "HOME_TEL",
        "HOME_PHONE",
        "TELEPHONE",
    ],
}


def get_cpt_sql(fine_name):
    # 解析 XML 文件
    root = _parse_xml(fine_name)
    # 使用 findall 方法查找所有 Query 元素
    query_elements = root.findall(".//Query")
    reslut = ""
    # 提取并打印每个Query元素的文本内容
    for query_element in query_elements:
        reslut += query_element.text or ""
    return reslut


def find_sensitive_fields(text):
    upper_text = text.upper()
    hit_fields = []
    for field_name, keywords in SENSITIVE_FIELD_RULES.items():
        matched_keywords = []
        for keyword in keywords:
            if keyword.upper() in upper_text and keyword not in matched_keywords:
                matched_keywords.append(keyword)
        if matched_keywords:
            hit_fields.append(
                f"{field_name}(命中关键字: {', '.join(matched_keywords)})"
            )
    return hit_fields


def extract_sub_path(file_path, anchor="数据仓库"):
    p = Path(file_path)
    parts = p.parts  # 自动处理 / 和 \

    if anchor in parts:
        idx = parts.index(anchor)
        return Path(*parts[idx:])  # 拼回路径
    else:
        return None


def find_report(xml_file_path):
    # 解析 XML 文件
    root = _parse_xml(xml_file_path)
    reports = []
    # 查找所有<Report>标签
    for report in root.findall(".//Report"):
        # 检查class和name属性是否匹配
        if report.get("class") == "com.fr.report.worksheet.WorkSheet":
            reports.append(report.get("name"))
    return reports


def find_column_name(xml_file_path):
    # 解析 XML 文件
    root = _parse_xml(xml_file_path)
    column_names = []
    # 查找所有<Attributes>标签
    for attributes in root.findall(".//Attributes"):
        # 检查dsName属性是否为'表头'
        if attributes.get("dsName") == "表头":
            # 获取columnName属性值
            column_name = attributes.get("columnName")
            column_names.append(column_name)
    return column_names


def find_clientPaging(xml_file_path):
    # 解析 XML 文件
    root = _parse_xml(xml_file_path)
    flag = "未开分页引擎"
    # 查找所有<Report>标签
    for report in root.findall(".//LayerReportAttr"):
        # 检查class和name属性是否匹配
        if report.get("clientPaging") == "true":
            flag = "新计算引擎"
        if report.get("clientPaging") == "true" and report.get("engineState") == "1":
            flag = "行式引擎"
    return flag


def get_cpt_yuan(fine_name):
    # 解析 XML 文件
    root = _parse_xml(fine_name)
    # 使用 findall 方法查找所有 Query 元素
    query_elements = root.findall(".//DatabaseName")
    reslut = []
    # 提取并打印每个Query元素的文本内容
    for query_element in query_elements:
        if query_element.text:
            reslut.append(query_element.text.replace("\n", ""))
    reslut = list(set(reslut))
    return ",".join(reslut)


def rule_menu(authority_name):
    print("===========rule_menu==========")
    try:
        data = read_data_from_file(authority_name)
        relsut = []
        cnt = 0
        relsut_test = ""
        for i in data.split("\n"):
            i.replace("，", ",").replace("\r\n", "\n")
            finememu = i.split(",")[1]
            cpturl = i.split(",")[0]
            if "数据仓库/" not in cpturl:
                relsut_test += f"{cpturl} 第一段路径不对 需要在开头加上 数据仓库/ "
                cnt += 1
            if ".cpt" not in cpturl and ".frm" not in cpturl:
                relsut_test += f"{cpturl} 第一段路径不对 目录里应有.cpt或.frm"
                cnt += 1
            if "(村镇银行发展部)" in cpturl:
                relsut_test += f"{cpturl} 第一段路径不对 (村镇银行发展部) 改为 中文括号（村镇银行发展部） "
                cnt += 1
            if "会计结算部" in cpturl or "会计部报表" in cpturl:
                relsut_test += (
                    f"{cpturl} 第一段路径不对 会计结算部/会计部报表 改为 运营管理部"
                )
                cnt += 1
            if " " in cpturl:
                relsut_test += f"{cpturl} 里面有空格"
                cnt += 1
            if "数据仓库/" in finememu:
                relsut_test += f"{finememu} 第二段路径不对 数据仓库/ 不需要写"
                cnt += 1
            if "会计结算部" in finememu or "会计部报表" in finememu:
                relsut_test += (
                    f"{finememu} 第二段路径不对 会计结算部/会计部报表 改为 运营管理部"
                )
                cnt += 1
            if "互联网金融部" in finememu:
                relsut_test += f"{finememu} 第二段路径不对 互联网金融部 改为 互联网金融"
                cnt += 1
            if "（村镇银行发展部）" in finememu:
                relsut_test += f"{finememu} 第二段路径不对 （村镇银行发展部） 改为 英文括号 (村镇银行发展部)"
                cnt += 1
            if ".cpt" in finememu:
                relsut_test += f"{finememu} 第二段路径不对 目录里不应有.cpt"
                cnt += 1
            if " " in finememu:
                relsut_test += f"{finememu} 里面有空格"
                cnt += 1
            relsut.append(finememu.split("/")[-1])
        return relsut, relsut_test, cnt
    except Exception as e:
        return [], str(e.args), 1


def normalize_fine_entry_name(value):
    if value is None:
        return ""
    text = str(value).replace("\ufeff", "").replace("\r", "").replace("\n", "").strip()
    text = text.replace("，", ",")
    if "/" in text:
        text = text.split("/")[-1]
    if text.endswith((".cpt", ".frm")):
        text = text.rsplit(".", 1)[0]
    return text.strip()


def rule_authority(authority_name, memu_url):
    print("===========rule_authority==========")
    role_lists, fine_lists = all_role(), all_fine()

    relsut_test = "存在问题:\n"
    try:
        data = read_data_from_file(authority_name)
        cnt = 0
        valid_report_names = {
            normalize_fine_entry_name(name)
            for name in fine_lists + memu_url
            if normalize_fine_entry_name(name)
        }
        for i in data.split("\n"):
            line = i.replace("，", ",").replace("\r\n", "").strip()
            if not line:
                continue
            repot_name = normalize_fine_entry_name(line.split(",")[0])
            r_lists = [
                item.replace("\n", "").strip()
                for item in line.split(",")[1:]
                if item.strip()
            ]
            for j in r_lists:
                if j not in role_lists:
                    print(j)
                    relsut_test += f"{j} 生产没有该角色 \n"
                    cnt += 1
            if repot_name and repot_name not in valid_report_names:
                relsut_test += f"{repot_name} 未有该目录 \n"
                cnt += 1
        return relsut_test, cnt
    except Exception as e:
        return str(e.args), 1


def rule_fine(fine_name):
    print("===========rule_fine==========")
    sstb_name = []
    viewlet_url = str(extract_sub_path(fine_name))
    print(fine_name)
    relsut_test = "存在问题:\n"
    cnt = 0
    data = read_data_from_file(fine_name)
    f_name = fine_name.split("/")[-1]
    if "[" not in f_name or "]" not in f_name:
        relsut_test += "该帆软没有报表编号\n"
        cnt += 1
    if " " in fine_name:
        relsut_test += "名称有带空值\n"
        cnt += 1
    if "DISTINCT" in data.upper():
        relsut_test += "请审核重点检查脚本中的distinct是否必须添加 有无关联出重复数据\n"
        cnt += 1
    if '<ATTR DIVIDEMODE="1"/>' in data:
        relsut_test += "该报表没有列表展示 全是分组，请确认是否需要分组\n"
        cnt += 1
    if "权限机构树" in data and "USER_DIRECTORY" not in data.upper():
        relsut_test += "机构树默认值不对\n"
        cnt += 1
    for i in sstb_name:
        if i.upper() in data.upper():
            relsut_test += f" {viewlet_url} 用错表 {i}"
            cnt += 1
    try:
        yuan = get_cpt_yuan(fine_name).upper()
        sheets = find_report(fine_name)
        reslut = get_cpt_sql(fine_name).upper()
        yq = find_clientPaging(fine_name)
        sensitive_fields = find_sensitive_fields(reslut)
        if sensitive_fields:
            relsut_test += f"检测到敏感信息字段: {','.join(sensitive_fields)}，请重点确认是否涉及证件或个人隐私信息展示\n"
            cnt += 1
        datekk = find_hardcoded_dates(reslut)
        datekk = ["'" + item + "'" for item in datekk]
        datekk = list(set(datekk))
        if len(datekk) > 0:
            relsut_test += (
                "检测到的写死日期 请甄别是否业务需求 (如果是注释日期去掉两头引号): "
                + " ".join(datekk)
                + "\n"
            )
            cnt += 1

        if "=(SELECT" in reslut:
            relsut_test += (
                "存在 = ( select 子查询 注意跑批效率 和 万一数据多条导致程序报错\n"
            )
            cnt += 1
        reslut = reslut.replace("JOIN(SELECT", "")
        if "IN(SELECT" in reslut:
            relsut_test += "存在 in ( select 子查询 注意跑批效率\n"
            cnt += 1
        if ".END_DT>=" in reslut:
            relsut_test += "检测到 END_DT>= 注意拉链数据重复\n"
            cnt += 1
        if "D_DATE=TO_DATE('" in data:
            relsut_test += (
                "存在关键字 D_DATE=TO_DATE(' 使用主题表请改为 d_date ='YYYYMMDD' \n"
            )
            cnt += 1
        if "D_DATE=DATE'" in data:
            relsut_test += (
                "存在关键 D_DATE = DATE' 使用主题表请改为 d_date ='YYYYMMDD' \n"
            )
            cnt += 1

        tables = extract_tables(reslut)
        tables2 = find_dot_strings(reslut)
        tables_total = tables + tables2
        sql_table = list(set(tables_total))
        for i in sql_table:
            if i.upper() in gjz_lists:
                pass
            elif "." not in i.upper():
                relsut_test += (
                    f"表名 {i} 没有带SCHAME请注意加上 如果是用with表注意效率\n"
                )
                cnt += 1
        sql_table = sorted(sql_table)

        return relsut_test, cnt, [viewlet_url, yuan, yq, sheets, sql_table]

    except Exception as e:
        return str(e.args), 1, ["", "", "", [], []]
