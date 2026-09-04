# !/bin/python
from datetime import datetime, timedelta

from pywebio.input import checkbox, input, input_group, textarea
from pywebio.output import put_text

from shared.ui.pywebio_helper import put_black_text, put_separator, start_pywebio_app


def is_last_day_of_month(date_str: str) -> bool:
    current = datetime.strptime(date_str, "%Y%m%d")
    next_month = current.replace(day=28) + timedelta(days=4)
    return current == next_month - timedelta(days=next_month.day)


def iter_dates(start_date: str, end_date: str):
    current = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date()
    while current <= end:
        yield current
        current += timedelta(days=1)


def render_sql(sql_template: str, start_date: str, end_date: str, only_month_end: bool):
    for current in iter_dates(start_date, end_date):
        current_ymd = current.strftime("%Y%m%d")
        current_dash = current.strftime("%Y-%m-%d")
        if only_month_end and not is_last_day_of_month(current_ymd):
            continue
        statement = sql_template.replace("yyyymmdd10", current_dash).replace(
            "yyyymmdd", current_ymd
        )
        put_text(statement)


def main():
    form = input_group(
        "按日期批量生成 SQL",
        [
            textarea(
                "SQL 模板，使用 yyyymmdd 和 yyyymmdd10 作为占位符",
                name="sql",
                value="select * from table where dt = yyyymmdd;",
            ),
            input("开始日期", name="start_date", value="20240101"),
            input("结束日期", name="end_date", value="20250101"),
            checkbox("仅输出月末日期", options=["是"], name="only_month_end", value=[]),
        ],
    )
    put_black_text("生成结果")
    put_separator()
    render_sql(
        sql_template=form["sql"],
        start_date=form["start_date"],
        end_date=form["end_date"],
        only_month_end=form["only_month_end"] == ["是"],
    )


if __name__ == "__main__":
    start_pywebio_app("Lakehouse Toolkit", main)
