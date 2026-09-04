"""报表依赖分析的公开入口，复用通用 metadata 查询逻辑。"""

from .metadata_dependency import main

if __name__ == "__main__":
    from shared.ui.pywebio_helper import start_pywebio_app

    start_pywebio_app("报表依赖分析", main)
