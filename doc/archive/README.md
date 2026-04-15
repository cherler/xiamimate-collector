# doc 目录归档说明

`doc/` 根目录现在只保留当前仍会被代码直接读取的输入资产：

- `amazon_{geo}_category_tree.csv`：`auto-collect` 调度器运行时读取的类目树文件，也作为 `fetch_categories` 默认中文类目映射来源。

以下文件不再作为当前运行输入，已归档到 `doc/archive/参考资料与模板/`：

- `amazon_us_category_mapping.csv`
- `跨境电商标准字段字典.csv`
- `跨境电商数据采集清单模板.csv`
- `跨境电商优先渠道清单.csv`
- `跨境电商数据获取渠道说明.md`

归档原则：

- `doc/` 根目录放运行或维护命令会直接读取的文件。
- 纯参考资料、模板和历史辅助说明放到 `doc/archive/`。