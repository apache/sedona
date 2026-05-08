<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# 为 Apache Sedona 贡献

本项目欢迎贡献。您可以在 [Sedona GitHub 仓库](https://github.com/apache/sedona) 上提交 Pull Request 来贡献代码或文档。

下面简述完成一次贡献的工作流程。

## 选取/公示任务

在开始贡献之前，确认贡献内容是否会被接受很重要。请先通过工单（ticket）告知社区您的意图。Sedona 同时接受 [GitHub issues](https://github.com/apache/sedona/issues?q=sort%3Aupdated-desc+is%3Aissue+is%3Aopen) 与 [JIRA](https://issues.apache.org/jira/projects/SEDONA)。新建 JIRA 工单时，会自动发送邮件通知 `dev@sedona.apache.org`。

## 编写代码贡献

代码贡献应包括：

* 类与方法的详尽文档。
* 用于证明代码正确性、并能在后续维护中保持的单元测试。对于 bug 修复，单元测试应能在没有该修复时复现 bug（如适用）。单元测试可以是 JUnit 测试或 Scala 测试。部分 Sedona 函数需要在 Scala 与 Java 两侧分别测试。
* 必要时更新对应的 Sedona 文档。

代码贡献必须在每个文件顶部包含 Apache 2.0 许可声明。

提交 PR 前请运行 `mvn spotless:apply` 来格式化代码。如果您修改了某个特定 Spark 版本的代码（例如 `spark/spark-3.5/` 下的源文件），请追加对应的 Maven CLI 参数：`mvn spotless:apply -Dscala=2.12 -Dspark=3.5`。

## 编写文档贡献

文档贡献应满足以下要求：

* 详细的解释与示例。
* 把新增文档放在合适的目录下。
* 必要时修改 ==mkdocs.yml==。

!!!note
	请阅读 [编译源码](../setup/compile.md#compile-the-documentation) 了解如何编译 Sedona 网站。

## 提交 Pull Request

完成贡献之后，最简单也最直观的提交方式是向 [GitHub 仓库](https://github.com/apache/sedona) 提交 Pull Request（PR）。

**请在 PR 名称中使用 JIRA 工单 ID 或 GitHub issue ID，例如 "[SEDONA-1] my subject" 或 "[GH-1] my subject"。**

创建 PR 时，请回答 PR 模板中的问题。

提交 PR 之后，GitHub Action 会自动检查构建是否正确。请关注 PR 状态并修复其中的所有问题。

## 评审 Pull Request

* 每个 PR 需要满足：(1) 至少有 1 名 committer 批准；(2) 没有 committer 表示反对。任何人都欢迎参与评审，但最终决定由 committer 做出。
* 其他评审者（包括社区成员与 committer）可以评论变更并建议修改。可以直接向同一分支推送更多 commit 来追加修改。
* 即使最终结果可能是拒绝整个变更，社区也鼓励大家进行积极、礼貌、迅速的技术讨论。
* 请注意：对 Sedona 关键部分（如 Sedona core、空间连接算法）的改动会接受更严格的评审，可能需要更多测试与正确性证明。
* 有时其他变更被合并后，会与您的 PR 产生冲突。冲突解决前 PR 无法合并。请手动解决冲突，并把结果推送到您的分支。

## 行为准则

请阅读 [Apache 软件基金会行为准则](https://www.apache.org/foundation/policies/conduct.html)。

凡正式或非正式参与 Apache 社区、声称与基金会有任何关联、参与任何与基金会相关的活动（尤其是以任何身份代表 ASF）的人员，都需遵守这一准则。
