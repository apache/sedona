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

Sedona 收到了来自社区的诸多帮助。本页列出了 Apache Sedona 的 Committers 与项目管理委员会成员（PMC）。
本页人员按姓氏排序。

## Committers

向 Sedona 贡献了足够多代码的贡献者会被晋升为 committer。Committer 拥有 Sedona 主仓库的写权限。

## 项目管理委员会（PMC）

当社区认为某位 committer 能够至少负责本项目的一个主要组件时，他/她会被晋升为 PMC 成员。

当前 Sedona PMC 成员如下：

|         姓名         |  GitHub ID   |        Apache ID        |
|:--------------------:|:------------:|:-----------------------:|
|     Adam Binford     |  Kimahriman  |  kimahriman@apache.org  |
|  Kanchan Chowdhury   |  kanchanchy  |  kanchanchy@apache.org  |
|  Kristin Cowalcijk   | Kontinuation | kontinuation@apache.org |
|     Furqaan Khan     | furqaankhan  |   furqaan@apache.org    |
|    Paweł Kociński    |   Imbruced   |   imbruced@apache.org   |
|       Yitao Li       |   yitao-li   |   yitaoli@apache.org    |
|    Netanel Malka     |  netanel246  |    malka@apache.org     |
|    Mohamed Sarwat    |    Sarwat    |   mosarwat@apache.org   |
|      Kengo Seki      |    sekikn    |    sekikn@apache.org    |
|     Sachio Wakai     |   SW186000   |    swakai@apache.org    |
|      Jinxuan Wu      |   jinxuan    |   jinxuanw@apache.org   |
|        Jia Yu        |   jiayuasu   |    jiayu@apache.org     |
|      Feng Zhang      | zhangfengcdt |  fengzhang@apache.org   |
|     Zongsi Zhang     | zongsizhang  | zongsizhang@apache.org  |
|     Felix Cheung     |              | felixcheung@apache.org  |
|     Von Gosling      |              |  vongosling@apache.org  |
|    Sunil Govindan    |              |    sunilg@apache.org    |
| Jean-Baptiste Onofré |              |   jbonofre@apache.org   |
|   George Percivall   |              |  percivall@apache.org   |

## 成为 Committer

开始为 Sedona 做贡献，请阅读 [如何贡献](rule.md) —— 任何人都可以向项目提交补丁、文档与示例。

PMC 会根据贡献情况，定期把活跃的贡献者晋升为新的 committer。新 committer 的资格条件包括：

* 对 Sedona 的持续贡献：committer 应当对 Sedona 有持续的重大贡献。
* 贡献的质量：committer 比社区中的其他成员更应提交简洁、经过良好测试、设计良好的补丁，同时具备评审补丁的能力。
* 社区参与度：committer 应在所有社区互动中保持建设性、友好的态度，活跃在 dev 邮件列表与 Discord，并帮助引导新贡献者与新用户。

PMC 也会增加新的 PMC 成员。PMC 成员需履行 Apache 指南中所规定的 PMC 职责，包括参与发布投票、维护 Apache 项目商标、对法律与许可问题负责、确保项目遵循 Apache 项目规范等。PMC 会定期把已展示出对上述职责的理解与执行能力的 committer 加入 PMC。

当前 Sedona Committers 如下：

|       姓名       |  GitHub ID  |        Apache ID        |
|:----------------:|:-----------:|:-----------------------:|
|   John Bampton   |  jbampton   |   johnbam@apache.org    |
| Dewey Dunnington | paleolimbot | paleolimbot@apache.org  |
|  Nilesh Gajwani  |   iGN5117   |    nilesh@apache.org    |
|   Peter Nguyen   |  petern48   |    petern@apache.org    |
|   Pranav Toggi   |  prantogg   |   prantogg@apache.org   |

## 提名 Committer 或 PMC 成员

步骤如下：

1. 发起投票（templates/committerVote.txt）。
2. 投票结束。如果结果为正，发出邀请给新 committer。

### 发起投票

我们在 private@sedona.apache.org 进行投票与讨论，以便进行坦诚的交流。

让 Vote 邮件运行一周。当达到 Consensus Approval（至少 3 个 +1 投票且无反对票）即视为通过。

#### PMC 投票模板

下面这封邮件用于发起对新 PMC 候选人的投票。新 PMC 成员需要先由现有 PMC 投票通过，再由董事会批准。

```
To: private@sedona.apache.org
Subject: [VOTE] New PMC candidate: [New PMC NAME]

[ 在此填写您提名该候选人的理由 ]

Voting ends one week from today, or until at least 3 +1 votes are cast.

```

### 关闭投票

下面这封邮件用于结束投票并向项目报告结果：

```
To: private@sedona.apache.org
Subject: [VOTE][RESULT] New PMC candidate: [New PMC NAME]

The vote has now closed: [paste the vote thread on https://lists.apache.org/list.html?private@sedona.apache.org]. The results are:

Binding Votes:

+1 [TOTAL BINDING +1 VOTES]
 0 [TOTAL BINDING +0/-0 VOTES]
-1 [TOTAL BINDING -1 VOTES]

The vote is ***successful/not successful***
```

### 通知 ASF Board

提名人需向 ASF Board（board@apache.org）发送一封邮件，附上投票结果链接，格式如下：

```
To: board at apache.org
CC: private at sedona.apache.org
Subject: [NOTICE] New PMC NAME for Apache Sedona PMC
Body:

New PMC NAME has been voted as a new member of the Apache Sedona PMC. The vote thread is at: *link to the vote result thread*
```

### 发送邀请

```
To: New PMC Email address
CC: private@sedona.apache.org

Hello [New PMC NAME],

The Sedona Project Management Committee (PMC)
hereby offers you committer privileges to the project
[as well as membership in the PMC]. These privileges are
offered on the understanding that you'll use them
reasonably and with common sense. We like to work on trust
rather than unnecessary constraints.

Being a committer enables you to more easily make
changes without needing to go through the patch
submission process. Being a PMC member enables you
to guide the direction of the project.

Being a committer does not require you to
participate any more than you already do. It does
tend to make one even more committed. You will
probably find that you spend more time here.

Of course, you can decline and instead remain as a
contributor, participating as you do now.

A. This personal invitation is a chance for you to
accept or decline in private. Either way, please
let us know in reply to the private@sedona.apache.org
address only.

B. If you accept, the next step is to register an iCLA:
    1. Details of the iCLA and the forms are found
    through this link: https://www.apache.org/licenses/#clas

    2. Instructions for its completion and return to
    the Secretary of the ASF are found at
    https://www.apache.org/licenses/#submitting

    3. When you transmit the completed iCLA, request
    to notify the Apache Sedona project and choose a
    unique Apache ID. Look to see if your preferred
    ID is already taken at
    https://people.apache.org/committer-index.html
    This will allow the Secretary to notify the PMC
    when your iCLA has been recorded.

When recording of your iCLA is noted, you will
receive a follow-up message with the next steps for
establishing you as a committer.

```

### PMC 接受邀请与 ICLA 说明

```
To: New PMC Email address
Cc: private@sedona.apache.org
Subject: Re: invitation to become Apache Sedona PMC

Welcome. Here are the next steps in becoming a project committer. After that we will make an announcement to the dev@sedona.apache.org

1. You need to send a Contributor License Agreement to the ASF.
Normally you would send an Individual CLA. If you also make
contributions done in work time or using work resources,
see the Corporate CLA. Ask us if you have any issues.
https://www.apache.org/licenses/#clas.

You need to choose a preferred ASF user name and alternatives.
In order to ensure it is available you can view a list of taken IDs at
https://people.apache.org/committer-index.html

Please notify us when you have submitted the CLA and by what means
you did so. This will enable us to monitor its progress.

We will arrange for your Apache user account when the CLA has
been recorded.

2. After that is done, please use your ASF email to subscribe to the dev@sedona.apache.org
and private@sedona.apache.org by sending an email to dev-subscribe@sedona.apache.org and
private-subscribe@sedona.apache.org. We generally discuss everything on the dev list and
keep the private@sedona.apache.org list for occasional matters which must be private.

The developer section of the website describes roles within the ASF and provides other
resources:
  https://www.apache.org/foundation/how-it-works.html
  https://www.apache.org/dev/

Just as before you became a committer, participation in any ASF community
requires adherence to the ASF Code of Conduct:
  https://www.apache.org/foundation/policies/conduct.html

Yours,
The Apache Sedona PMC
```

### 创建 ASF 账户

ICLA 提交后，使用 [ASF New Account Request 表单](https://whimsy.apache.org/officers/acreq) 提交申请。Sedona 的 mentor 会请求账号。

Sedona 毕业后，由 PMC 主席（Chair）来发起请求。

### 加入到系统

新 PMC 用其 ASF 账号订阅 Sedona 邮件列表后，PMC 中的某位成员需把新成员加入 Whimsy 系统（https://whimsy.apache.org/roster/pmc/sedona）。

### PMC 公告

账号创建完成后，下面这封邮件用于在 sedona-dev 列表中宣布新 committer：

```
To: dev@sedona.apache.org
Subject: new committer: ###New PMC NAME

The Project Management Committee (PMC) for Apache Sedona
has invited New PMC NAME to become a committer and we are pleased
to announce that they have accepted.

### add specific details here ###

Being a committer enables easier contribution to the
project since there is no need to go via the patch
submission process. This should enable better productivity.
A PMC member helps manage and guide the direction of the project.
```

### Committer Done 模板

在 committer 账号建立完成后：

```
To: New Committer Email
CC: private@sedona.apache.org
Subject: account request: New Committer NAME

New Committer NAME, as you know, the ASF Infrastructure has set up your
committer account with the username '####'.

You have commit access to specific sections of the
ASF repository, as follows:
https://github.com/apache/sedona

You need to link your ASF Account with your GitHub account.

Here are the steps

1. Verify you have a GitHub ID enabled with 2FA
	* https://help.github.com/articles/securing-your-account-with-two-factor-authentication-2fa/
2. Enter your GitHub ID into your Apache ID profile https://id.apache.org/
3. Merge your Apache and GitHub accounts using
	* GitBox (Apache Account Linking utility) https://gitbox.apache.org/setup/
	* You should see 3 green checks in GitBox.
	* Wait at least 30  minutes for an email inviting you to Apache GitHub Organization and accept invitation
4. After accepting the GitHub Invitation verify that you are a
member of the team https://github.com/orgs/apache/teams/sedona-committers

Optionally, if you want, please follow the instructions to set up your GitHub, SSH, svn password, svn configuration, email forwarding, etc.
https://www.apache.org/dev/#committers

Additionally, if you have been elected to the Sedona
 Project Mgmt. Committee (PMC): Verify you are part of the LDAP sedona
  pmc https://whimsy.apache.org/roster/pmc/sedona
```
