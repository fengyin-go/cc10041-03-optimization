"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class StreamSourceService(BaseModuleService):
    MODULE = "stream_source"
    ENTITY = "流式源"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["源名称", "消息队列主题", "序列化格式", "消费者组", "启动偏移"]
    REQUIRED_FIELDS = ["源名称", "消息队列主题", "序列化格式"]
    STATUS_ORDER = ["待验证", "已验证", "已暂停"]
    ACTION_RULES = {"验证连通": "已验证", "暂停消费": "已暂停"}
    NEGATIVE_ACTIONS = []
