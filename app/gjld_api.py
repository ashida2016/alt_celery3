"""硅基流动（SiliconFlow）Chat Completion 公用接口封装。

提供公用函数 ``gjld_chat_completion``：通过硅基流动的 OpenAI 兼容
协议，输入任意问题并获得文本回答。API-KEY 从环境变量
``API_KEY_GJLD`` 读取，BASE-URL 从 ``BASE_URL`` 读取，模型从
``GJLD_MODEL`` 读取（均可省略并使用默认值）。
"""

import os
from typing import Any

from openai import OpenAI

# 硅基流动 OpenAI 兼容协议的默认地址
DEFAULT_BASE_URL = "https://api.siliconflow.cn/v1"
DEFAULT_MODEL = "Qwen/Qwen2.5-72B-Instruct"


def _build_client() -> OpenAI:
    """构建硅基流动 API 客户端。

    Returns:
        OpenAI 兼容客户端实例。

    Raises:
        RuntimeError: 未配置 API_KEY_GJLD 环境变量时抛出。
    """
    api_key = os.environ.get("API_KEY_GJLD", "")
    if not api_key:
        raise RuntimeError("环境变量 API_KEY_GJLD 未配置（硅基流动 API-KEY）")
    base_url = os.environ.get("BASE_URL", DEFAULT_BASE_URL)
    return OpenAI(api_key=api_key, base_url=base_url)


def gjld_chat_completion(
    question: str,
    *,
    system: str | None = None,
    model: str | None = None,
    temperature: float = 0.7,
    timeout: float = 60.0,
) -> str:
    """调用硅基流动 Chat Completion 接口，输入任意问题并获得返回值。

    Args:
        question: 用户问题（任意自然语言内容）。
        system: 可选的系统提示词。
        model: 模型名称，缺省读取环境变量 ``GJLD_MODEL``。
        temperature: 采样温度（默认 0.7）。
        timeout: 请求超时秒数（默认 60）。

    Returns:
        模型返回的文本内容。

    Raises:
        RuntimeError: API-KEY 未配置时抛出。
        openai.APIError: 接口调用失败时由 SDK 抛出。
    """
    model_name = model or os.environ.get("GJLD_MODEL", DEFAULT_MODEL)
    messages: list[dict[str, Any]] = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": question})

    client = _build_client()
    response = client.chat.completions.create(
        model=model_name,
        messages=messages,  # type: ignore[arg-type]
        temperature=temperature,
        timeout=timeout,
    )
    return response.choices[0].message.content or ""
