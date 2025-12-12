"""
Message processing module
Processes Claude Code history, merges consecutive user messages, ensures Amazon Q format compliance
"""
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def merge_user_messages(user_messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge multiple userInputMessage contents

    Args:
        user_messages: List of userInputMessage

    Returns:
        Merged userInputMessage
    """
    if not user_messages:
        return {}

    all_contents = []
    base_context = None
    base_origin = None
    base_model = None
    all_images = []
    all_tool_results = []

    for msg in user_messages:
        content = msg.get("content", "")
        msg_ctx = msg.get("userInputMessageContext", {})

        # Preserve first message's context info
        if base_context is None:
            base_context = msg_ctx.copy() if msg_ctx else {}
            # Remove toolResults from base to merge them separately
            if "toolResults" in base_context:
                all_tool_results.extend(base_context.pop("toolResults"))
        else:
            # Collect toolResults from subsequent messages
            if "toolResults" in msg_ctx:
                all_tool_results.extend(msg_ctx["toolResults"])

        # Preserve first message's origin
        if base_origin is None:
            base_origin = msg.get("origin", "KIRO_CLI")

        # Preserve first message's modelId
        if base_model is None and "modelId" in msg:
            base_model = msg["modelId"]

        # Add content (preserve all content including system-reminder)
        if content:
            all_contents.append(content)

        # Collect images
        msg_images = msg.get("images")
        if msg_images:
            all_images.append(msg_images)

    # Merge content with double newline separator
    merged_content = "\n\n".join(all_contents)

    # Build merged message
    merged_msg = {
        "content": merged_content,
        "userInputMessageContext": base_context or {},
        "origin": base_origin or "KIRO_CLI"
    }

    # Add merged toolResults if any
    if all_tool_results:
        merged_msg["userInputMessageContext"]["toolResults"] = all_tool_results

    # Preserve modelId if present
    if base_model:
        merged_msg["modelId"] = base_model

    # Only keep images from the last 2 messages that have images
    if all_images:
        kept_images = []
        for img_list in all_images[-2:]:
            kept_images.extend(img_list)
        if kept_images:
            merged_msg["images"] = kept_images

    return merged_msg


def process_history_for_amazonq(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process Claude Code history to comply with Amazon Q requirements

    Strategy:
    1. Merge consecutive userInputMessage
    2. Preserve all content (including system-reminder)
    3. Ensure strict user-assistant message alternation

    Args:
        history: Claude Code history

    Returns:
        Processed history compliant with Amazon Q format
    """
    if not history:
        return []

    processed_history = []
    pending_user_messages = []

    for idx, msg in enumerate(history):
        if "userInputMessage" in msg:
            # Collect consecutive user messages
            pending_user_messages.append(msg["userInputMessage"])
            logger.debug(f"[MESSAGE_PROCESSOR] Message {idx}: collecting userInputMessage, pending count: {len(pending_user_messages)}")

        elif "assistantResponseMessage" in msg:
            # When encountering assistant message, first merge previous user messages
            if pending_user_messages:
                logger.info(f"[MESSAGE_PROCESSOR] Message {idx}: merging {len(pending_user_messages)} userInputMessage(s)")
                merged_user_msg = merge_user_messages(pending_user_messages)
                processed_history.append({
                    "userInputMessage": merged_user_msg
                })
                pending_user_messages = []

            # Add assistant message
            logger.debug(f"[MESSAGE_PROCESSOR] Message {idx}: adding assistantResponseMessage")
            processed_history.append(msg)

    # Process remaining user messages at the end
    if pending_user_messages:
        logger.info(f"[MESSAGE_PROCESSOR] Processing {len(pending_user_messages)} remaining userInputMessage(s) at end")
        merged_user_msg = merge_user_messages(pending_user_messages)
        processed_history.append({
            "userInputMessage": merged_user_msg
        })

    logger.info(f"[MESSAGE_PROCESSOR] History processing complete: {len(history)} -> {len(processed_history)} messages")

    # Validate message alternation
    try:
        validate_message_alternation(processed_history)
    except ValueError as e:
        logger.error(f"[MESSAGE_PROCESSOR] Message alternation validation failed: {e}")
        raise

    return processed_history


def merge_duplicate_tool_results(tool_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge duplicate toolResults with the same toolUseId

    Args:
        tool_results: List of tool results

    Returns:
        Merged tool results list
    """
    if not tool_results:
        return []

    merged_tool_results = []
    seen_tool_use_ids = set()

    for result in tool_results:
        tool_use_id = result.get("toolUseId")
        if tool_use_id in seen_tool_use_ids:
            # Find existing entry and merge content
            for existing in merged_tool_results:
                if existing.get("toolUseId") == tool_use_id:
                    existing["content"].extend(result.get("content", []))
                    logger.info(f"[MESSAGE_PROCESSOR] Merged duplicate toolUseId {tool_use_id}")
                    break
        else:
            # New entry
            seen_tool_use_ids.add(tool_use_id)
            merged_tool_results.append(result)

    return merged_tool_results


def validate_message_alternation(history: List[Dict[str, Any]]) -> bool:
    """
    Validate that messages strictly alternate (user-assistant-user-assistant...)

    Args:
        history: History records

    Returns:
        Whether valid

    Raises:
        ValueError: If messages don't alternate
    """
    if not history:
        return True

    last_role = None

    for idx, msg in enumerate(history):
        if "userInputMessage" in msg:
            current_role = "user"
        elif "assistantResponseMessage" in msg:
            current_role = "assistant"
        else:
            logger.warning(f"[MESSAGE_PROCESSOR] Message {idx} is neither user nor assistant: {list(msg.keys())}")
            continue

        if last_role == current_role:
            error_msg = f"Message {idx} violates alternation rule: consecutive {current_role} messages"
            logger.error(f"[MESSAGE_PROCESSOR] {error_msg}")
            logger.error(f"[MESSAGE_PROCESSOR] Previous message: {list(history[idx-1].keys())}")
            logger.error(f"[MESSAGE_PROCESSOR] Current message: {list(msg.keys())}")
            raise ValueError(error_msg)

        last_role = current_role

    logger.info("[MESSAGE_PROCESSOR] Message alternation validation passed")
    return True


def log_history_summary(history: List[Dict[str, Any]], prefix: str = ""):
    """
    Log history summary for debugging

    Args:
        history: History records
        prefix: Log prefix
    """
    if not history:
        logger.info(f"{prefix}History is empty")
        return

    summary = []
    for idx, msg in enumerate(history):
        if "userInputMessage" in msg:
            content = msg["userInputMessage"].get("content", "")
            content_preview = content[:80].replace("\n", " ") if content else ""
            summary.append(f"  [{idx}] USER: {content_preview}...")
        elif "assistantResponseMessage" in msg:
            content = msg["assistantResponseMessage"].get("content", "")
            content_preview = content[:80].replace("\n", " ") if content else ""
            summary.append(f"  [{idx}] ASSISTANT: {content_preview}...")

    logger.info(f"{prefix}History summary ({len(history)} messages):\n" + "\n".join(summary))
