from typing import List, Dict, Any

def calculate_node_weight(node: Any) -> float:
    """
    Computes a composite capacity score for a node.
    - Base weight: CPU cores
    - Device multiplier: CUDA (2.5x), Metal (2.0x), CPU (1.0x)
    """
    device_type = getattr(node, "device_type", "cpu") or "cpu"
    cores = getattr(node, "cores", 4) or 4
    ram_gb = getattr(node, "ram_gb", 8.0) or 8.0

    device_multiplier = 1.0
    if str(device_type).lower() == "cuda":
        device_multiplier = 2.5
    elif str(device_type).lower() == "metal":
        device_multiplier = 2.0

    score = (cores * device_multiplier) + (ram_gb * 0.1)
    return max(1.0, float(score))


def create_proportional_chunks(prompts: List[str], active_nodes: List[Any]) -> List[Dict[str, Any]]:
    """
    Splits prompts into weighted slices matching active worker capacities.
    """
    total_tasks = len(prompts)
    if total_tasks == 0:
        return []

    # Fallback if no nodes are online: create uniform chunks of 5
    if not active_nodes:
        chunk_size = 5
        return [
            {"target_node_id": None, "prompts": prompts[i:i + chunk_size]}
            for i in range(0, total_tasks, chunk_size)
        ]

    # Calculate total fleet weight
    weights = [calculate_node_weight(node) for node in active_nodes]
    total_weight = sum(weights) or 1.0

    chunks: List[Dict[str, Any]] = []
    cursor = 0

    for idx, node in enumerate(active_nodes):
        node_id = getattr(node, "node_id", None)
        
        # Calculate fair share for this node
        fraction = weights[idx] / total_weight
        assigned_count = int(round(fraction * total_tasks))

        # Guarantee the final node gets all remaining items
        if idx == len(active_nodes) - 1:
            assigned_count = total_tasks - cursor

        if assigned_count > 0 and cursor < total_tasks:
            slice_prompts = prompts[cursor:cursor + assigned_count]
            chunks.append({
                "target_node_id": node_id,
                "prompts": slice_prompts
            })
            cursor += assigned_count

    # If any prompts remain unassigned due to edge-case rounding, package them
    if cursor < total_tasks:
        chunks.append({
            "target_node_id": getattr(active_nodes[0], "node_id", None),
            "prompts": prompts[cursor:]
        })

    return chunks