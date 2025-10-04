from typing import Dict, List

def get_seeding_order(seed_config: Dict) -> List[str]:
    """
    Sử dụng thuật toán sắp xếp Topo (biến thể của Kahn's algorithm)
    để xác định thứ tự seed đúng từ file config.
    """
    # 1. Xây dựng đồ thị phụ thuộc
    # a. Adjacency list: Ai phụ thuộc vào mình
    # Ví dụ: adj['users'] = ['orders'] (orders phụ thuộc vào users)
    adj: Dict[str, List[str]] = {table: [] for table in seed_config}
    
    # b. In-degree: Mình đang phụ thuộc vào bao nhiêu thằng
    # Ví dụ: in_degree['users'] = 0, in_degree['orders'] = 1
    in_degree: Dict[str, int] = {table: 0 for table in seed_config}

    for table_name, table_config in seed_config.items():
        relations = table_config.get("relations", {})
        for fk_col, rel_info in relations.items():
            parent_table = rel_info.get("table")
            if parent_table in adj:
                adj[parent_table].append(table_name)
                in_degree[table_name] += 1

    # 2. Tìm các node có in-degree = 0 (các bảng không phụ thuộc vào ai)
    queue = [table for table, degree in in_degree.items() if degree == 0]
    
    # 3. Bắt đầu sắp xếp
    sorted_order = []
    while queue:
        current_table = queue.pop(0)
        sorted_order.append(current_table)

        # Giảm in-degree của các bảng phụ thuộc vào nó
        for dependent_table in adj.get(current_table, []):
            in_degree[dependent_table] -= 1
            # Nếu một bảng hết phụ thuộc, cho nó vào hàng đợi
            if in_degree[dependent_table] == 0:
                queue.append(dependent_table)

    # 4. Kiểm tra chu trình (cycle)
    # Nếu đồ thị có chu trình (A -> B -> A), không thể sắp xếp được
    if len(sorted_order) != len(seed_config):
        raise ValueError("A cyclic dependency was detected in the seed configuration. Please check your 'relations'.")

    return sorted_order