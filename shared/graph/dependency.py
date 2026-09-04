# !/bin/python
from __future__ import annotations

from collections import defaultdict, deque


def parse_job_dependencies(dependencies_str, prefix="33:"):
    if not dependencies_str:
        return []

    result = []
    seen = set()
    for item in str(dependencies_str).split("|"):
        item = item.strip()
        if not item.startswith(prefix):
            continue
        job_name = item[len(prefix) :].strip()
        if not job_name or job_name in seen:
            continue
        seen.add(job_name)
        result.append(job_name)
    return result


def build_dependency_graph(jobs):
    graph = defaultdict(list)
    for current_job, dependencies_str in jobs:
        current_job = "" if current_job is None else str(current_job).strip()
        if not current_job:
            continue

        deps = parse_job_dependencies(dependencies_str)
        graph.setdefault(current_job, [])
        for dep in deps:
            graph[current_job].append(dep)
            graph.setdefault(dep, [])
    return graph


def find_job_dependency_cycles(jobs, only_nodes=None, max_cycles=20):
    graph = build_dependency_graph(jobs)
    return find_cycles(graph, only_nodes=only_nodes, max_cycles=max_cycles)


def _cycle_key(cycle):
    nodes = cycle[:-1]
    if not nodes:
        return tuple(cycle)

    rotations = [tuple(nodes[i:] + nodes[:i]) for i in range(len(nodes))]
    return min(rotations)


def find_cycles(graph, only_nodes=None, max_cycles=20):
    only_nodes = set(only_nodes or [])
    status = {}
    stack = []
    stack_index = {}
    cycles = []
    cycle_keys = set()

    for start_node in graph:
        if status.get(start_node, 0) != 0:
            continue

        status[start_node] = 1
        stack.append(start_node)
        stack_index[start_node] = 0
        dfs_stack = [(start_node, iter(graph.get(start_node, [])))]

        while dfs_stack:
            node, neighbors = dfs_stack[-1]
            try:
                neighbor = next(neighbors)
            except StopIteration:
                dfs_stack.pop()
                status[node] = 2
                stack_index.pop(node, None)
                stack.pop()
                continue

            neighbor_status = status.get(neighbor, 0)
            if neighbor_status == 0:
                status[neighbor] = 1
                stack_index[neighbor] = len(stack)
                stack.append(neighbor)
                dfs_stack.append((neighbor, iter(graph.get(neighbor, []))))
                continue

            if neighbor_status != 1:
                continue

            cycle = stack[stack_index[neighbor] :] + [neighbor]
            cycle_nodes = set(cycle[:-1])
            if only_nodes and not cycle_nodes.intersection(only_nodes):
                continue

            key = _cycle_key(cycle)
            if key in cycle_keys:
                continue

            cycle_keys.add(key)
            cycles.append(cycle)
            if max_cycles and len(cycles) >= max_cycles:
                return cycles

    return cycles


def build_reverse_dependency_graph(jobs):
    graph = defaultdict(list)
    for current_job, dependencies_str in jobs:
        if dependencies_str:
            deps = [
                dep.split(":", 1)[1]
                for dep in dependencies_str.split("|")
                if ":" in dep
            ]
        else:
            deps = []
        for dep in deps:
            graph[dep].append(current_job)
    return graph


def find_all_dependent_jobs(start_job, graph):
    if start_job not in graph:
        return []
    result = []
    queue = deque((job, 1) for job in graph[start_job])
    visited = set()
    while queue:
        job, level = queue.popleft()
        if job in visited:
            continue
        visited.add(job)
        result.append((job, level))
        for next_job in graph.get(job, []):
            if next_job not in visited:
                queue.append((next_job, level + 1))
    return result
