package com.lineage.demo.components;

import java.util.*;

public class LineageTopologyBuilder {

    // 节点类：表示表中的一个字段
    public static class Node {
        private final String tableName;
        private final String columnName;

        public Node(String tableName, String columnName) {
            this.tableName = tableName;
            this.columnName = columnName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getColumnName() {
            return columnName;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Node node = (Node) obj;
            return Objects.equals(tableName, node.tableName) &&
                    Objects.equals(columnName, node.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableName, columnName);
        }

        @Override
        public String toString() {
            return tableName + "." + columnName;
        }
    }

    // 边类：表示数据流动的方向
    public static class Edge {
        private final Node source;
        private final Node target;

        public Edge(Node source, Node target) {
            this.source = source;
            this.target = target;
        }

        public Node getSource() {
            return source;
        }

        public Node getTarget() {
            return target;
        }

        @Override
        public String toString() {
            return source.toString() + " -> " + target.toString();
        }
    }

    // 图的邻接表表示法
    private final Map<Node, List<Edge>> adjacencyList = new HashMap<>();

    // 添加节点
    public void addNode(Node node) {
        adjacencyList.putIfAbsent(node, new ArrayList<>());
    }

    // 添加边
    public void addEdge(Node source, Node target) {
        addNode(source);
        addNode(target);
        adjacencyList.get(source).add(new Edge(source, target));
    }

    // 获取节点的所有出边
    public List<Edge> getEdges(Node node) {
        return adjacencyList.getOrDefault(node, new ArrayList<>());
    }

    // 打印拓扑结构
    public void printTopology() {
        for (Map.Entry<Node, List<Edge>> entry : adjacencyList.entrySet()) {
            for (Edge edge : entry.getValue()) {
                System.out.println(edge);
            }
        }
    }

    // 生成 Graphviz DOT 格式的拓扑结构
    public String toDotFormat() {
        StringBuilder dotBuilder = new StringBuilder("digraph LineageGraph {\n");
        for (Map.Entry<Node, List<Edge>> entry : adjacencyList.entrySet()) {
            for (Edge edge : entry.getValue()) {
                dotBuilder.append("  \"")
                        .append(edge.getSource())
                        .append("\" -> \"")
                        .append(edge.getTarget())
                        .append("\";\n");
            }
        }
        dotBuilder.append("}");
        return dotBuilder.toString();
    }
}

