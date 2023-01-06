package org.apache.iotdb.warehouse.server.query.optimize;

import java.util.Map;

public class PlanTree {

    Map<Integer, String> id2seriesName;
    Node root;

    public PlanTree() {}


    class Node1 {
        int node_id;
        String op_name;
        int from, to;
        Node1(int id, String name, int to_id) {
            node_id = id; op_name=name; to = to_id;
        }
        void setFrom(int from_id) {from = from_id; }
    }

    class Node {
        Node1 left_node, right_node;
        Node(Node1 left, Node1 right) {
            left_node = left; right_node = right;
        }
    }
}
