package compression;

import java.util.ArrayList;
import java.util.List;

public class TreeNodeHeap {
    private List<TreeNode> _data = new ArrayList<TreeNode>();
    public int size() {
        return _data.size();
    }
    public void push(TreeNode s) {
        _data.add(s);
        heapUp(_data.size() - 1);
    }

    public TreeNode pop() {
        TreeNode result = null;
        result = _data.get(0);
        _data.set(0, _data.get(_data.size() - 1));
        _data.remove(_data.size() - 1);
        heapDown(0);
        return result;
    }

    private void heapUp(int idx) {
        if (idx < 0 || idx >= _data.size()) {
            return;
        }
        int cur = idx;
        int parent = (cur - 1) / 2;
        TreeNode item = _data.get(cur);
        while (cur > 0 && item.w < _data.get(parent).w) {
            _data.set(cur, _data.get(parent));
            cur = parent;
            parent = (cur - 1) / 2;
        }
        _data.set(cur, item);
    }

    private void heapDown(int idx) {
        if (idx < 0 || idx >= _data.size()) {
            return;
        }
        int cur = idx;
        TreeNode item = _data.get(cur);
        int child = cur * 2 + 1; // left child
        while (child < _data.size()) {
            // right child
            if (child + 1 < _data.size() && _data.get(child).w > _data.get(child + 1).w) {
                child = child + 1;
            }
            if (item.w <= _data.get(child).w) {
                break;
            }
            _data.set(cur, _data.get(child));
            cur = child;
            child = cur * 2 + 1;
        }
        _data.set(cur, item);
    }
}
