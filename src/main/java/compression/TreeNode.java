package compression;

public class TreeNode {
    public short n;
    public int w;
    public TreeNode left;
    public TreeNode right;

    public TreeNode() {
        n = 0;
        w = 0;
        left = null;
        right = null;
    }

    public TreeNode(short _n, int _w) {
        n = _n;
        w = _w;
        left = null;
        right = null;
    }

    public TreeNode(short _n, int _w, TreeNode _left, TreeNode _right) {
        n = _n;
        w = _w;
        left = _left;
        right = _right;
    }
}

