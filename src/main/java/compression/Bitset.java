package compression;

public class Bitset {
    public byte[] _data = null;
    public byte _rbuf;
    public int _rpos;

    public Bitset() {
        _rbuf = 0;
        _rpos = 0;
    }

    public int read() {
        byte buf = _data[_rpos / 8];
        int res = (buf >>> (7 - (_rpos % 8))) & 1;
        _rpos += 1;
        return res;
    }

    public void clear() {
        _data = null;
    }
}
