读写接口透明，对数据分块进行数据 checksum

调用的时候，需要使用 f *os.File 作为 NewWriter 的参数传入，封装了一层 flush

本地缓存一个 blocksize 大小，写满就加上 checksum 写下去，未满就先放在缓存中