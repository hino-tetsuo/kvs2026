# kvs2026
KVS with Tokyo cabinet in mind

# compile and run
Tokyo Cabinet vs 自作KVM ベンチマーク対決

インストール (Mac):
   brew install tokyo-cabinet
 
コンパイル:
   gcc -O3 -o bench_vs bench_vs.c -I/usr/local/include -L/usr/local/lib -ltokyocabinet -lm
 
実行:
   ./bench_vs [件数(デフォルト:100000)]

