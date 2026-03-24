# 测试一般请求
# for i in {1..10}; do curl -k https://127.0.0.1:8443/api/v1/get; echo; done
# 测试 HTTP/3
for i in {1..20}; do curl -s --http3 -k -o /dev/null -w "%{http_code}\n" https://127.0.0.1:8443/api/v1/get; done
