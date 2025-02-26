# 微服务实现
这是一个通过 github.com/doublemo/nakama-kit包，实现的微服务程序

# 如何访问
运行nakama主程序后，通过浏览器http://localhost:7350/v2/any/服务角色名称/所要调用的方法

本例子[GET]:
```sh
http://localhost:7350/v2/any/say_hello/test?http_key=http_key_value
```
```json
{
    "header": {
        "cid": "chat",
        "name": "say_hello"
    },
    "stringContent": "99998888"
}
```


POST
```sh
curl -X POST -k http://localhost:7350/v2/any/say_hello/test -d '{"query":{"http_key":{"value":["http_key_value"]}}}'
```

```json
{
    "header": {
        "cid": "chat",
        "name": "say_hello"
    },
    "stringContent": "99998888"
}
```