### q1 
command 
```bash
docker exec redpanda-1 rpk version  
```
output
```
v22.3.5 (rev 28b2443)
```

### q2
command
```bash
docker exec redpanda-1 rpk topic create test-topic 
```
output
```
TOPIC       STATUS
test-topic  OK
```