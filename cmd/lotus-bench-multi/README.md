
```cassandraql
#使用说明：
#编译：
make bench-multi

#使用--save-sectorinfo命令会将bench整个过程生成的所有文件和各个步骤需要的参数保存在固定目录：/opt/lotus/bench-save/中.例如：
./bench-multi sealing --sector-size 512MB --save-sectorinfo

# 执行过--save-sectorinfo命令之后，后续就可以使用--only-precommit1和only-post等等命令只做某一步计算。例如：
./bench-multi sealing --sector-size 512MB --only-post

```

