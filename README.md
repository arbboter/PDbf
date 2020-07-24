# PDbf
Header-only版本的高性能DBF文件读写库，支持文件创建、读取和写入，使用内存缓存操作

## 特性
1.支持DBF格式文件批量读写的C++库，使用原始的STL编写，使用C++标准语法。
2.每次读取和写入均使用行缓存，如果文件小或者机器硬件足够，全部操作可以直接在内存中完成，性能高。
3.其中`CIDbf`为虚类，列出了主要使用的函数接口，并封装了三个字符串去空白操作函数
4.其中`CPDbf`为主要类，实现了DBF的各种操作，使用时使用该类即可
5.其中`CCMPDbf`为附带类，使用`CPDbf`实现DBF文件对比
6.类`CPDbf`支持相同格式的DBF文件合并，函数为`int Append(CPDbf& oDbf)`

## 示例代码
1.创建DBF文件
```cpp
// 构建表头
char szBuf[128] = { 0 };
TDbfField oField;
vector<TDbfField> vecHeader;
memset(&oField, 0, sizeof(oField));
for (size_t i=0; i<4; i++)
{
    memset(szBuf, 0, sizeof(szBuf));
    sprintf(oField.szName, "Field_%lu", i + 1);
    oField.cLength = rand() % 16 + 1;
    oField.cType = (i & 1) ? 'N' : 'C';
    vecHeader.push_back(oField);
}

// 创建DBF文件
CPDbf oDbf;
string strFile = "test.dbf";
if (oDbf.Create(strFile, vecHeader))
{
    printf("创建DBF文件失败\n");
    return 0;
}
```
2.批量写入数据
```cpp
// 写入数据
size_t nNum = 32;
if (oDbf.PrepareAppend(nNum))
{
    printf("申请预写入数据失败\n");
    return 0;
}
for (size_t i = 0; i < nNum; i++)
{
    if (oDbf.WriteGo(i))
    {
        printf("跳转缓存行写指针失败\n");
        return 0;
    }
    for (size_t j=0; j<vecHeader.size(); j++)
    {
        memset(szBuf, 0, sizeof(szBuf));
        if (vecHeader[j].cType == 'N')
        {
            sprintf(szBuf, "%lu", i*10000 + j);
        }
        else
        {
            sprintf(szBuf, "V%lu_%lu", i, j);
        }
        if (oDbf.WriteString(j, szBuf))
        {
            printf("行写入字段失败\n");
            return 0;
        }
    }
}

// 写入数据提交
oDbf.WriteCommit();
oDbf.Close();
```
3.批量读取数据
```cpp
// 读取文件
if (oDbf.Open(strFile))
{
    printf("打开DBF文件失败\n");
    return 0;
}
// 读取字段头
vecHeader = oDbf.GetField();
for (size_t i=0; i< vecHeader.size(); i++)
{
    printf("%-20s ", vecHeader[i].szName);
}
printf("\n");
// 读取记录
nNum = 10;
string strField;
size_t nRecNum = oDbf.GetRecNum();
for (size_t i = nNum; i < nRecNum; i+=nNum)
{
    // 读取DBF文件缓存行
    nNum = MMin(nNum, nRecNum-i);
    if (oDbf.Read(i, nNum))
    {
        printf("读取DBF文件缓存行失败\n");
        return 0;
    }
    // 按行从缓存获取数据
    for (size_t j=0; j<nNum; j++)
    {
        // 跳转到缓存行
        if (oDbf.ReadGo(j))
        {
            printf("设置DBF文件缓存行读取指针失败\n");
            return 0;
        }
        // 读取行字段
        for (size_t k=0; k<vecHeader.size(); k++)
        {
            if (oDbf.ReadString(k, strField))
            {
                printf("读取行字段数据失败\n");
                return 0;
            }
            printf("%-20s ", strField.c_str());
        }
        printf("\n");
    }
}
// 关闭文件
oDbf.Close();
```

## 测试代码
`test.cpp`为测试代码，执行结果如下：
```bash
$g++ test.cpp
$./a.out
Field_1              Field_2              Field_3              Field_4
V10_0                100001               V10_2                1000
V11_0                110001               V11_2                1100
V12_0                120001               V12_2                1200
V13_0                130001               V13_2                1300
V14_0                140001               V14_2                1400
V15_0                150001               V15_2                1500
V16_0                160001               V16_2                1600
V17_0                170001               V17_2                1700
V18_0                180001               V18_2                1800
V19_0                190001               V19_2                1900
V20_0                200001               V20_2                2000
V21_0                210001               V21_2                2100
V22_0                220001               V22_2                2200
V23_0                230001               V23_2                2300
V24_0                240001               V24_2                2400
V25_0                250001               V25_2                2500
V26_0                260001               V26_2                2600
V27_0                270001               V27_2                2700
V28_0                280001               V28_2                2800
V29_0                290001               V29_2                2900
V30_0                300001               V30_2                3000
V31_0                310001               V31_2                3100
```
