// PDbf.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "PDbf.h"

using namespace std;

int main()
{
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
    return 0;
}