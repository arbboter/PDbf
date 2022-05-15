/*
 * Copyright 2020 Arbboter email:arbboter@gmail.com
 *
 *     #    ######  ######  ######  ####### ####### ####### ######  
 *    # #   #     # #     # #     # #     #    #    #       #     # 
 *   #   #  #     # #     # #     # #     #    #    #       #     # 
 *  #     # ######  ######  ######  #     #    #    #####   ######  
 *  ####### #   #   #     # #     # #     #    #    #       #   #   
 *  #     # #    #  #     # #     # #     #    #    #       #    #  
 *  #     # #     # ######  ######  #######    #    ####### #     # 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __P_DBF_H__
#define __P_DBF_H__
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <map>
#include <assert.h>
#include <time.h>

#define MMax(a, b) ( (a)>(b) ? (a):(b) )
#define MMin(a, b) ( (a)<(b) ? (a):(b) )

// DBF接口
class CIDbf
{
public:
    // 错误码
    enum ERetCode
    {
        DBF_SUCC,           // 成功
        DBF_ERROR,          // 错误
        DBF_FILE_ERROR,     // 文件错误
        DBF_PARA_ERROR,     // 参数错误
        DBF_CACHE_ERROR,    // 缓存错误
    };
    // 判断文件是否打开
    virtual bool IsOpen() = 0;

    // 打开文件
    virtual int Open(const std::string& strFile, bool bReadOnly = true) = 0;

    // 关闭DBF文件
    virtual void Close() = 0;

    // 文件记录行
    virtual size_t GetRecNum() = 0;
    // 字段数
    virtual size_t GetFieldNum() = 0;

    // 直接操作文件，性能低，记录号首行为0
    virtual std::string ReadString(size_t nRecNo, const std::string& strName) = 0;
    virtual int WriteString(size_t nRecNo, const std::string& strName, const std::string& strValue) = 0;
    virtual int ReadField(size_t nRecNo, size_t nCol, std::string& strValue) = 0;
    virtual int Append(size_t nAppendNum) = 0;
    virtual int WriteField(size_t nRecNo, size_t nCol, const std::string& strValue) = 0;

    // 缓存处理，性能高
    // 读取文件记录
    // 读取记录行到缓存
    virtual int Read(int nRecNo, int nRecNum) = 0;
    // 设置读指针, 从0开始, 缓存记录行行数
    virtual int ReadGo(int nRec) = 0;
    // 读取字段
    virtual std::string ReadString(const std::string& strName) = 0;
    virtual double ReadDouble(const std::string& strName) = 0;
    virtual int ReadInt(const std::string& strName) = 0;
    virtual long ReadLong(const std::string& strName) = 0;
    virtual int ReadString(const std::string& strName, std::string& strValue) = 0;
    virtual int ReadDouble(const std::string& strName, double& fValue) = 0;
    virtual int ReadInt(const std::string& strName, int& nValue) = 0;
    virtual int ReadLong(const std::string& strName, long& nValue) = 0;
    virtual int ReadString(size_t nCol, std::string& strValue) = 0;
    virtual int ReadDouble(size_t nCol, double& fValue) = 0;
    virtual int ReadInt(size_t nCol, int& nValue) = 0;
    virtual int ReadLong(size_t nCol, long& nValue) = 0;

    // 写文件记录
    // 清空数据
    virtual int Zap() = 0;
    // 申请写入数据
    virtual int PrepareAppend(size_t nRecNum) = 0;
    // 提交写数据数据到文件
    virtual int WriteCommit() = 0;
    // 提交写入的数据到文件
    virtual int FileCommit() = 0;
    // 设置写指针, 从0开始, 缓存记录行行数
    virtual int WriteGo(int nRec) = 0;
    // 根据字段名写字段数据
    virtual int WriteString(const std::string& strName, const std::string& strValue) = 0;
    virtual int WriteDouble(const std::string& strName, double fValue) = 0;
    virtual int WriteInt(const std::string& strName, int nValue) = 0;
    virtual int WriteLong(const std::string& strName, long nValue) = 0;
    // 根据索引写
    virtual int WriteString(size_t nCol, const std::string& strValue) = 0;
    virtual int WriteDouble(size_t nCol, double fValue) = 0;
    virtual int WriteInt(size_t nCol, int nValue) = 0;
    virtual int WriteLong(size_t nCol, long nValue) = 0;

    // 字符串函数
    static std::string Ltrim(const std::string& s)
    {
        size_t nPos = 0;
        for (nPos = 0; nPos < s.size(); nPos++)
        {
            if (!isspace(s[nPos]))
            {
                break;
            }
        }
        return s.substr(nPos);
    }
    static std::string Rtrim(const std::string& s)
    {
        size_t nPos = 0;
        for (nPos = s.size(); nPos > 0; nPos--)
        {
            if (!isspace(s[nPos - 1]))
            {
                break;
            }
        }
        return s.substr(0, nPos);
    }
    static std::string Trim(const std::string& s)
    {
        return Ltrim(Rtrim(s));
    }
public:
    virtual ~CIDbf() {}

};

// DBF文件类型
enum EFV
{
    FV_NS = 0x00,       // 不确定的文件类型
    FV_FB2 = 0x02,      // FoxBASE
    FV_FB3 = 0x03,      // FoxBASE + / Dbase III plus, no memo
    FV_VFP = 0x30,      // Visual FoxPro
    FV_VFPAI = 0x31,    // Visual FoxPro, autoincrement enabled
    FV_D4TABLE = 0x43,  // dBASE IV SQL table files, no memo
    FV_D4FILE = 0x63,   // dBASE IV SQL system files, no memo
    FV_MD3P = 0x83,     // FoxBASE + / dBASE III PLUS, with memo
    FV_MD4 = 0x8B,      // dBASE IV with memo
    FV_MD4TABLE = 0xCB, // dBASE IV SQL table files, with memo
    FV_MFP2 = 0xF5,     // FoxPro 2.x(or earlier) with memo
    FV_FP = 0xFB,       // FoxBASE
};
// DBF头(32字节)
class TDbfHeader
{
public:
    char            cVer;           // 版本标志
    unsigned char   cYy;            // 最后更新年
    unsigned char   cMm;            // 最后更新月
    unsigned char   cDd;            // 最后更日
    unsigned int    nRecNum;        // 文件包含的总记录数
    unsigned short  nHeaderLen;     // 文件头长度
    unsigned short  nRecLen;        // 记录长度
    char            szReserved[20]; // 保留

    TDbfHeader()
    {
        assert(sizeof(*this) == 32);
        cVer = FV_NS;
        cYy = 0;
        cMm = 0;
        cDd = 0;
        nRecNum = 0;
        nHeaderLen = 0;
        nRecLen = 0;
        memset(szReserved, 0, sizeof(szReserved));
    }
};
// 字段
class TDbfField
{
public:
    // 字段名
    char szName[11];
    // 字段类型，是ASCII码值
    // B-二进制 C-字符型 D-日期类型YYYYMMDD G-各种字符 N-数值型 L-逻辑性 M-各种字符
    unsigned char cType;
    // 保留字节，用于以后添加新的说明性信息时使用，这里用0来填写
    unsigned int nReserved1;
    // 记录项长度，二进制型
    unsigned char cLength;
    // 记录项的精度，二进制型
    unsigned char cPrecisionLength;
    // 保留字节，用于以后添加新的说明性信息时使用，这里用0来填写
    // 本程序在打开文件时，记录对应字段的其实偏移值
    unsigned short nPosition;
    // 工作区ID
    unsigned char cWorkId;
    // 保留字节，用于以后添加新的说明性信息时使用，这里用0来填写
    char szReserved[10];
    // MDX标识。如果存在一个MDX 格式的索引文件，那么这个记录项为真，否则为空
    char cMdxFlag;

    TDbfField()
    {
        assert(sizeof(*this) == 32);
        memset(szName, 0, sizeof(szName));
        cType = 0;
        nReserved1 = 0;
        cLength = 0;
        cPrecisionLength = 0;
        nPosition = 0;
        cWorkId = 0;
        memset(szReserved, 0, sizeof(szReserved));
        cMdxFlag = 0;
    }
};
// 记录行缓存
class CRecordBuf
{
public:
    CRecordBuf(size_t nRecCapacity, size_t nRecLen)
    {
        m_nRecNum = 0;
        m_nRecCapacity = nRecCapacity;
        m_nRecLen = nRecLen;
        size_t nSize = m_nRecCapacity * m_nRecLen;
        m_pBuf = new char[nSize];
        memset(m_pBuf, ' ', nSize);
        m_nCurRec = 0;
    }
    ~CRecordBuf()
    {
        delete[] m_pBuf;
        m_pBuf = NULL;
    }

    // 返回记录数
    inline size_t Size()
    {
        return m_nRecNum;
    }

    // 数据区内存大小
    inline size_t DataSize()
    {
        return RecNum() * RecLen();
    }

    // 追加记录行
    inline bool Push(char* pRec, int nNum)
    {
        if (nNum + m_nRecNum > m_nRecCapacity)
        {
            return false;
        }
        memcpy(m_pBuf, pRec, nNum * m_nRecLen);
        return true;
    }

    // 获取记录行
    char* At(size_t nIdx)
    {
        if (nIdx >= m_nRecNum)
        {
            return NULL;
        }
        return &m_pBuf[nIdx * m_nRecLen];
    }

    // 修改对象大小
    void Resize(size_t nNum)
    {
        // 减小
        if (nNum < m_nRecCapacity)
        {
            m_nRecCapacity = nNum;
        }
        if (nNum < m_nRecNum)
        {
            m_nRecNum = nNum;
        }
        // 扩大
        if (nNum > m_nRecCapacity)
        {
            char* pBuf = new char[nNum * m_nRecLen];
            memset(pBuf, 0, nNum * m_nRecLen);
            memcpy(pBuf, m_pBuf, m_nRecCapacity*m_nRecLen);
            m_nRecCapacity = nNum;
            // 内存处理
            delete m_pBuf;
            m_pBuf = pBuf;
        }
    }

    // 是否为空
    bool IsEmpty()
    {
        return m_nRecNum == 0;
    }
    // 返回缓存大小
    size_t BufSize()
    {
        return m_nRecCapacity * m_nRecLen;
    }
    // 设置当前行，从0开始
    inline bool ReadGo(size_t nRec)
    {
        if (nRec >= m_nRecNum)
        {
            return false;
        }
        m_nCurRec = nRec;
        return true;
    }
    // 设置当前行，从0开始
    inline bool WriteGo(size_t nRec)
    {
        if (nRec >= m_nRecCapacity)
        {
            return false;
        }
        m_nCurRec = nRec;
        m_nRecNum = MMax(nRec + 1, m_nRecNum);
        return true;
    }
    inline void WriteReset()
    {
        m_nCurRec = 0;
        m_nRecNum = 0;
    }
    // 获取当前行数据
    char* GetCurRow()
    {
        return m_pBuf + m_nCurRec * m_nRecLen;
    }

    // 返回数据
    inline char* Data() { return m_pBuf; }
    inline size_t& RecNum() { return m_nRecNum; }
    inline size_t  RecLen() { return m_nRecLen; }
private:
    // 当前记录数
    size_t m_nRecNum;
    // 每条记录长度
    size_t m_nRecLen;
    // 可容纳记录数
    size_t m_nRecCapacity;
    // 数据
    char* m_pBuf;
    // 当前数据行
    size_t m_nCurRec;
};

// 定义跨平台函数
#ifdef _WIN32
#define ws_fopen fopen_s
#define ws_localtime localtime_s
#else
int ws_fopen(FILE** _Stream, char const* _FileName, char const* _Mode)
{
    *_Stream = fopen(_FileName, _Mode);
    if (*_Stream)
    {
        return 0;
    }
    return -1;
}
int ws_localtime(struct tm* const _Tm, time_t const* const _Time)
{
    *_Tm = *localtime(_Time);
    return 0;
}
#define sprintf_s sprintf
#endif

class CPDbf : public CIDbf
{
public:
    // 构造函数
    CPDbf()
        :m_cBlank(' ')
    {
        // 文件句柄
        m_pFile = NULL;
        // 备注信息长度
        m_nRemarkLen = 0;
        // 当前行数
        m_nCurRec = 0;
        // 文件行缓存
        m_pWriteBuf = NULL;
        m_pReadBuf = NULL;
        m_bReadOnly = true;
        // 获取当前年月日
        int nY = 0, nM = 0, nD = 0;
        GetCurDate(nY, nM, nD);
        m_cYear = nY;
        m_cMonth = nM;
        m_cDay = nD;
    }
    ~CPDbf()
    {
        Close();
    }

    // 判断文件是否打开
    inline bool IsOpen() { return m_pFile != NULL; }

    // 打开文件
    int Open(const std::string& strFile, bool bReadOnly = true)
    {
        // 检查是否已打开
        if (IsOpen())
        {
            return DBF_SUCC;
        }
        m_bReadOnly = bReadOnly;
        if (ws_fopen(&m_pFile, strFile.c_str(), bReadOnly ? "rb" : "rb+"))
        {
            return DBF_FILE_ERROR;
        }
        // 读取文件头信息
        if (ReadHeader())
        {
            return DBF_FILE_ERROR;
        }
        // 读取字段信息
        if (ReadField())
        {
            return DBF_FILE_ERROR;
        }

        // 变量初始化
        m_nCurRec = 0;
        m_strFilePath = strFile;
        return DBF_SUCC;
    }

    // 获取字段信息
    std::vector<TDbfField> GetField() { return m_vecField; }

    // 关闭DBF文件
    void Close()
    {
        // 关闭文件句柄
        if (m_pFile)
        {
            fclose(m_pFile);
            m_pFile = NULL;
        }

        // 内存清理
        delete m_pWriteBuf;
        m_pWriteBuf = NULL;

        delete m_pReadBuf;
        m_pReadBuf = NULL;
    }

    // 创建文件,字段值的名称、长度及类型是必填项
    virtual int Create(const std::string& strFile, const std::vector<TDbfField>& vecField)
    {
        // 设置头信息
        TDbfHeader oHeader;
        std::map<std::string, size_t> mapField;
        std::vector<TDbfField> vecNewField = vecField;
        memset(&oHeader, 0, sizeof(oHeader));
        oHeader.cVer = FV_FB3;
        oHeader.cYy = m_cYear;
        oHeader.cMm = m_cMonth;
        oHeader.cDd = m_cDay;
        // 头+32*字段数+1
        oHeader.nHeaderLen = (unsigned short)(sizeof(TDbfHeader) + 32 * vecNewField.size() + 1);
        oHeader.nRecLen = 1;
        for (size_t i = 0; i < vecNewField.size(); i++)
        {
            // 以计算的偏移值为准
            vecNewField[i].nPosition = oHeader.nRecLen;
            oHeader.nRecLen += vecNewField[i].cLength;
            mapField.insert(std::pair<std::string, size_t>(vecNewField[i].szName, i));
        }

        // 新建文件
        Close();
        FILE* pFile = NULL;
        if (NewFile(strFile, oHeader, vecNewField, &pFile))
        {
            return DBF_ERROR;
        }

        // 设置成员变量值
        m_bReadOnly = false;
        m_oHeader = oHeader;
        m_vecField = vecNewField;
        m_mapField = mapField;
        m_pFile = pFile;
        m_nCurRec = 0;
        m_strFilePath = strFile;
        m_nRemarkLen = GetRemarkSize(m_oHeader.cVer);
        return DBF_SUCC;
    }
    // 追加数据
    int Append(CPDbf& oDbf)
    {
        // 格式校验
        if (oDbf.m_oHeader.nHeaderLen != m_oHeader.nHeaderLen || oDbf.m_oHeader.nRecLen != m_oHeader.nRecLen)
        {
            return DBF_PARA_ERROR;
        }
        // 读取缓存数据
        if (oDbf.Read(0, oDbf.GetRecNum()))
        {
            return DBF_ERROR;
        }

        // 追加数据
        if (Go(m_oHeader.nRecNum))
        {
            return DBF_ERROR;
        }
        // 写入数据记录
        CRecordBuf* pBuf = oDbf.m_pReadBuf;
        size_t nAppendSize = pBuf->Size() * pBuf->RecLen();
        size_t nWrite = _write(pBuf->Data(), 1, nAppendSize);
        if (nAppendSize != nWrite)
        {
            return DBF_ERROR;
        }

        // 更新头数据
        m_oHeader.nRecNum += pBuf->Size();
        m_oHeader.cYy = m_cYear;
        m_oHeader.cMm = m_cMonth;
        m_oHeader.cDd = m_cDay;
        if (WriteHeader())
        {
            return DBF_ERROR;
        }
        // 当前行更新
        m_nCurRec = m_oHeader.nRecNum;
        return DBF_SUCC;
    }

    // 清空数据
    int Zap()
    {
        if (!IsOpen() || m_bReadOnly)
        {
            return DBF_ERROR;
        }

        // 设置头信息
        m_oHeader.cYy = m_cYear;
        m_oHeader.cMm = m_cMonth;
        m_oHeader.cDd = m_cDay;
        m_oHeader.nRecNum = 0;

        // 新建文件
        fclose(m_pFile);
        m_pFile = NULL;
        if (!NewFile(m_strFilePath, m_oHeader, m_vecField, &m_pFile))
        {
            return DBF_ERROR;
        }
        // 默认值
        m_nCurRec = 0;
        return DBF_SUCC;
    }

    // 直接操作文件类函数，记录号首行为0
    // 按字符串格式读取字段
    virtual std::string ReadString(size_t nRecNo, const std::string& strName)
    {
        std::string strValue;
        if (!IsOpen() || nRecNo >= m_oHeader.nRecNum)
        {
            return strValue;
        }
        size_t nSeq = FindField(strName);
        if (nSeq >= m_vecField.size())
        {
            return strValue;
        }
        ReadField(nRecNo, nSeq, strValue);
        return strValue;
    }
    // 按字符串格式写入字段
    virtual int WriteString(size_t nRecNo, const std::string& strName, const std::string& strValue)
    {
        if (!IsOpen() || nRecNo >= m_oHeader.nRecNum || m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        size_t nSeq = FindField(strName);
        if (nSeq >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        return WriteField(nRecNo, nSeq, strValue);
    }
    // 按字符串格式读取字段
    virtual int ReadField(size_t nRecNo, size_t nCol, std::string& strValue)
    {
        if (!IsOpen() || nRecNo >= m_oHeader.nRecNum || nCol>=m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        int nRet = DBF_ERROR;
        // 计算目标记录行的位置
        TDbfField& oField = m_vecField[nCol];
        size_t nCurOffset = RecordOffset() + nRecNo * m_oHeader.nRecLen + oField.nPosition;
        // 切换到对应文件记录行
        char* pField = new char[oField.nPosition];
        fseek(m_pFile, nCurOffset, SEEK_SET);
        size_t nRead = fread(pField, 1, oField.cLength, m_pFile);
        if (nRead == oField.cLength)
        {
            strValue = std::string(pField, oField.cLength);
            nRet = DBF_SUCC;
        }
        delete[] pField;
        return nRet;
    }
    // 在文件后追加记录数，nAppendNum指定新增的行数
    virtual int Append(size_t nAppendNum)
    {
        if (!IsOpen() || m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // 分配内存
        CRecordBuf oBuf(nAppendNum, m_oHeader.nRecLen);
        size_t nCurOffset = FileSize() - 1;
        fseek(m_pFile, nCurOffset, SEEK_SET);
        size_t nRead = fwrite(oBuf.Data(), 1, oBuf.DataSize(), m_pFile);
        if (nRead != oBuf.DataSize())
        {
            return DBF_ERROR;
        }
        
        // 更新头
        m_oHeader.nRecNum += nAppendNum;
        if (WriteHeader() != DBF_SUCC)
        {
            return DBF_ERROR;
        }

        // 更新文件结束标志
        if (WriteEndFlag())
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // 写字符串字段数据 
    virtual int WriteField(size_t nRecNo, size_t nCol, const std::string& strValue)
    {
        if (!IsOpen() || nRecNo >= m_oHeader.nRecNum || nCol >= m_vecField.size() || m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // 计算目标记录行的位置
        TDbfField& oField = m_vecField[nCol];
        size_t nCurOffset = RecordOffset() + nRecNo * m_oHeader.nRecLen  + oField.nPosition;
        char* pField = new char[oField.cLength];
        memset(pField, m_cBlank, oField.cLength);
        memcpy(pField, strValue.c_str(), MMin(oField.cLength, strValue.size()));
        // 切换到对应文件记录行
        fseek(m_pFile, nCurOffset, SEEK_SET);
        // 写字段数据
        size_t nWrite = fwrite(pField, 1, oField.cLength, m_pFile);
        delete[] pField;
        if (nWrite != oField.cLength)
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // 文件记录行
    inline size_t GetRecNum() { return m_oHeader.nRecNum; }
    // 字段数
    inline size_t GetFieldNum() { return m_vecField.size(); }

    // 读取记录行到缓存
    int Read(int nRecNo, int nRecNum)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        // 检查跳转记录行
        if (!IsValidRecNo(nRecNo + nRecNum) || Go(nRecNo))
        {
            return DBF_PARA_ERROR;
        }
        size_t nSize = nRecNum * m_oHeader.nRecLen;
        // 如果当前读缓存不够，销毁读缓存
        if (m_pReadBuf)
        {
            if (nSize > m_pReadBuf->BufSize())
            {
                delete m_pReadBuf;
                m_pReadBuf = NULL;
            }
        }
        // 分配读内存
        if (!m_pReadBuf)
        {
            m_pReadBuf = new CRecordBuf(nRecNum, m_oHeader.nRecLen);
        }
        size_t nRead = _read(m_pReadBuf->Data(), 1, nSize);
        if (nRead != nSize)
        {
            return DBF_ERROR;
        }
        m_pReadBuf->RecNum() = nRecNum;
        return DBF_SUCC;
    }

    // 设置读指针, 从0开始, 缓存记录行行数
    int ReadGo(int nRec)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (!m_pReadBuf)
        {
            return DBF_PARA_ERROR;
        }
        if (!m_pReadBuf->ReadGo(nRec))
        {
            return DBF_PARA_ERROR;
        }
        return DBF_SUCC;
    }

    // 读取字段,按字段名
    virtual std::string ReadString(const std::string& strName)
    {
        std::string strValue;
        ReadString(strName, strValue);
        return strValue;
    }
    virtual double ReadDouble(const std::string& strName)
    {
        double fValue = 0.0f;
        ReadDouble(strName, fValue);
        return fValue;
    }
    virtual int ReadInt(const std::string& strName)
    {
        int nValue = 0;
        ReadInt(strName, nValue);
        return nValue;
    }
    virtual long ReadLong(const std::string& strName)
    {
        long nValue = 0;
        ReadLong(strName, nValue);
        return nValue;
    }

    int ReadString(const std::string& strName, std::string& strValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        if (nSeq >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        return ReadString(nSeq, strValue);
    }
    int ReadDouble(const std::string& strName, double& fValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        if (nSeq >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        return ReadDouble(nSeq, fValue);
    }
    int ReadInt(const std::string& strName, int& nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        if (nSeq >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        return ReadInt(nSeq, nValue);
    }
    int ReadLong(const std::string& strName, long& nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        if (nSeq >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        return ReadLong(nSeq, nValue);
    }
    // 读取字段，按字段号
    int ReadString(size_t nCol, std::string& strValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (ReadField(nCol, strValue))
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }
    int ReadDouble(size_t nCol, double& fValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        std::string strField;
        if (ReadField(nCol, strField))
        {
            return DBF_ERROR;
        }
        fValue = atof(strField.c_str());
        return DBF_SUCC;
    }
    int ReadInt(size_t nCol, int& nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        std::string strField;
        if (ReadField(nCol, strField))
        {
            return DBF_ERROR;
        }
        nValue = atoi(strField.c_str());
        return DBF_SUCC;
    }
    int ReadLong(size_t nCol, long& nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        std::string strField;
        if (ReadField(nCol, strField))
        {
            return DBF_ERROR;
        }
        nValue = atol(strField.c_str());
        return DBF_SUCC;
    }

    // 申请写入数据
    int PrepareAppend(size_t nRecNum)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        // 检查跳转记录行
        if (nRecNum == 0)
        {
            return DBF_PARA_ERROR;
        }
        size_t nSize = nRecNum * m_oHeader.nRecLen;
        // 如果写读缓存不够，销毁缓存
        if (m_pWriteBuf)
        {
            if (nSize > m_pWriteBuf->BufSize())
            {
                delete m_pWriteBuf;
                m_pWriteBuf = NULL;
            }
        }
        // 分配读内存
        if (!m_pWriteBuf)
        {
            m_pWriteBuf = new CRecordBuf(nRecNum, m_oHeader.nRecLen);
        }
        m_pWriteBuf->WriteReset();
        return DBF_SUCC;
    }

    // 提交写数据数据到文件
    int WriteCommit()
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }

        if (!m_pWriteBuf)
        {
            return DBF_PARA_ERROR;
        }

        // 写缓存不为空，拷贝数据
        if (m_pWriteBuf->IsEmpty())
        {
            return DBF_SUCC;
        }

        // 设置写指针
        if (Go(m_oHeader.nRecNum))
        {
            return DBF_ERROR;
        }
        // 写入数据记录
        size_t nAppendSize = m_pWriteBuf->Size() * m_pWriteBuf->RecLen();
        size_t nWrite = _write(m_pWriteBuf->Data(), 1, nAppendSize);
        if (nAppendSize != nWrite)
        {
            return DBF_ERROR;
        }

        // 更新头数据
        m_oHeader.nRecNum += m_pWriteBuf->Size();
        m_oHeader.cYy = m_cYear;
        m_oHeader.cMm = m_cMonth;
        m_oHeader.cDd = m_cDay;
        if (WriteHeader())
        {
            return DBF_ERROR;
        }
        // 当前行更新
        m_nCurRec = m_oHeader.nRecNum;
        return DBF_SUCC;
    }
    // 提交写入的数据到文件
    int FileCommit()
    {
        if (!IsOpen() || m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // 写入文件结束标志
        if (WriteEndFlag())
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // 设置写指针, 从0开始, 缓存记录行行数
    int WriteGo(int nRec)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (!m_pWriteBuf)
        {
            return DBF_PARA_ERROR;
        }
        if (!m_pWriteBuf->WriteGo(nRec))
        {
            return DBF_PARA_ERROR;
        }
        return DBF_SUCC;
    }

    // 根据字段名写字段数据
    int WriteString(const std::string& strName, const std::string& strValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        return WriteString(nSeq, strValue);
    }
    int WriteDouble(const std::string& strName, double fValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        return WriteDouble(nSeq, fValue);
    }
    int WriteInt(const std::string& strName, int nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        return WriteInt(nSeq, nValue);
    }
    int WriteLong(const std::string& strName, long nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        size_t nSeq = FindField(strName);
        return WriteLong(nSeq, nValue);
    }
    // 根据索引写
    int WriteString(size_t nCol, const std::string& strValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (nCol >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        return WriteField(nCol, strValue);
    }
    int WriteDouble(size_t nCol, double fValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (nCol >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        TDbfField& oField = m_vecField[nCol];
        char szBuf[64] = { 0 };
        sprintf_s(szBuf, "%%%d.%df", (int)oField.cLength, (int)oField.cPrecisionLength);
        return WriteField(nCol, szBuf);
    }
    int WriteInt(size_t nCol, int nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (nCol >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        char szBuf[64] = { 0 };
        sprintf_s(szBuf, "%d", nValue);
        return WriteField(nCol, szBuf);
    }
    int WriteLong(size_t nCol, long nValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        if (nCol >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        char szBuf[64] = { 0 };
        sprintf_s(szBuf, "%ld", nValue);
        return WriteField(nCol, szBuf);
    }

protected:
    // 跳转到指定行，从0开始
    int Go(int nRec)
    {
        int nRet = DBF_SUCC;
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        // 校验行是否正确
        if (!IsValidRecNo(nRec))
        {
            return DBF_PARA_ERROR;
        }
        // 设置记录行
        m_nCurRec = nRec;
        return nRet;
    }

    // 读取数据原始字段
    int ReadField(const std::string& strName, std::string& strValue)
    {
        if (!m_pReadBuf || m_pReadBuf->IsEmpty())
        {
            return DBF_ERROR;
        }
        // 获取字段位置
        size_t nField = FindField(strName);
        return ReadField(nField, strValue);
    }
    int ReadField(size_t nField, std::string& strValue)
    {
        int nRet = DBF_SUCC;
        // 检查字段位置
        if (nField >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        if (!m_pReadBuf || m_pReadBuf->IsEmpty())
        {
            return DBF_ERROR;
        }
        // 获取字段信息
        char* pField = m_pReadBuf->GetCurRow() + m_vecField[nField].nPosition;
        strValue = std::string(pField, m_vecField[nField].cLength);
        return nRet;
    }

    // 写数据字段信息
    int WriteField(const std::string& strName, const std::string& strValue)
    {
        if (!m_pWriteBuf)
        {
            return DBF_ERROR;
        }
        // 获取字段位置
        size_t nField = FindField(strName);
        return WriteField(nField, strValue);
    }
    int WriteField(size_t nField, const std::string& strValue)
    {
        int nRet = DBF_SUCC;
        // 检查字段位置
        if (nField >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        // 获取字段信息
        char* pField = m_pWriteBuf->GetCurRow();
        pField += m_vecField[nField].nPosition;
        size_t nMaxFieldSize = m_vecField[nField].cLength;
        // 置字段为空
        memset(pField, m_cBlank, nMaxFieldSize);
        // 拷贝字段数据
        size_t nSize = MMin(nMaxFieldSize, strValue.size());
        if (!memcpy(pField, strValue.c_str(), nSize))
        {
            nRet = DBF_ERROR;
        }
        return nRet;
    }

    // 判断行号是否合法，包括已写入的文件行数+缓存行数
    bool IsValidRecNo(unsigned int nNum)
    {
        size_t nRec = nNum;
        size_t nWrite = m_pWriteBuf ? m_pWriteBuf->RecNum() : 0;
        if (nRec > m_oHeader.nRecNum + nWrite)
        {
            return false;
        }
        return true;
    }

    // 数据记录起始偏移值
    size_t RecordOffset()
    {
        assert(IsOpen());
        return m_oHeader.nHeaderLen + m_nRemarkLen;
    }

private:
    // 读取文件头信息
    int ReadHeader()
    {
        assert(IsOpen());
        // 读取头信息
        char szHeader[32] = { 0 };
        fseek(m_pFile, SEEK_SET, 0);
        size_t nRead = fread(szHeader, 1, sizeof(m_oHeader), m_pFile);
        if (nRead != sizeof(m_oHeader))
        {
            return DBF_FILE_ERROR;
        }
        memcpy(&m_oHeader, szHeader, sizeof(szHeader));

        // 更新备注信息长度
        m_nRemarkLen = GetRemarkSize(m_oHeader.cVer);
        return DBF_SUCC;
    }
    // 获取备注长度
    size_t GetRemarkSize(char cVer) const
    {
        size_t nRemark = 0;
        int nVer = cVer;
        switch (nVer)
        {
        case FV_MD3P:
        case FV_MD4:
        case FV_MD4TABLE:
        case FV_MFP2:
        case FV_VFP:
            nRemark = 263;
            break;
        default:
            break;
        }
        return nRemark;
    }

    // 读取字段信息
    int ReadField()
    {
        assert(IsOpen());
        // 字段总长度 = 头(32) + n*字段(32) + 1(0x1D)
        int nFieldLen = m_oHeader.nHeaderLen - sizeof(m_oHeader);
        if (nFieldLen % 32 != 1)
        {
            return DBF_FILE_ERROR;
        }

        // 读取字段信息
        int nRet = DBF_SUCC;
        char* pField = new char[nFieldLen];
        fseek(m_pFile, sizeof(m_oHeader), SEEK_SET);
        size_t nRead = fread(pField, 1, nFieldLen, m_pFile);
        if (nRead != nFieldLen)
        {
            delete[] pField;
            pField = NULL;
            return DBF_FILE_ERROR;
        }

        // 解析字段
        TDbfField oField;
        char* pCur = pField;
        char* pEnd = pField + nFieldLen - sizeof(m_oHeader);
        // 字段偏移值从1开始，首位为标志位
        int nOffset = 1;
        m_vecField.clear();
        m_mapField.clear();
        while (pCur < pEnd)
        {
            // 拷贝字段信息
            memcpy(&oField, pCur, sizeof(oField));
            pCur += sizeof(oField);

            // 计算字段偏移值
            oField.nPosition = nOffset;
            nOffset += oField.cLength;

            // 保存字段
            m_vecField.push_back(oField);
            m_mapField.insert(std::pair<std::string, size_t>(oField.szName, m_vecField.size() - 1));
        }
        // 校验最后一位是否为0x0D
        if (*pCur != 0x0D)
        {
            nRet = DBF_FILE_ERROR;
        }
        // 内存清理
        delete[] pField;
        pField = NULL;
        return nRet;
    }

    // 获取当前时间
    void GetCurDate(int& nYear, int& nMon, int& nDay)
    {
        time_t t = time(0);
        struct tm tmTime = { 0 };
        ws_localtime(&tmTime, &t);
        nYear = (tmTime.tm_year + 1900) % 100;
        nMon = tmTime.tm_mon + 1;
        nDay = tmTime.tm_mday;
    }

protected:
    // 读取记录行函数，不允许跨文件数据与缓存数据读取，且读取数据时必须按行读取
    virtual size_t _read(void* ptr, size_t size, size_t nmemb)
    {
        int nRet = DBF_SUCC;
        assert(IsOpen());
        // 判断行数
        if (m_nCurRec < m_oHeader.nRecNum)
        {
            // 计算目标记录行的位置
            size_t nCurOffset = m_nCurRec * m_oHeader.nRecLen + RecordOffset();

            // 切换到对应文件记录行
            fseek(m_pFile, nCurOffset, SEEK_SET);
            return fread(ptr, size, nmemb, m_pFile);
        }
        // 从缓存行读取数据，写缓存行
        else if (m_pWriteBuf)
        {
            char* pData = m_pWriteBuf->Data() + (m_nCurRec - m_oHeader.nRecNum)*m_oHeader.nRecLen;
            if (!memcpy(ptr, pData, size * nmemb))
            {
                nRet = DBF_ERROR;
            }
            else
            {
                nRet = nmemb;
            }
        }
        else
        {
            nRet = DBF_PARA_ERROR;
        }
        return nRet;
    }
    // 写入记录行函数，不允许跨文件数据与缓存数据操作，且操作数据时必须按行
    virtual size_t _write(void* ptr, size_t size, size_t nmemb)
    {
        int nRet = DBF_SUCC;
        assert(IsOpen());
        if (m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // 判断行数
        if (m_nCurRec <= m_oHeader.nRecNum)
        {
            // 计算目标记录行的位置
            size_t nCurOffset = m_nCurRec * m_oHeader.nRecLen + RecordOffset();

            // 切换到对应记录行，写入数据
            fseek(m_pFile, nCurOffset, SEEK_SET);
            return fwrite(ptr, size, nmemb, m_pFile);
        }
        // 写入
        else if (m_pWriteBuf)
        {
            char* pData = m_pWriteBuf->Data() + (m_nCurRec - m_oHeader.nRecNum)*m_oHeader.nRecLen;
            if (!memcpy(pData, ptr, size * nmemb))
            {
                nRet = DBF_ERROR;
            }
            else
            {
                nRet = nmemb;
            }
        }
        else
        {
            nRet = DBF_PARA_ERROR;
        }

        return nRet;
    }
    // 写入文件结束标志
    size_t WriteEndFlag()
    {
        int nRet = DBF_SUCC;
        assert(IsOpen());
        if (m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        const char cEndFlag = 0X1A;

        // 计算目标位置
        size_t nOffset = FileSize() - 1;

        // 切换到对应记录行，写入数据
        fseek(m_pFile, nOffset, SEEK_SET);
        if (fwrite(&cEndFlag, 1, 1, m_pFile) != sizeof(cEndFlag))
        {
            return DBF_ERROR;
        }

        return nRet;
    }

    // 查找字段位置
    virtual size_t FindField(const std::string& strField)
    {
        std::map<std::string, size_t>::iterator e = m_mapField.find(strField);
        if (e != m_mapField.end())
        {
            return e->second;
        }
        return -1;
    }

    // 写头数据到文件
    int WriteHeader()
    {
        fseek(m_pFile, 0, SEEK_SET);
        if (fwrite(&m_oHeader, 1, sizeof(m_oHeader), m_pFile) != sizeof(m_oHeader))
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // 返回文件大小
    size_t FileSize()
    {
        // 文件头 + 记录数据 + 文件尾巴
        return RecordOffset() + m_oHeader.nRecNum * m_oHeader.nRecLen + 1;
    }

    // 新建空文件
    int NewFile(const std::string& strFile, const TDbfHeader& oHeader, const std::vector<TDbfField>& vecField, FILE** ppFile) const
    {
        *ppFile = NULL;
        int nRet = ws_fopen(ppFile, strFile.c_str(), "wb");
        if (nRet)
        {
            return DBF_FILE_ERROR;
        }
        FILE* pFile = *ppFile;

        // 写入头
        int nWrite = fwrite(&oHeader, 1, sizeof(oHeader), pFile);
        if (nWrite != sizeof(oHeader))
        {
            fclose(pFile);
            return DBF_FILE_ERROR;
        }
        // 写入字段信息
        for (size_t i = 0; i < vecField.size(); i++)
        {
            nWrite = fwrite(&vecField[i], 1, sizeof(vecField[i]), pFile);
            if (nWrite != sizeof(vecField[i]))
            {
                fclose(pFile);
                return DBF_FILE_ERROR;
            }
        }
        // 写入结束标志
        const char cHeadEndFlag = 0X0D;
        if (fwrite(&cHeadEndFlag, 1, sizeof(cHeadEndFlag), pFile) != sizeof(cHeadEndFlag))
        {
            fclose(pFile);
            return DBF_FILE_ERROR;
        }
        // 备注信息
        size_t nRemarkSize = GetRemarkSize(oHeader.cVer);
        if (nRemarkSize)
        {
            char* pRemark = new char[nRemarkSize];
            memset(pRemark, 0, nRemarkSize);
            nWrite = fwrite(pRemark, 1, nRemarkSize, pFile);
            delete[] pRemark;
            if (nWrite != nRemarkSize)
            {
                fclose(pFile);
                return DBF_FILE_ERROR;
            }
        }
        // 文件结束
        //const char cFileEndFlag = 0X1A;
        //if (fwrite(&cFileEndFlag, 1, sizeof(cFileEndFlag), pFile) != sizeof(cFileEndFlag))
        //{
        //    fclose(pFile);
        //    return DBF_FILE_ERROR;
        //}
        return DBF_SUCC;
    }

private:
    // 当前年月日
    unsigned char m_cYear;
    unsigned char m_cMonth;
    unsigned char m_cDay;

    // 文件对象
    FILE* m_pFile;
    // 文件路径
    std::string m_strFilePath;
    // 是否只读
    bool m_bReadOnly;
    // 文件头信息
    TDbfHeader m_oHeader;
    // 文件字段信息
    std::vector<TDbfField> m_vecField;
    // 字段名字
    std::map<std::string, size_t> m_mapField;
    // 备注信息长度
    size_t m_nRemarkLen;
    // 当前行数
    size_t m_nCurRec;
    // 空白字符
    const char m_cBlank;

    // 文件写记录行缓存（未保存到文件的数据）
    CRecordBuf* m_pWriteBuf;
    // 文件读记录行缓存（仅用于读）
    CRecordBuf* m_pReadBuf;
};

class CCMPDbf
{
public:
    CCMPDbf()
    {
        m_nMaxRowDiffs = 128;
        m_nMaxDiffs = 256;
        m_nCurDiffs = 0;
        m_nCurRowDiffs = 0;
    }
public:
    // 比较的字段
    std::vector<std::string> m_vecField;
    // 行最大差异值
    size_t m_nMaxRowDiffs;
    // 差异最大值
    size_t m_nMaxDiffs;
    // 当前已发现的行差异
    size_t m_nCurRowDiffs;
    // 当前已发现的差异
    size_t m_nCurDiffs;

    // 比较两个BDF文件，差异则返回true
    bool Cmp(const std::string& strFile1, const std::string& strFile2)
    {
        CPDbf oDbf1;
        CPDbf oDbf2;

        // 打开文件
        oDbf1.Open(strFile1, true);
        if (!oDbf1.IsOpen())
        {
            return false;
        }
        oDbf2.Open(strFile2, true);
        if (!oDbf2.IsOpen())
        {
            oDbf1.Close();
            return false;
        }

        // 读取字段并比较
        size_t nRecNum1 = oDbf1.GetRecNum();
        size_t nRecNum2 = oDbf1.GetRecNum();
        size_t nRecNum = MMin(nRecNum1, nRecNum2);
        size_t nReadNum = 10000;
        m_nCurDiffs = 0;
        m_nCurRowDiffs = 0;
        bool bEq = false;
        std::string strBuf1;
        std::string strBuf2;
        for (size_t i = 0; i < nRecNum; i += nReadNum)
        {
            // 计算读取行数
            nReadNum = MMin(nReadNum, nRecNum - i);

            // 读取行
            if (oDbf1.Read(i, nReadNum) || oDbf2.Read(i, nReadNum))
            {
                return false;
            }

            // 字段比较
            for (size_t j = 0; j < nReadNum; j++)
            {
                bEq = true;
                oDbf1.ReadGo(i + j);
                oDbf2.ReadGo(i + j);
                for (size_t k = 0; k < m_vecField.size(); k++)
                {
                    strBuf1 = strBuf2 = "";
                    oDbf1.ReadString(m_vecField[k], strBuf1);
                    oDbf2.ReadString(m_vecField[k], strBuf2);
                    if (CIDbf::Trim(strBuf1) != CIDbf::Trim(strBuf2))
                    {
                        m_nCurDiffs++;
                        bEq = false;
                    }
                }
                if (!bEq)
                {
                    m_nCurRowDiffs++;
                }
            }
        }

        // 行差异
        if (nRecNum1 != nRecNum2)
        {
            size_t nLineDiff = (MMax(nRecNum1, nRecNum2) - MMin(nRecNum1, nRecNum2));
            m_nCurRowDiffs += nLineDiff;
            m_nCurDiffs += (m_vecField.size() * nLineDiff);
        }
        oDbf1.Close();
        oDbf2.Close();

        return (m_nCurDiffs == 0 && m_nMaxDiffs == 0);
    }
};

#endif
