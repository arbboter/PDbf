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

// DBF�ӿ�
class CIDbf
{
public:
    // ������
    enum ERetCode
    {
        DBF_SUCC,           // �ɹ�
        DBF_ERROR,          // ����
        DBF_FILE_ERROR,     // �ļ�����
        DBF_PARA_ERROR,     // ��������
        DBF_CACHE_ERROR,    // �������
    };
    // �ж��ļ��Ƿ��
    virtual bool IsOpen() = 0;

    // ���ļ�
    virtual int Open(const std::string& strFile, bool bReadOnly = true) = 0;

    // �ر�DBF�ļ�
    virtual void Close() = 0;

    // �ļ���¼��
    virtual size_t GetRecNum() = 0;
    // �ֶ���
    virtual size_t GetFieldNum() = 0;

    // ֱ�Ӳ����ļ������ܵͣ���¼������Ϊ0
    virtual std::string ReadString(size_t nRecNo, const std::string& strName) = 0;
    virtual int WriteString(size_t nRecNo, const std::string& strName, const std::string& strValue) = 0;
    virtual int ReadField(size_t nRecNo, size_t nCol, std::string& strValue) = 0;
    virtual int Append(size_t nAppendNum) = 0;
    virtual int WriteField(size_t nRecNo, size_t nCol, const std::string& strValue) = 0;

    // ���洦�����ܸ�
    // ��ȡ�ļ���¼
    // ��ȡ��¼�е�����
    virtual int Read(int nRecNo, int nRecNum) = 0;
    // ���ö�ָ��, ��0��ʼ, �����¼������
    virtual int ReadGo(int nRec) = 0;
    // ��ȡ�ֶ�
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

    // д�ļ���¼
    // �������
    virtual int Zap() = 0;
    // ����д������
    virtual int PrepareAppend(size_t nRecNum) = 0;
    // �ύд�������ݵ��ļ�
    virtual int WriteCommit() = 0;
    // �ύд������ݵ��ļ�
    virtual int FileCommit() = 0;
    // ����дָ��, ��0��ʼ, �����¼������
    virtual int WriteGo(int nRec) = 0;
    // �����ֶ���д�ֶ�����
    virtual int WriteString(const std::string& strName, const std::string& strValue) = 0;
    virtual int WriteDouble(const std::string& strName, double fValue) = 0;
    virtual int WriteInt(const std::string& strName, int nValue) = 0;
    virtual int WriteLong(const std::string& strName, long nValue) = 0;
    // ��������д
    virtual int WriteString(size_t nCol, const std::string& strValue) = 0;
    virtual int WriteDouble(size_t nCol, double fValue) = 0;
    virtual int WriteInt(size_t nCol, int nValue) = 0;
    virtual int WriteLong(size_t nCol, long nValue) = 0;

    // �ַ�������
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

// DBF�ļ�����
enum EFV
{
    FV_NS = 0x00,       // ��ȷ�����ļ�����
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
// DBFͷ(32�ֽ�)
class TDbfHeader
{
public:
    char            cVer;           // �汾��־
    unsigned char   cYy;            // ��������
    unsigned char   cMm;            // ��������
    unsigned char   cDd;            // ������
    unsigned int    nRecNum;        // �ļ��������ܼ�¼��
    unsigned short  nHeaderLen;     // �ļ�ͷ����
    unsigned short  nRecLen;        // ��¼����
    char            szReserved[20]; // ����

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
// �ֶ�
class TDbfField
{
public:
    // �ֶ���
    char szName[11];
    // �ֶ����ͣ���ASCII��ֵ
    // B-������ C-�ַ��� D-��������YYYYMMDD G-�����ַ� N-��ֵ�� L-�߼��� M-�����ַ�
    unsigned char cType;
    // �����ֽڣ������Ժ�����µ�˵������Ϣʱʹ�ã�������0����д
    unsigned int nReserved1;
    // ��¼��ȣ���������
    unsigned char cLength;
    // ��¼��ľ��ȣ���������
    unsigned char cPrecisionLength;
    // �����ֽڣ������Ժ�����µ�˵������Ϣʱʹ�ã�������0����д
    // �������ڴ��ļ�ʱ����¼��Ӧ�ֶε���ʵƫ��ֵ
    unsigned short nPosition;
    // ������ID
    unsigned char cWorkId;
    // �����ֽڣ������Ժ�����µ�˵������Ϣʱʹ�ã�������0����д
    char szReserved[10];
    // MDX��ʶ���������һ��MDX ��ʽ�������ļ�����ô�����¼��Ϊ�棬����Ϊ��
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
// ��¼�л���
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

    // ���ؼ�¼��
    inline size_t Size()
    {
        return m_nRecNum;
    }

    // �������ڴ��С
    inline size_t DataSize()
    {
        return RecNum() * RecLen();
    }

    // ׷�Ӽ�¼��
    inline bool Push(char* pRec, int nNum)
    {
        if (nNum + m_nRecNum > m_nRecCapacity)
        {
            return false;
        }
        memcpy(m_pBuf, pRec, nNum * m_nRecLen);
        return true;
    }

    // ��ȡ��¼��
    char* At(size_t nIdx)
    {
        if (nIdx >= m_nRecNum)
        {
            return NULL;
        }
        return &m_pBuf[nIdx * m_nRecLen];
    }

    // �޸Ķ����С
    void Resize(size_t nNum)
    {
        // ��С
        if (nNum < m_nRecCapacity)
        {
            m_nRecCapacity = nNum;
        }
        if (nNum < m_nRecNum)
        {
            m_nRecNum = nNum;
        }
        // ����
        if (nNum > m_nRecCapacity)
        {
            char* pBuf = new char[nNum * m_nRecLen];
            memset(pBuf, 0, nNum * m_nRecLen);
            memcpy(pBuf, m_pBuf, m_nRecCapacity*m_nRecLen);
            m_nRecCapacity = nNum;
            // �ڴ洦��
            delete m_pBuf;
            m_pBuf = pBuf;
        }
    }

    // �Ƿ�Ϊ��
    bool IsEmpty()
    {
        return m_nRecNum == 0;
    }
    // ���ػ����С
    size_t BufSize()
    {
        return m_nRecCapacity * m_nRecLen;
    }
    // ���õ�ǰ�У���0��ʼ
    inline bool ReadGo(size_t nRec)
    {
        if (nRec >= m_nRecNum)
        {
            return false;
        }
        m_nCurRec = nRec;
        return true;
    }
    // ���õ�ǰ�У���0��ʼ
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
    // ��ȡ��ǰ������
    char* GetCurRow()
    {
        return m_pBuf + m_nCurRec * m_nRecLen;
    }

    // ��������
    inline char* Data() { return m_pBuf; }
    inline size_t& RecNum() { return m_nRecNum; }
    inline size_t  RecLen() { return m_nRecLen; }
private:
    // ��ǰ��¼��
    size_t m_nRecNum;
    // ÿ����¼����
    size_t m_nRecLen;
    // �����ɼ�¼��
    size_t m_nRecCapacity;
    // ����
    char* m_pBuf;
    // ��ǰ������
    size_t m_nCurRec;
};

// �����ƽ̨����
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
    // ���캯��
    CPDbf()
        :m_cBlank(' ')
    {
        // �ļ����
        m_pFile = NULL;
        // ��ע��Ϣ����
        m_nRemarkLen = 0;
        // ��ǰ����
        m_nCurRec = 0;
        // �ļ��л���
        m_pWriteBuf = NULL;
        m_pReadBuf = NULL;
        m_bReadOnly = true;
        // ��ȡ��ǰ������
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

    // �ж��ļ��Ƿ��
    inline bool IsOpen() { return m_pFile != NULL; }

    // ���ļ�
    int Open(const std::string& strFile, bool bReadOnly = true)
    {
        // ����Ƿ��Ѵ�
        if (IsOpen())
        {
            return DBF_SUCC;
        }
        m_bReadOnly = bReadOnly;
        if (ws_fopen(&m_pFile, strFile.c_str(), bReadOnly ? "rb" : "rb+"))
        {
            return DBF_FILE_ERROR;
        }
        // ��ȡ�ļ�ͷ��Ϣ
        if (ReadHeader())
        {
            return DBF_FILE_ERROR;
        }
        // ��ȡ�ֶ���Ϣ
        if (ReadField())
        {
            return DBF_FILE_ERROR;
        }

        // ������ʼ��
        m_nCurRec = 0;
        m_strFilePath = strFile;
        return DBF_SUCC;
    }

    // ��ȡ�ֶ���Ϣ
    std::vector<TDbfField> GetField() { return m_vecField; }

    // �ر�DBF�ļ�
    void Close()
    {
        // �ر��ļ����
        if (m_pFile)
        {
            fclose(m_pFile);
            m_pFile = NULL;
        }

        // �ڴ�����
        delete m_pWriteBuf;
        m_pWriteBuf = NULL;

        delete m_pReadBuf;
        m_pReadBuf = NULL;
    }

    // �����ļ�,�ֶ�ֵ�����ơ����ȼ������Ǳ�����
    virtual int Create(const std::string& strFile, const std::vector<TDbfField>& vecField)
    {
        // ����ͷ��Ϣ
        TDbfHeader oHeader;
        std::map<std::string, size_t> mapField;
        std::vector<TDbfField> vecNewField = vecField;
        memset(&oHeader, 0, sizeof(oHeader));
        oHeader.cVer = FV_FB3;
        oHeader.cYy = m_cYear;
        oHeader.cMm = m_cMonth;
        oHeader.cDd = m_cDay;
        // ͷ+32*�ֶ���+1
        oHeader.nHeaderLen = (unsigned short)(sizeof(TDbfHeader) + 32 * vecNewField.size() + 1);
        oHeader.nRecLen = 1;
        for (size_t i = 0; i < vecNewField.size(); i++)
        {
            // �Լ����ƫ��ֵΪ׼
            vecNewField[i].nPosition = oHeader.nRecLen;
            oHeader.nRecLen += vecNewField[i].cLength;
            mapField.insert(std::pair<std::string, size_t>(vecNewField[i].szName, i));
        }

        // �½��ļ�
        Close();
        FILE* pFile = NULL;
        if (NewFile(strFile, oHeader, vecNewField, &pFile))
        {
            return DBF_ERROR;
        }

        // ���ó�Ա����ֵ
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
    // ׷������
    int Append(CPDbf& oDbf)
    {
        // ��ʽУ��
        if (oDbf.m_oHeader.nHeaderLen != m_oHeader.nHeaderLen || oDbf.m_oHeader.nRecLen != m_oHeader.nRecLen)
        {
            return DBF_PARA_ERROR;
        }
        // ��ȡ��������
        if (oDbf.Read(0, oDbf.GetRecNum()))
        {
            return DBF_ERROR;
        }

        // ׷������
        if (Go(m_oHeader.nRecNum))
        {
            return DBF_ERROR;
        }
        // д�����ݼ�¼
        CRecordBuf* pBuf = oDbf.m_pReadBuf;
        size_t nAppendSize = pBuf->Size() * pBuf->RecLen();
        size_t nWrite = _write(pBuf->Data(), 1, nAppendSize);
        if (nAppendSize != nWrite)
        {
            return DBF_ERROR;
        }

        // ����ͷ����
        m_oHeader.nRecNum += pBuf->Size();
        m_oHeader.cYy = m_cYear;
        m_oHeader.cMm = m_cMonth;
        m_oHeader.cDd = m_cDay;
        if (WriteHeader())
        {
            return DBF_ERROR;
        }
        // ��ǰ�и���
        m_nCurRec = m_oHeader.nRecNum;
        return DBF_SUCC;
    }

    // �������
    int Zap()
    {
        if (!IsOpen() || m_bReadOnly)
        {
            return DBF_ERROR;
        }

        // ����ͷ��Ϣ
        m_oHeader.cYy = m_cYear;
        m_oHeader.cMm = m_cMonth;
        m_oHeader.cDd = m_cDay;
        m_oHeader.nRecNum = 0;

        // �½��ļ�
        fclose(m_pFile);
        m_pFile = NULL;
        if (!NewFile(m_strFilePath, m_oHeader, m_vecField, &m_pFile))
        {
            return DBF_ERROR;
        }
        // Ĭ��ֵ
        m_nCurRec = 0;
        return DBF_SUCC;
    }

    // ֱ�Ӳ����ļ��ຯ������¼������Ϊ0
    // ���ַ�����ʽ��ȡ�ֶ�
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
    // ���ַ�����ʽд���ֶ�
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
    // ���ַ�����ʽ��ȡ�ֶ�
    virtual int ReadField(size_t nRecNo, size_t nCol, std::string& strValue)
    {
        if (!IsOpen() || nRecNo >= m_oHeader.nRecNum || nCol>=m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        int nRet = DBF_ERROR;
        // ����Ŀ���¼�е�λ��
        TDbfField& oField = m_vecField[nCol];
        size_t nCurOffset = RecordOffset() + nRecNo * m_oHeader.nRecLen + oField.nPosition;
        // �л�����Ӧ�ļ���¼��
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
    // ���ļ���׷�Ӽ�¼����nAppendNumָ������������
    virtual int Append(size_t nAppendNum)
    {
        if (!IsOpen() || m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // �����ڴ�
        CRecordBuf oBuf(nAppendNum, m_oHeader.nRecLen);
        size_t nCurOffset = FileSize() - 1;
        fseek(m_pFile, nCurOffset, SEEK_SET);
        size_t nRead = fwrite(oBuf.Data(), 1, oBuf.DataSize(), m_pFile);
        if (nRead != oBuf.DataSize())
        {
            return DBF_ERROR;
        }
        
        // ����ͷ
        m_oHeader.nRecNum += nAppendNum;
        if (WriteHeader() != DBF_SUCC)
        {
            return DBF_ERROR;
        }

        // �����ļ�������־
        if (WriteEndFlag())
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // д�ַ����ֶ����� 
    virtual int WriteField(size_t nRecNo, size_t nCol, const std::string& strValue)
    {
        if (!IsOpen() || nRecNo >= m_oHeader.nRecNum || nCol >= m_vecField.size() || m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // ����Ŀ���¼�е�λ��
        TDbfField& oField = m_vecField[nCol];
        size_t nCurOffset = RecordOffset() + nRecNo * m_oHeader.nRecLen  + oField.nPosition;
        char* pField = new char[oField.cLength];
        memset(pField, m_cBlank, oField.cLength);
        memcpy(pField, strValue.c_str(), MMin(oField.cLength, strValue.size()));
        // �л�����Ӧ�ļ���¼��
        fseek(m_pFile, nCurOffset, SEEK_SET);
        // д�ֶ�����
        size_t nWrite = fwrite(pField, 1, oField.cLength, m_pFile);
        delete[] pField;
        if (nWrite != oField.cLength)
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // �ļ���¼��
    inline size_t GetRecNum() { return m_oHeader.nRecNum; }
    // �ֶ���
    inline size_t GetFieldNum() { return m_vecField.size(); }

    // ��ȡ��¼�е�����
    int Read(int nRecNo, int nRecNum)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        // �����ת��¼��
        if (!IsValidRecNo(nRecNo + nRecNum) || Go(nRecNo))
        {
            return DBF_PARA_ERROR;
        }
        size_t nSize = nRecNum * m_oHeader.nRecLen;
        // �����ǰ�����治�������ٶ�����
        if (m_pReadBuf)
        {
            if (nSize > m_pReadBuf->BufSize())
            {
                delete m_pReadBuf;
                m_pReadBuf = NULL;
            }
        }
        // ������ڴ�
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

    // ���ö�ָ��, ��0��ʼ, �����¼������
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

    // ��ȡ�ֶ�,���ֶ���
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
    // ��ȡ�ֶΣ����ֶκ�
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

    // ����д������
    int PrepareAppend(size_t nRecNum)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        // �����ת��¼��
        if (nRecNum == 0)
        {
            return DBF_PARA_ERROR;
        }
        size_t nSize = nRecNum * m_oHeader.nRecLen;
        // ���д�����治�������ٻ���
        if (m_pWriteBuf)
        {
            if (nSize > m_pWriteBuf->BufSize())
            {
                delete m_pWriteBuf;
                m_pWriteBuf = NULL;
            }
        }
        // ������ڴ�
        if (!m_pWriteBuf)
        {
            m_pWriteBuf = new CRecordBuf(nRecNum, m_oHeader.nRecLen);
        }
        m_pWriteBuf->WriteReset();
        return DBF_SUCC;
    }

    // �ύд�������ݵ��ļ�
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

        // д���治Ϊ�գ���������
        if (m_pWriteBuf->IsEmpty())
        {
            return DBF_SUCC;
        }

        // ����дָ��
        if (Go(m_oHeader.nRecNum))
        {
            return DBF_ERROR;
        }
        // д�����ݼ�¼
        size_t nAppendSize = m_pWriteBuf->Size() * m_pWriteBuf->RecLen();
        size_t nWrite = _write(m_pWriteBuf->Data(), 1, nAppendSize);
        if (nAppendSize != nWrite)
        {
            return DBF_ERROR;
        }

        // ����ͷ����
        m_oHeader.nRecNum += m_pWriteBuf->Size();
        m_oHeader.cYy = m_cYear;
        m_oHeader.cMm = m_cMonth;
        m_oHeader.cDd = m_cDay;
        if (WriteHeader())
        {
            return DBF_ERROR;
        }
        // ��ǰ�и���
        m_nCurRec = m_oHeader.nRecNum;
        return DBF_SUCC;
    }
    // �ύд������ݵ��ļ�
    int FileCommit()
    {
        if (!IsOpen() || m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // д���ļ�������־
        if (WriteEndFlag())
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // ����дָ��, ��0��ʼ, �����¼������
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

    // �����ֶ���д�ֶ�����
    int WriteString(const std::string& strName, const std::string& strValue)
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
        return WriteString(nSeq, strValue);
    }
    int WriteDouble(const std::string& strName, double fValue)
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
        return WriteDouble(nSeq, fValue);
    }
    int WriteInt(const std::string& strName, int nValue)
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
        return WriteInt(nSeq, nValue);
    }
    int WriteLong(const std::string& strName, long nValue)
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
        return WriteLong(nSeq, nValue);
    }
    // ��������д
    int WriteString(size_t nCol, const std::string& strValue)
    {
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
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
    // ��ת��ָ���У���0��ʼ
    int Go(int nRec)
    {
        int nRet = DBF_SUCC;
        if (!IsOpen())
        {
            return DBF_FILE_ERROR;
        }
        // У�����Ƿ���ȷ
        if (!IsValidRecNo(nRec))
        {
            return DBF_PARA_ERROR;
        }
        // ���ü�¼��
        m_nCurRec = nRec;
        return nRet;
    }

    // ��ȡ����ԭʼ�ֶ�
    int ReadField(const std::string& strName, std::string& strValue)
    {
        if (!m_pReadBuf || m_pReadBuf->IsEmpty())
        {
            return DBF_ERROR;
        }
        // ��ȡ�ֶ�λ��
        size_t nField = FindField(strName);
        return ReadField(nField, strValue);
    }
    int ReadField(size_t nField, std::string& strValue)
    {
        int nRet = DBF_SUCC;
        // ����ֶ�λ��
        if (nField >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        if (!m_pReadBuf || m_pReadBuf->IsEmpty())
        {
            return DBF_ERROR;
        }
        // ��ȡ�ֶ���Ϣ
        char* pField = m_pReadBuf->GetCurRow() + m_vecField[nField].nPosition;
        strValue = std::string(pField, m_vecField[nField].cLength);
        return nRet;
    }

    // д�����ֶ���Ϣ
    int WriteField(const std::string& strName, const std::string& strValue)
    {
        if (!m_pWriteBuf)
        {
            return DBF_ERROR;
        }
        // ��ȡ�ֶ�λ��
        size_t nField = FindField(strName);
        return WriteField(nField, strValue);
    }
    int WriteField(size_t nField, const std::string& strValue)
    {
        int nRet = DBF_SUCC;
        // ����ֶ�λ��
        if (nField >= m_vecField.size())
        {
            return DBF_PARA_ERROR;
        }
        // ��ȡ�ֶ���Ϣ
        char* pField = m_pWriteBuf->GetCurRow();
        pField += m_vecField[nField].nPosition;
        size_t nMaxFieldSize = m_vecField[nField].cLength;
        // ���ֶ�Ϊ��
        memset(pField, m_cBlank, nMaxFieldSize);
        // �����ֶ�����
        size_t nSize = MMin(nMaxFieldSize, strValue.size());
        if (!memcpy(pField, strValue.c_str(), nSize))
        {
            nRet = DBF_ERROR;
        }
        return nRet;
    }

    // �ж��к��Ƿ�Ϸ���������д����ļ�����+��������
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

    // ���ݼ�¼��ʼƫ��ֵ
    size_t RecordOffset()
    {
        assert(IsOpen());
        return m_oHeader.nHeaderLen + m_nRemarkLen;
    }

private:
    // ��ȡ�ļ�ͷ��Ϣ
    int ReadHeader()
    {
        assert(IsOpen());
        // ��ȡͷ��Ϣ
        char szHeader[32] = { 0 };
        fseek(m_pFile, SEEK_SET, 0);
        size_t nRead = fread(szHeader, 1, sizeof(m_oHeader), m_pFile);
        if (nRead != sizeof(m_oHeader))
        {
            return DBF_FILE_ERROR;
        }
        memcpy(&m_oHeader, szHeader, sizeof(szHeader));

        // ���±�ע��Ϣ����
        m_nRemarkLen = GetRemarkSize(m_oHeader.cVer);
        return DBF_SUCC;
    }
    // ��ȡ��ע����
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

    // ��ȡ�ֶ���Ϣ
    int ReadField()
    {
        assert(IsOpen());
        // �ֶ��ܳ��� = ͷ(32) + n*�ֶ�(32) + 1(0x1D)
        int nFieldLen = m_oHeader.nHeaderLen - sizeof(m_oHeader);
        if (nFieldLen % 32 != 1)
        {
            return DBF_FILE_ERROR;
        }

        // ��ȡ�ֶ���Ϣ
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

        // �����ֶ�
        TDbfField oField;
        char* pCur = pField;
        char* pEnd = pField + nFieldLen - sizeof(m_oHeader);
        // �ֶ�ƫ��ֵ��1��ʼ����λΪ��־λ
        int nOffset = 1;
        m_vecField.clear();
        m_mapField.clear();
        while (pCur < pEnd)
        {
            // �����ֶ���Ϣ
            memcpy(&oField, pCur, sizeof(oField));
            pCur += sizeof(oField);

            // �����ֶ�ƫ��ֵ
            oField.nPosition = nOffset;
            nOffset += oField.cLength;

            // �����ֶ�
            m_vecField.push_back(oField);
            m_mapField.insert(std::pair<std::string, size_t>(oField.szName, m_vecField.size() - 1));
        }
        // У�����һλ�Ƿ�Ϊ0x0D
        if (*pCur != 0x0D)
        {
            nRet = DBF_FILE_ERROR;
        }
        // �ڴ�����
        delete[] pField;
        pField = NULL;
        return nRet;
    }

    // ��ȡ��ǰʱ��
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
    // ��ȡ��¼�к�������������ļ������뻺�����ݶ�ȡ���Ҷ�ȡ����ʱ���밴�ж�ȡ
    virtual size_t _read(void* ptr, size_t size, size_t nmemb)
    {
        int nRet = DBF_SUCC;
        assert(IsOpen());
        // �ж�����
        if (m_nCurRec < m_oHeader.nRecNum)
        {
            // ����Ŀ���¼�е�λ��
            size_t nCurOffset = m_nCurRec * m_oHeader.nRecLen + RecordOffset();

            // �л�����Ӧ�ļ���¼��
            fseek(m_pFile, nCurOffset, SEEK_SET);
            return fread(ptr, size, nmemb, m_pFile);
        }
        // �ӻ����ж�ȡ���ݣ�д������
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
    // д���¼�к�������������ļ������뻺�����ݲ������Ҳ�������ʱ���밴��
    virtual size_t _write(void* ptr, size_t size, size_t nmemb)
    {
        int nRet = DBF_SUCC;
        assert(IsOpen());
        if (m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        // �ж�����
        if (m_nCurRec <= m_oHeader.nRecNum)
        {
            // ����Ŀ���¼�е�λ��
            size_t nCurOffset = m_nCurRec * m_oHeader.nRecLen + RecordOffset();

            // �л�����Ӧ��¼�У�д������
            fseek(m_pFile, nCurOffset, SEEK_SET);
            return fwrite(ptr, size, nmemb, m_pFile);
        }
        // д��
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
    // д���ļ�������־
    size_t WriteEndFlag()
    {
        int nRet = DBF_SUCC;
        assert(IsOpen());
        if (m_bReadOnly)
        {
            return DBF_PARA_ERROR;
        }
        const char cEndFlag = 0X1A;

        // ����Ŀ��λ��
        size_t nOffset = FileSize() - 1;

        // �л�����Ӧ��¼�У�д������
        fseek(m_pFile, nOffset, SEEK_SET);
        if (fwrite(&cEndFlag, 1, 1, m_pFile) != sizeof(cEndFlag))
        {
            return DBF_ERROR;
        }

        return nRet;
    }

    // �����ֶ�λ��
    virtual size_t FindField(const std::string& strField)
    {
        std::map<std::string, size_t>::iterator e = m_mapField.find(strField);
        if (e != m_mapField.end())
        {
            return e->second;
        }
        return -1;
    }

    // дͷ���ݵ��ļ�
    int WriteHeader()
    {
        fseek(m_pFile, 0, SEEK_SET);
        if (fwrite(&m_oHeader, 1, sizeof(m_oHeader), m_pFile) != sizeof(m_oHeader))
        {
            return DBF_ERROR;
        }
        return DBF_SUCC;
    }

    // �����ļ���С
    size_t FileSize()
    {
        // �ļ�ͷ + ��¼���� + �ļ�β��
        return RecordOffset() + m_oHeader.nRecNum * m_oHeader.nRecLen + 1;
    }

    // �½����ļ�
    int NewFile(const std::string& strFile, const TDbfHeader& oHeader, const std::vector<TDbfField>& vecField, FILE** ppFile) const
    {
        *ppFile = NULL;
        int nRet = ws_fopen(ppFile, strFile.c_str(), "wb");
        if (nRet)
        {
            return DBF_FILE_ERROR;
        }
        FILE* pFile = *ppFile;

        // д��ͷ
        int nWrite = fwrite(&oHeader, 1, sizeof(oHeader), pFile);
        if (nWrite != sizeof(oHeader))
        {
            fclose(pFile);
            return DBF_FILE_ERROR;
        }
        // д���ֶ���Ϣ
        for (size_t i = 0; i < vecField.size(); i++)
        {
            nWrite = fwrite(&vecField[i], 1, sizeof(vecField[i]), pFile);
            if (nWrite != sizeof(vecField[i]))
            {
                fclose(pFile);
                return DBF_FILE_ERROR;
            }
        }
        // д�������־
        const char cHeadEndFlag = 0X0D;
        if (fwrite(&cHeadEndFlag, 1, sizeof(cHeadEndFlag), pFile) != sizeof(cHeadEndFlag))
        {
            fclose(pFile);
            return DBF_FILE_ERROR;
        }
        // ��ע��Ϣ
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
        // �ļ�����
        //const char cFileEndFlag = 0X1A;
        //if (fwrite(&cFileEndFlag, 1, sizeof(cFileEndFlag), pFile) != sizeof(cFileEndFlag))
        //{
        //    fclose(pFile);
        //    return DBF_FILE_ERROR;
        //}
        return DBF_SUCC;
    }

private:
    // ��ǰ������
    unsigned char m_cYear;
    unsigned char m_cMonth;
    unsigned char m_cDay;

    // �ļ�����
    FILE* m_pFile;
    // �ļ�·��
    std::string m_strFilePath;
    // �Ƿ�ֻ��
    bool m_bReadOnly;
    // �ļ�ͷ��Ϣ
    TDbfHeader m_oHeader;
    // �ļ��ֶ���Ϣ
    std::vector<TDbfField> m_vecField;
    // �ֶ�����
    std::map<std::string, size_t> m_mapField;
    // ��ע��Ϣ����
    size_t m_nRemarkLen;
    // ��ǰ����
    size_t m_nCurRec;
    // �հ��ַ�
    const char m_cBlank;

    // �ļ�д��¼�л��棨δ���浽�ļ������ݣ�
    CRecordBuf* m_pWriteBuf;
    // �ļ�����¼�л��棨�����ڶ���
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
    // �Ƚϵ��ֶ�
    std::vector<std::string> m_vecField;
    // ��������ֵ
    size_t m_nMaxRowDiffs;
    // �������ֵ
    size_t m_nMaxDiffs;
    // ��ǰ�ѷ��ֵ��в���
    size_t m_nCurRowDiffs;
    // ��ǰ�ѷ��ֵĲ���
    size_t m_nCurDiffs;

    // �Ƚ�����BDF�ļ��������򷵻�true
    bool Cmp(const std::string& strFile1, const std::string& strFile2)
    {
        CPDbf oDbf1;
        CPDbf oDbf2;

        // ���ļ�
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

        // ��ȡ�ֶβ��Ƚ�
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
            // �����ȡ����
            nReadNum = MMin(nReadNum, nRecNum - i);

            // ��ȡ��
            if (oDbf1.Read(i, nReadNum) || oDbf2.Read(i, nReadNum))
            {
                return false;
            }

            // �ֶαȽ�
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

        // �в���
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
